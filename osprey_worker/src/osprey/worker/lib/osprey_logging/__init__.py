import datetime
import importlib.resources
import inspect
import logging
import logging.config
import os
import sys
import types
from logging.handlers import SysLogHandler
from typing import Dict, List, Optional, Tuple, Union, cast

import pythonjsonlogger.jsonlogger
import pytz
import yaml
from flask import g as flask_global
from flask import has_request_context

try:
    import curses
except ImportError:
    curses = None  # type: ignore # Mypy doesn't handle Optional imports like this well


DATADOG_LOG_TAGS = ['dd.service', 'dd.env', 'dd.version', 'dd.trace_id', 'dd.span_id']
# Duplicate strings because importing from shared_constants seems to cause build issues
DATADOG_ROUTE_METADATA_ATTR = 'route_metadata'
DATADOG_ROUTE_OWNER_TAG = 'team'
DATADOG_ROUTE_NAME_TAG = 'route_name'


def _stderr_supports_color():
    color = False
    if curses and sys.stderr.isatty():
        # noinspection PyBroadException
        try:
            curses.setupterm()
            if curses.tigetnum('colors') > 0:
                color = True
        except Exception:
            pass
    return color


def environ_bool(key: str, default: bool | None = None):
    val = os.environ.get(key, default)
    match val:
        case str():
            return val.lower() == 'true'
        case bool() | None:
            return val


class PrettyLogFormatter(logging.Formatter):
    """Log formatter used in Tornado.

    Key features of this formatter are:

    * Color support when logging to a terminal that supports it.
    * Timestamps on every log line.
    * Robust against str/bytes encoding problems.

    This formatter is enabled automatically by
    `tornado.options.parse_command_line` (unless ``--logging=none`` is
    used).
    """

    def __init__(self, color=True, *args, **kwargs):
        logging.Formatter.__init__(self, *args, **kwargs)
        self._color = color and _stderr_supports_color()
        if self._color:
            fg_color = curses.tigetstr('setaf') or curses.tigetstr('setf') or ''
            self._colors = {
                logging.DEBUG: str(curses.tparm(fg_color, 4), 'ascii'),  # Blue
                logging.INFO: str(curses.tparm(fg_color, 2), 'ascii'),  # Green
                logging.WARNING: str(curses.tparm(fg_color, 3), 'ascii'),  # Yellow
                logging.ERROR: str(curses.tparm(fg_color, 1), 'ascii'),  # Red
            }
            self._normal = str(curses.tigetstr('sgr0'), 'ascii')
        self._include_datadog = environ_bool('OSPREY_DD_LOGGING', True)

    def format(self, record):
        try:
            record.message = record.getMessage()
        except Exception as e:
            record.message = f'Bad message ({e!r}): {record.__dict__!r}'
        assert isinstance(record.message, str)  # guaranteed by logging
        record.asctime = datetime.datetime.fromtimestamp(record.created, pytz.UTC).strftime('%y%m%d %H:%M:%S.%f')
        prefix = '[%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d]' % record.__dict__

        if self._color:
            prefix = self._colors.get(record.levelno, self._normal) + prefix + self._normal

        # Check if we've patched logging with ddtrace, and if so include the ddtrace info in log lines
        if getattr(logging, '_datadog_patch', True) and self._include_datadog:
            log_tags = []
            for tag_name in DATADOG_LOG_TAGS:
                tag = getattr(record, tag_name, None)
                if tag:
                    log_tags.append('{}={}'.format(tag_name, tag))
            for tag_name, tag_value in get_route_metadata_logging_tags():
                log_tags.append('{}={}'.format(tag_name, tag_value or 'unknown'))

            if log_tags:
                prefix += ' [' + ' '.join(log_tags) + ']'

        # Encoding notes:  The logging module prefers to work with character
        # strings, but only enforces that log messages are instances of
        # basestring.  In python 2, non-ascii bytestrings will make
        # their way through the logging framework until they blow up with
        # an unhelpful decoding error (with this formatter it happens
        # when we attach the prefix, but there are other opportunities for
        # exceptions further along in the framework).
        #
        # If a byte string makes it this far, convert it to unicode to
        # ensure it will make it out to the logs.  Use repr() as a fallback
        # to ensure that all byte strings can be converted successfully,
        # but don't do it by default so we don't add extra quotes to ascii
        # bytestrings.  This is a bit of a hacky place to do this, but
        # it's worth it since the encoding errors that would otherwise
        # result are so useless (and tornado is fond of using utf8-encoded
        # byte strings whereever possible).
        def safe_unicode(value):
            try:
                if isinstance(value, str):
                    return value
                return value.decode('utf-8')
            except UnicodeDecodeError:
                return repr(value)

        formatted = prefix + ' ' + safe_unicode(record.message)
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            # exc_text contains multiple lines.  We need to safe_unicode
            # each line separately so that non-utf8 bytes don't cause
            # all the newlines to turn into '\n'.
            lines = [formatted.rstrip()]
            lines.extend(safe_unicode(ln) for ln in record.exc_text.split('\n'))
            formatted = '\n'.join(lines)
        return formatted.replace('\n', '\n    ')


class JsonLogFormatter(pythonjsonlogger.jsonlogger.JsonFormatter):  # type: ignore
    def __init__(self, *args, rename_fields: Optional[Dict[str, str]] = None, **kwargs):
        """Support renaming fields in python-json-logger<2.0.1 and python_json_logger>=2.0.1

        In the latter version, `rename_fields` is supported in the superclass's constructor parameters. In
        older versions of this dep, passing `rename_fields` to the superclass's constructor crashes so
        save that state here instead.
        """

        # TODO: Remove this workaround if osprey doesn't need to pin python-json-logger<2.0.1
        handles_rename_fields = 'rename_fields' in inspect.signature(super().__init__).parameters
        if handles_rename_fields:
            super().__init__(*args, rename_fields=rename_fields, **kwargs)
        else:
            self.rename_fields = rename_fields if rename_fields else {}
            super().__init__(*args, **kwargs)

        self._include_datadog = environ_bool('OSPREY_DD_LOGGING', True)

    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:
        # https://stackoverflow.com/questions/6290739/python-logging-use-milliseconds-in-time-format
        dt = datetime.datetime.fromtimestamp(record.created, tz=datetime.timezone.utc)
        if datefmt:
            s = dt.strftime(datefmt)
        else:
            t = dt.strftime(self.default_time_format)
            s = '%s.%03d' % (t, record.msecs)
        return s

    def add_fields(self, log_record: Dict, record: logging.LogRecord, message_dict: Dict) -> None:  # type: ignore
        # backport rename_fields from v2.0.1+ of python-json-logger into py2-compatible versions of JsonFormatter.
        for field in self._required_fields:
            if field in self.rename_fields:
                log_record[self.rename_fields[field]] = record.__dict__.get(field)
            else:
                log_record[field] = record.__dict__.get(field)
        log_record.update(message_dict)
        pythonjsonlogger.jsonlogger.merge_record_extra(record, log_record, reserved=self._skip_fields)

        if self.timestamp:
            key = self.timestamp if type(self.timestamp) is str else 'timestamp'
            if 'timezone' in dir(datetime):
                log_record[key] = datetime.datetime.fromtimestamp(record.created, tz=datetime.timezone.utc)
            else:
                # workaround for missing datetime.timezone in python 2
                log_record[key] = datetime.datetime.utcnow()

        # Check if we've patched logging with ddtrace, and if so include the ddtrace info in log objects
        if getattr(logging, '_datadog_patch', True) and self._include_datadog:
            for tag_name in DATADOG_LOG_TAGS:
                tag = getattr(record, tag_name, None)
                if tag:
                    log_record[tag_name] = tag
            for tag_name, tag in get_route_metadata_logging_tags():
                log_record[tag_name] = tag or 'unknown'


def _remove_handlers_except(name: str, logger: logging.Logger) -> None:
    """Remove all handlers from the given logger except for the handler with the given name.

    Raises ValueError if no handler with the given name is found on the given logger.
    """
    handlers = {handler.name: handler for handler in logger.handlers}
    if name not in handlers:
        raise ValueError(f"Log handler '{name}' not found on logger '{logger.name}'; found {list(handlers.keys())}")

    for handler in logger.handlers:
        if handler.name != name:
            logger.removeHandler(handler)


def get_route_metadata_logging_tags() -> List[Tuple[str, Optional[str]]]:
    """Fetches route metadata from the Flask global context so it can be added as log tags.

    Returns an empty array if no metadata exists. Else, returns a list of tuples of tag name and tag value.
    """
    if not has_request_context() or not hasattr(flask_global, DATADOG_ROUTE_METADATA_ATTR):
        return []
    return cast(List[Tuple[str, Optional[str]]], flask_global.get(DATADOG_ROUTE_METADATA_ATTR).items())


def configure_logging(
    package: Union[types.ModuleType, str] = 'osprey.worker.lib.osprey_logging',
    handler_name: str = 'pretty_stderr',
) -> None:
    """Apply a global logging configuration YAML within the given package. If handler name is given as an argument, or
    is overridden via environment variable OSPREY_PYTHON_LOG_HANDLER, remove all other handlers from the root logger.

    NOTE: Consider calling this function from the top of your application's call stack only, since it can globally
    reconfigure all logs for the process as a side effect.
    """
    config_yaml = importlib.resources.files(package).joinpath('logging.yaml').read_text()
    configuration = yaml.safe_load(config_yaml)
    logging.config.dictConfig(configuration)

    root_logger = logging.getLogger()
    handler_name = os.environ.get('OSPREY_PYTHON_LOG_HANDLER', handler_name)
    if handler_name is not None:
        _remove_handlers_except(handler_name, root_logger)


def enable_pretty_logging(logger=None, log_level='info'):
    if logger is None:
        logger = logging.getLogger()
    if hasattr(logger, 'pretty'):
        return
    logger.pretty = True
    # if a PrettyLogFormatter was added in some other way (e.g. above in configure_logging)
    # then `pretty` won't have been set yet.  We should bail out here if that's the case.
    if any(isinstance(h.formatter, PrettyLogFormatter) for h in logger.handlers):
        return
    logger.setLevel(logging.getLevelName(log_level.upper()))
    channel = logging.StreamHandler()
    channel.setFormatter(PrettyLogFormatter())
    logger.addHandler(channel)


def enable_syslog(address, fmt='%(name)s: %(levelname)s %(message)s'):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    hostname, port = address.split(':')
    syslog = SysLogHandler(address=(hostname, int(port)))
    syslog.setFormatter(logging.Formatter(fmt))
    logger.addHandler(syslog)
