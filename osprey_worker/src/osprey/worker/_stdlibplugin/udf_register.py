from typing import Any, Sequence, Type

from osprey.engine.stdlib.udfs.domain_chopper import DomainChopper
from osprey.engine.stdlib.udfs.domain_tld import DomainTld
from osprey.engine.stdlib.udfs.email_domain import EmailDomain, EmailSubdomain
from osprey.engine.stdlib.udfs.email_local_part import EmailLocalPart
from osprey.engine.stdlib.udfs.entity import Entity, EntityJson
from osprey.engine.stdlib.udfs.experiments import (
    Experiment,
    ExperimentsBucketAssignment,
    ExperimentWhen,
)
from osprey.engine.stdlib.udfs.extract_cookie import ExtractCookie
from osprey.engine.stdlib.udfs.get_action_name import GetActionName
from osprey.engine.stdlib.udfs.import_ import Import
from osprey.engine.stdlib.udfs.ip_network import IpNetwork
from osprey.engine.stdlib.udfs.json_data import JsonData
from osprey.engine.stdlib.udfs.list_length import ListLength
from osprey.engine.stdlib.udfs.list_read import ListRead
from osprey.engine.stdlib.udfs.list_sort import ListSort
from osprey.engine.stdlib.udfs.mx_lookup import MXLookup
from osprey.engine.stdlib.udfs.phone_country import PhoneCountry
from osprey.engine.stdlib.udfs.phone_prefix import PhonePrefix
from osprey.engine.stdlib.udfs.random_bool import RandomBool
from osprey.engine.stdlib.udfs.random_int import RandomInt
from osprey.engine.stdlib.udfs.regex_match import RegexMatch, RegexMatchMap
from osprey.engine.stdlib.udfs.require import Require
from osprey.engine.stdlib.udfs.resolve_optional import ResolveOptional
from osprey.engine.stdlib.udfs.rules import Rule, WhenRules
from osprey.engine.stdlib.udfs.string import (
    StringClean,
    StringEndsWith,
    StringExtractDomains,
    StringExtractURLs,
    StringJoin,
    StringLength,
    StringLStrip,
    StringReplace,
    StringRStrip,
    StringSplit,
    StringStartsWith,
    StringStrip,
    StringToLower,
    StringToUpper,
)
from osprey.engine.stdlib.udfs.string_base64 import Base64Decode, Base64Encode
from osprey.engine.stdlib.udfs.string_hashes import (
    HashMd5,
    HashSha1,
    HashSha256,
    HashSha512,
)
from osprey.engine.stdlib.udfs.time_bucket import (
    GetSnowflakeBucket,
    GetTimedeltaBucket,
    GetTimestampBucket,
)
from osprey.engine.stdlib.udfs.time_delta import TimeDelta
from osprey.engine.stdlib.udfs.time_since import TimeSince
from osprey.engine.stdlib.udfs.verdicts import DeclareVerdict
from osprey.engine.udf.base import UDFBase
from osprey.worker.adaptor.plugin_manager import hookimpl_osprey


@hookimpl_osprey
def register_udfs() -> Sequence[Type[UDFBase[Any, Any]]]:
    """
    Registers all stdlib UDFs.
    Does not need to be called directly, as its hook is called by the plugin manager.
    """
    return [
        DeclareVerdict,
        DomainChopper,
        DomainTld,
        EmailDomain,
        EmailSubdomain,
        EmailLocalPart,
        Entity,
        EntityJson,
        Experiment,
        ExperimentWhen,
        ExperimentsBucketAssignment,
        ExtractCookie,
        GetActionName,
        Import,
        IpNetwork,
        JsonData,
        ListLength,
        ListRead,
        ListSort,
        MXLookup,
        PhoneCountry,
        PhonePrefix,
        RandomBool,
        RandomInt,
        RegexMatch,
        RegexMatchMap,
        Require,
        ResolveOptional,
        Rule,
        Base64Encode,
        Base64Decode,
        HashMd5,
        HashSha1,
        HashSha256,
        HashSha512,
        StringLength,
        StringToLower,
        StringToUpper,
        StringStartsWith,
        StringEndsWith,
        StringStrip,
        StringRStrip,
        StringLStrip,
        StringReplace,
        StringJoin,
        StringSplit,
        StringClean,
        StringExtractDomains,
        StringExtractURLs,
        GetTimedeltaBucket,
        GetTimestampBucket,
        GetSnowflakeBucket,
        TimeDelta,
        TimeSince,
        WhenRules,
    ]
