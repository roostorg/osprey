import pytest
from osprey.worker.stress.cli import (
    EXIT_INTERNAL_ERROR,
    build_parser,
    cmd_measure,
    main,
)


class TestParser:
    def test_run_minimal_args_parse(self) -> None:
        args = build_parser().parse_args(['run'])
        assert args.command == 'run'
        assert args.events == 1000
        assert args.rate == 100.0
        assert args.report == 'human'

    def test_run_threshold_args_parse(self) -> None:
        args = build_parser().parse_args(
            [
                'run',
                '--events',
                '50',
                '--rate',
                '10',
                '--threshold-drop-rate',
                '0.05',
                '--threshold-p95-ms',
                '750',
                '--report',
                'json',
            ]
        )
        assert args.events == 50
        assert args.rate == 10.0
        assert args.threshold_drop_rate == 0.05
        assert args.threshold_p95_ms == 750.0
        assert args.report == 'json'

    def test_run_kafka_args_parse(self) -> None:
        args = build_parser().parse_args(
            [
                'run',
                '--bootstrap-servers',
                'kafka-a:9092,kafka-b:9092',
                '--input-topic',
                'my.input',
                '--output-topic',
                'my.output',
            ]
        )
        assert args.bootstrap_servers == 'kafka-a:9092,kafka-b:9092'
        assert args.input_topic == 'my.input'
        assert args.output_topic == 'my.output'

    def test_no_command_requires_one(self) -> None:
        with pytest.raises(SystemExit):
            build_parser().parse_args([])


class TestMeasureStub:
    def test_measure_returns_internal_error_for_now(self, capsys: pytest.CaptureFixture[str]) -> None:
        args = build_parser().parse_args(['measure'])
        rc = cmd_measure(args)
        assert rc == EXIT_INTERNAL_ERROR
        captured = capsys.readouterr()
        assert '#236' in captured.err

    def test_main_dispatches_to_measure(self) -> None:
        rc = main(['measure'])
        assert rc == EXIT_INTERNAL_ERROR
