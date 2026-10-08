from pathlib import Path

import pytest
from osprey.engine.ast.grammar import Source
from osprey.engine.ast.sources import CONFIG_PATH, Sources, SourcesBuilder, SourcesConfig


def _write_ruleset(root: Path) -> None:
    root.joinpath('main.sml').write_text('Foo = 1\n')


def test_from_path_without_any_config(tmp_path: Path) -> None:
    _write_ruleset(tmp_path)

    sources = Sources.from_path(tmp_path)

    assert sources.paths() == {'main.sml'}
    assert dict(sources.config) == {}


def test_from_path_with_root_config(tmp_path: Path) -> None:
    _write_ruleset(tmp_path)
    tmp_path.joinpath(CONFIG_PATH).write_text('sample_rate: 0.5\n')

    sources = Sources.from_path(tmp_path)

    assert dict(sources.config) == {'sample_rate': 0.5}


def test_from_path_with_config_directory(tmp_path: Path) -> None:
    _write_ruleset(tmp_path)
    config_dir = tmp_path.joinpath('config')
    config_dir.mkdir()
    config_dir.joinpath('sampling.yaml').write_text('sample_rate: 0.5\n')

    sources = Sources.from_path(tmp_path)

    assert dict(sources.config) == {'sample_rate': 0.5}


def test_add_config_with_no_sources_is_a_noop() -> None:
    builder = SourcesBuilder().add_source(Source(path='main.sml', contents='Foo = 1\n'))

    builder.add_config()

    assert dict(builder.build().config) == {}


def test_add_config_after_an_empty_add_config_still_works() -> None:
    builder = SourcesBuilder().add_source(Source(path='main.sml', contents='Foo = 1\n'))

    builder.add_config()
    builder.add_config(Source(path=CONFIG_PATH, contents='sample_rate: 0.5\n'))

    assert dict(builder.build().config) == {'sample_rate': 0.5}


def test_add_config_twice_still_raises() -> None:
    builder = SourcesBuilder().add_source(Source(path='main.sml', contents='Foo = 1\n'))
    builder.add_config(Source(path=CONFIG_PATH, contents='sample_rate: 0.5\n'))

    with pytest.raises(ValueError, match='A configuration already exists'):
        builder.add_config(Source(path='config/other.yaml', contents='other: 1\n'))


def test_sources_config_rejects_zero_sources() -> None:
    with pytest.raises(ValueError, match='requires at least one source'):
        SourcesConfig()


def test_to_dict_without_any_config(tmp_path: Path) -> None:
    _write_ruleset(tmp_path)

    sources = Sources.from_path(tmp_path)

    assert sources.to_dict() == {'main.sml': 'Foo = 1\n'}


def test_to_dict_round_trips_without_any_config() -> None:
    sources = Sources.from_dict({'main.sml': 'Foo = 1\n'})

    assert Sources.from_dict(sources.to_dict()).to_dict() == sources.to_dict()


def test_config_source_is_available_when_config_is_empty() -> None:
    config = SourcesConfig(Source(path=CONFIG_PATH, contents=''))

    assert config.source.contents == ''
