import pytest
from etl.connectors.fantasy_pl_api import FantasyPLAPIClient
import yaml
from pathlib import Path

def pytest_namespace():
    return {
        'base_url': None,
        'bootstrap_path': None,
        'fixtures_path': None,
        'player_path': None
    }

@pytest.fixture
def setup():

    # retrieve config values from yaml file
    yaml_file_path = __file__.replace(".py", ".yaml")

    if Path(yaml_file_path).exists:
        with open(yaml_file_path) as yaml_file:
            yaml_config = yaml.safe_load(yaml_file)
    else:
        raise Exception(f"Missing {yaml_file_path} file.")
    
    base_url = yaml_config.get("base_url")
    bootstrap_path = yaml_config.get("bootstrap_path")
    fixtures_path = yaml_config.get("fixtures_path")
    player_path = yaml_config.get("player_path")

    # store the configuration values in the pytest namespace
    pytest.base_url = base_url
    pytest.bootstrap_path = bootstrap_path
    pytest.fixtures_path = fixtures_path
    pytest.player_path = player_path

def test_get_base_info(setup):

    base_url = pytest.base_url
    bootstrap_path = pytest.bootstrap_path

    fantasyPL_API_client = FantasyPLAPIClient(base_url=base_url)
    base_info = fantasyPL_API_client.get_base_info(bootstrap_path=bootstrap_path)

    assert len(base_info.gameweeks) == 38
    assert len(base_info.teams) == 20
    assert len(base_info.players) > 0

def test_get_fixtures():

    base_url = pytest.base_url
    fixtures_path = pytest.fixtures_path

    fantasyPL_API_client = FantasyPLAPIClient(base_url=base_url)
    fixtures = fantasyPL_API_client.get_fixtures(fixtures_path=fixtures_path)

    assert len(fixtures) == 380
    
def test_get_player_stats():

    base_url = pytest.base_url
    player_path = pytest.player_path

    fantasyPL_API_client = FantasyPLAPIClient(base_url=base_url)
    players = fantasyPL_API_client.get_player_stats(player_path=player_path)

    assert len(players) > 0