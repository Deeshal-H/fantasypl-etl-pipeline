from pathlib import Path
import yaml
import pandas as pd
import logging
from datetime import datetime
from io import BytesIO
from etl.connectors.fantasy_pl_api import FantasyPLAPIClient
from etl.connectors.s3 import FantasyPLS3Client

if __name__ == "__main__":
    
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting pipeline run")

    yaml_file_path = __file__.replace(".py", ".yaml")

    if Path(yaml_file_path).exists:
        with open(yaml_file_path) as yaml_file:
            yaml_config = yaml.safe_load(yaml_file)
    else:
        raise Exception(f"Missing {yaml_file_path} file.")
    
    s3_bucket_name = yaml_config.get("s3_bucket_name")
    s3_bucket_base_url = yaml_config.get("s3_bucket_base_url")
    base_url = yaml_config.get("base_url")
    bootstrap_path = yaml_config.get("bootstrap_path")
    fixtures_path = yaml_config.get("fixtures_path")
    player_path = yaml_config.get("player_path")

    # df_fixtures = pd.read_parquet(path=".\etl\\data\\fixtures.parquet")
    # df_fixtures.to_clipboard()

    fantasyPL_S3_client = FantasyPLS3Client()
    fantasyPL_S3_client.deleteFilesFromBucketFolder(bucket_name=s3_bucket_name, folder="landing")

    fantasyPL_API_client = FantasyPLAPIClient(base_url=base_url)
    base_info = fantasyPL_API_client.get_base_info(bootstrap_path=bootstrap_path)
    fixtures = fantasyPL_API_client.get_fixtures(fixtures_path=fixtures_path)

    s3_url_gameweeks = f"{s3_bucket_base_url}landing/gameweeks.parquet"
    df_gameweeks = pd.json_normalize(data=base_info.gameweeks)
    df_gameweeks_dropped = df_gameweeks.drop(columns=['chip_plays'])
    df_gameweeks_dropped.to_parquet(path=s3_url_gameweeks, index=False)

    s3_url_teams = f"{s3_bucket_base_url}landing/teams.parquet"
    df_teams = pd.json_normalize(data=base_info.teams)
    df_teams.to_parquet(path=s3_url_teams, index=False)

    s3_url_players = f"{s3_bucket_base_url}landing/players.parquet"
    df_players = pd.json_normalize(data=base_info.players)
    df_players.to_parquet(path=s3_url_players, index=False)
    
    s3_url_fixtures = f"{s3_bucket_base_url}landing/fixtures.parquet"
    df_fixtures = pd.json_normalize(data=fixtures)
    df_fixtures_dropped = df_fixtures.drop(columns=['stats'])
    df_fixtures_dropped.to_parquet(path=s3_url_fixtures, index=False)

    player_id_list = [x['id'] for x in base_info.players]

    list_player_stats = []

    count = 0

    for player_id in player_id_list:
        print(player_path.replace("{element_id}", str(player_id)))
        player_stats = fantasyPL_API_client.get_player_stats(player_path=player_path.replace("{element_id}", str(player_id)))
        list_player_stats.extend(player_stats)

    df_player_stats = pd.DataFrame(list_player_stats)

    s3_url_player_stats = f"{s3_bucket_base_url}landing/player_stats.parquet"
    df_player_stats.to_parquet(path=s3_url_player_stats, index=False)