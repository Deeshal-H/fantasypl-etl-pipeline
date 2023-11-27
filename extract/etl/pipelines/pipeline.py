from pathlib import Path
import yaml
import pandas as pd
from etl.assets.pipeline_logging import PipelineLogging
from etl.connectors.fantasy_pl_api import FantasyPLAPIClient
from etl.connectors.s3 import FantasyPLS3Client

if __name__ == "__main__":

    # retrieve config values from yaml file
    yaml_file_path = __file__.replace(".py", ".yaml")

    if Path(yaml_file_path).exists:
        with open(yaml_file_path, encoding='utf-8') as yaml_file:
            yaml_config = yaml.safe_load(yaml_file)
    else:
        raise Exception(f"Missing {yaml_file_path} file.")

    log_path = yaml_config.get("config").get("log_folder_path")
    s3_bucket_name = yaml_config.get("s3_bucket_name")
    base_url = yaml_config.get("base_url")
    bootstrap_path = yaml_config.get("bootstrap_path")
    fixtures_path = yaml_config.get("fixtures_path")
    player_path = yaml_config.get("player_path")

    # instantiate PipelineLogging
    pipeline_logger = PipelineLogging(pipeline_name="fantasy-pl", log_path=log_path)

    pipeline_logger.log_to_file(message="Starting pipeline run")

    # instantiate FantasyPLS3Client
    fantasyPL_S3_client = FantasyPLS3Client()

    # delete any file that already exists in the landing S3 location
    pipeline_logger.log_to_file(message="Deleting existing files from landing location")
    fantasyPL_S3_client.deleteFilesFromBucketFolder(bucket_name=s3_bucket_name, folder="landing")

    # retrieve the base info and fixtures data from the FPL API
    pipeline_logger.log_to_file(message="Retrieving base info and fixtures")
    fantasyPL_API_client = FantasyPLAPIClient(base_url=base_url)
    base_info = fantasyPL_API_client.get_base_info(bootstrap_path=bootstrap_path)
    fixtures = fantasyPL_API_client.get_fixtures(fixtures_path=fixtures_path)

    # create a pandas dataframe from the gameweeks json and upload it to the S3 landing location
    df_gameweeks = pd.json_normalize(data=base_info.gameweeks)
    df_gameweeks_dropped = df_gameweeks.drop(columns=['chip_plays'])
    fantasyPL_S3_client.dataframe_to_s3(input_datafame=df_gameweeks_dropped, bucket_name=s3_bucket_name, filepath="landing/gameweeks.parquet", format="parquet")
    pipeline_logger.log_to_file(message="Uploaded gameweeks file")

    # create a pandas dataframe from the teams json and upload it to the S3 landing location
    df_teams = pd.json_normalize(data=base_info.teams)
    fantasyPL_S3_client.dataframe_to_s3(input_datafame=df_teams, bucket_name=s3_bucket_name, filepath="landing/teams.parquet", format="parquet")
    pipeline_logger.log_to_file(message="Uploaded teams file")

    # create a pandas dataframe from the players json and upload it to the S3 landing location
    df_players = pd.json_normalize(data=base_info.players)
    fantasyPL_S3_client.dataframe_to_s3(input_datafame=df_players, bucket_name=s3_bucket_name, filepath="landing/players.parquet", format="parquet")
    pipeline_logger.log_to_file(message="Uploaded players file")

    # create a pandas dataframe from the fixtures json and upload it to the S3 landing location
    df_fixtures = pd.json_normalize(data=fixtures)
    df_fixtures_dropped = df_fixtures.drop(columns=['stats'])
    fantasyPL_S3_client.dataframe_to_s3(input_datafame=df_fixtures_dropped, bucket_name=s3_bucket_name, filepath="landing/fixtures.parquet", format="parquet")
    pipeline_logger.log_to_file(message="Uploaded fixtures file")

    # starting the individual player stats retrieval
    pipeline_logger.log_to_file(message="Starting player files upload")

    # get a list of all the player ids
    player_id_list = [x['id'] for x in base_info.players]

    list_player_stats = []

    # loop through the player ids, retrieval the player stats and add results to a list
    for player_id in player_id_list:
        pipeline_logger.log_to_file(message=f"Retrieving stats for player {player_id}")
        player_stats = fantasyPL_API_client.get_player_stats(player_path=player_path.replace("{element_id}", str(player_id)))
        list_player_stats.extend(player_stats)

    # convert the list of all player stats to a pandas dataframe
    df_player_stats = pd.DataFrame(list_player_stats)

    # upload the player stats to the S3 landing location
    fantasyPL_S3_client.dataframe_to_s3(input_datafame=df_player_stats, bucket_name=s3_bucket_name, filepath="landing/player_stats.parquet", format="parquet")
    pipeline_logger.log_to_file(message="Uploaded player stats file")

    pipeline_logger.log_to_file(message="Finished pipeline run")