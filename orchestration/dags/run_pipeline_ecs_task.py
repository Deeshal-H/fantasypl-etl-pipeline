import datetime
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import task
from dotenv import load_dotenv
import logging
import os

# set the airbyte connections' connection id
AIRFLOW_AIRBYTE_CONNECTION_ID = "airbyte-connection"
AIRBYTE_FIXTURES_S3_TO_SNOWFLAKE_CONNECTION_ID = "f34748bc-1539-48d6-becc-b92164fbf41b"
AIRBYTE_GAMEWEEKS_S3_SNOWFLAKE_CONNECTION_ID = "f8b9ec14-bf65-43b7-b8a6-ea8af5f7c3d2"
AIRBYTE_PLAYER_STATS_S3_SNOWFLAKE_CONNECTION_ID = "3833b2bb-a59b-4ab3-91b0-2284ba08ffb0"
AIRBYTE_PLAYERS_S3_SNOWFLAKE_CONNECTION_ID = "16ea918e-77f8-4433-9d1f-5fc3c44a7f4a"
AIRBYTE_TEAMS_S3_SNOWFLAKE_CONNECTION_ID = "b3d2f6ce-9b3d-4520-acf5-3946af9d3500"

# load the environment variables and get the aws access keys 
load_dotenv()

access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
default_region = os.environ.get("AWS_DEFAULT_REGION")

# os.environ["AWS_ACCESS_KEY_ID"] = access_key_id
# os.environ["AWS_SECRET_ACCESS_KEY"] = secret_access_key
# os.environ["AWS_DEFAULT_REGION"] = default_region

@task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
def watcher():
    raise AirflowException("Failing task because one or more upstream tasks failed.")

with DAG(
    dag_id='run_extract_pipeline_ecs_task',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['fantasy PL', 'extract', 'dbt build', 'airbyte', 'ECS']
) as dag:
    
    # task to run the extract pipeline task
    extract_pipeline_ecs_task = EcsRunTaskOperator(
        task_id = 'run_extract_pipeline_ecs_task',
        cluster='default',
        task_definition='ecs-task-def-fantasy-pl:5',
        overrides={
            "containerOverrides": [
                {
                    "name": "container-fantasy-pl"
                }
            ]
        }
    )

    # task to trigger airbyte syncs
    trigger_airbyte_gameweeks_sync = AirbyteTriggerSyncOperator(
       task_id='airbyte_trigger_gameweeks_sync',
       airbyte_conn_id=AIRFLOW_AIRBYTE_CONNECTION_ID,
       connection_id=AIRBYTE_GAMEWEEKS_S3_SNOWFLAKE_CONNECTION_ID,
       asynchronous=False
    )

    trigger_airbyte_fixtures_sync = AirbyteTriggerSyncOperator(
       task_id='airbyte_trigger_fixtures_sync',
       airbyte_conn_id=AIRFLOW_AIRBYTE_CONNECTION_ID,
       connection_id=AIRBYTE_FIXTURES_S3_TO_SNOWFLAKE_CONNECTION_ID,
       asynchronous=False
    )

    airbyte_trigger_teams_sync = AirbyteTriggerSyncOperator(
       task_id='airbyte_trigger_teams_sync',
       airbyte_conn_id=AIRFLOW_AIRBYTE_CONNECTION_ID,
       connection_id=AIRBYTE_TEAMS_S3_SNOWFLAKE_CONNECTION_ID,
       asynchronous=False
    )

    trigger_airbyte_players_sync = AirbyteTriggerSyncOperator(
       task_id='airbyte_trigger_players_sync',
       airbyte_conn_id=AIRFLOW_AIRBYTE_CONNECTION_ID,
       connection_id=AIRBYTE_PLAYERS_S3_SNOWFLAKE_CONNECTION_ID,
       asynchronous=False
    )

    trigger_airbyte_player_stats_sync = AirbyteTriggerSyncOperator(
       task_id='airbyte_trigger_player_stats_sync',
       airbyte_conn_id=AIRFLOW_AIRBYTE_CONNECTION_ID,
       connection_id=AIRBYTE_PLAYER_STATS_S3_SNOWFLAKE_CONNECTION_ID,
       asynchronous=False
    )

    # task to run the dbt build task
    dbt_build_ecs_task = EcsRunTaskOperator(
        task_id = 'run_dbt_build_ecs_task',
        cluster='default',
        task_definition='ecs-task-def-fpl-dbtbuild:1',
        overrides={
            "containerOverrides": [
                {
                    "name": "container-fpl-dbtbuild"
                }
            ]
        }
    )

    extract_pipeline_ecs_task >> \
    [trigger_airbyte_gameweeks_sync, trigger_airbyte_fixtures_sync, airbyte_trigger_teams_sync, trigger_airbyte_players_sync, trigger_airbyte_player_stats_sync] \
    >> dbt_build_ecs_task

    dag.tasks >> watcher()

