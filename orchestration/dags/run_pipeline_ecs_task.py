import datetime
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from dotenv import load_dotenv
import logging
import os

load_dotenv()

access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
default_region = os.environ.get("AWS_DEFAULT_REGION")

os.environ["AWS_ACCESS_KEY_ID"] = access_key_id
os.environ["AWS_SECRET_ACCESS_KEY"] = secret_access_key
os.environ["AWS_DEFAULT_REGION"] = default_region

with DAG(
    dag_id='run_pipeline_ecs_task',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['ECS', 'Extract']
) as dag:
    run_ecs = EcsRunTaskOperator(
        task_id = 'run_pipeline_ecs_task',
        cluster='default',
        task_definition='ecs-task-def-fantasy-pl:4',
        overrides={
            "containerOverrides": [
                {
                    "name": "container-fantasy-pl"
                }
            ]
        }
    )

    run_ecs