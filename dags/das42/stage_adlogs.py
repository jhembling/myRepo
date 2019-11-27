import os
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import S3KeySensor
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from datetime import datetime, timedelta

# custom utils
from das42.utils.job_config import JobConfig
from das42.utils.sql_utils import SqlUtils


JOB_ARGS = JobConfig.get_config()
DEFAULTS = JOB_ARGS["default_args"]
ENV = JOB_ARGS["env_name"]
TEAM_NAME = JOB_ARGS["team_name"]
SF_CONN_ID = JOB_ARGS["snowflake_conn_id"]
SF2_CONN_ID = JOB_ARGS["snowflake_conn_id_2"]
SF_ROLE = JOB_ARGS["snowflake"]["role"]
SF_WAREHOUSE = JOB_ARGS["snowflake"]["warehouse"]
SF_DATABASE = JOB_ARGS["snowflake"]["database"]
S3_CONNECTION = JOB_ARGS["s3_conn_id"]

# create DAG
DAG = DAG(
    "stage_transform",
    default_args=DEFAULTS,
    start_date=datetime(2018, 1, 1),
    schedule_interval=JOB_ARGS["schedule_interval"],
    catchup=False,
    max_active_runs=1,
)

stage_finish = DummyOperator(task_id="adlogs_snowflake_staging_finish")

# staging ad logs hourly
for table in JOB_ARGS["tables"]:

    stage_sql_path = os.path.join(
        JOB_ARGS["stage_sql_path"],
        table
        )

    load_sql_path = os.path.join(
        JOB_ARGS["load_sql_path"],
        table
        )

    query_log = SqlUtils.load_query(stage_sql_path).split("---")

    load_log = SqlUtils.load_query(load_sql_path).split("---")

    key_sensor = S3KeySensor(
        task_id='check_{}_bucket'.format(table),
        bucket_key='stage_{}_logs_dev/20190704/15/log/*.csv'.format(table),
        wildcard_match=True,
        bucket_name='das42-airflow-training',
        aws_conn_id=S3_CONNECTION,
        poke_interval=10,
        dag=DAG
    )


    stage_adlogs_hourly_job = SnowflakeOperator(
        task_id="stage_logs_{}_hourly".format(table),
        snowflake_conn_id=SF_CONN_ID,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        sql=query_log,
        params={
            "env": ENV,
            "team_name": TEAM_NAME
        },
        autocommit=True,
        trigger_rule='all_done',
        dag=DAG
    )

    transform_adlogs_hourly_job = SnowflakeOperator(
        task_id="transform_logs_{}_hourly".format(table),
        snowflake_conn_id=SF2_CONN_ID,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        sql=load_log,
        params={
            "env": ENV,
            "team_name": TEAM_NAME
        },
        autocommit=True,
        trigger_rule='all_done',
        dag=DAG
    )

    key_sensor >> stage_adlogs_hourly_job >> transform_adlogs_hourly_job >> stage_finish
