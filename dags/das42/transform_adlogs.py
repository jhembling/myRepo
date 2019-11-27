import os
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import ExternalTaskSensor
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
    "transform_logs",
    default_args=DEFAULTS,
    start_date=datetime(2018, 1, 1),
    schedule_interval=JOB_ARGS["schedule_interval"],
    catchup=False,
    max_active_runs=1,
)

stage_finish = DummyOperator(task_id="snowflake_transformation_finish")

# staging ad logs hourly
task_sensor = ExternalTaskSensor(
    dag=DAG,
    task_id='wait_for_different_dag',
    external_dag_id='stage_transform',
    external_task_id='adlogs_snowflake_staging_finish',
    poke_interval=10,
)

ipn_sql_path = os.path.join(
    JOB_ARGS["transform_sql_path"],
    'ipn_blacklist'
    )

smart_sql_path = os.path.join(
    JOB_ARGS["transform_sql_path"],
    'smart_clip_patch'
    )

set_sql_path = os.path.join(
    JOB_ARGS["transform_sql_path"],
    'set_interaction_smartclip_placement'
    )

ipn_transform = SqlUtils.load_query(ipn_sql_path).split("---")

smart_transform = SqlUtils.load_query(smart_sql_path).split("---")

set_transform = SqlUtils.load_query(set_sql_path).split("---")

set_adlogs_hourly_job = SnowflakeOperator(
    task_id="set_transform_logs_int_hourly",
    snowflake_conn_id=SF2_CONN_ID,
    warehouse=SF_WAREHOUSE,
    database=SF_DATABASE,
    sql=set_transform,
    params={
        "env": ENV,
        "team_name": TEAM_NAME
    },
    autocommit=True,
    trigger_rule='all_done',
    dag=DAG
)

ipn_state_adlogs_hourly_job = SnowflakeOperator(
    task_id="ipn_transform_logs_state_hourly",
    snowflake_conn_id=SF2_CONN_ID,
    warehouse=SF_WAREHOUSE,
    database=SF_DATABASE,
    sql=ipn_transform,
    params={
        "env": ENV,
        "team_name": TEAM_NAME
    },
    autocommit=True,
    trigger_rule='all_done',
    dag=DAG
)

for table in ['imp', 'click']:

    ipn_adlogs_hourly_job = SnowflakeOperator(
        task_id="ipn_transform_logs_{}_hourly".format(table),
        snowflake_conn_id=SF2_CONN_ID,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        sql=ipn_transform,
        params={
            "env": ENV,
            "team_name": TEAM_NAME
        },
        autocommit=True,
        trigger_rule='all_done',
        dag=DAG
    )

    smart_adlogs_hourly_job = SnowflakeOperator(
        task_id="smart_transform_logs_{}_hourly".format(table),
        snowflake_conn_id=SF2_CONN_ID,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        sql=smart_transform,
        params={
            "env": ENV,
            "team_name": TEAM_NAME
        },
        autocommit=True,
        trigger_rule='all_done',
        dag=DAG
    )


    task_sensor >> ipn_adlogs_hourly_job >> smart_adlogs_hourly_job >> stage_finish

task_sensor >> set_adlogs_hourly_job >> stage_finish

task_sensor >> ipn_state_adlogs_hourly_job >> stage_finish
