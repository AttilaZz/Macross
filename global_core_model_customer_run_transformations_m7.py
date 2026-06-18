import os
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from federer_airflow.context import ExecutionContext
from federer_airflow.dbt.snowflake import DbtTaskGroupSnowflake
from federer_airflow.utils.pager_duty import notify_pager_duty

from global_core_model_customer_dags.utils.model_alerting import email

dag_directory = os.path.dirname(os.path.abspath(__file__))

AIRFLOW_EXECUTION_CONTEXT = ExecutionContext()

ENV_CODE = AIRFLOW_EXECUTION_CONTEXT.get_environment_code()

SNOWFLAKE_ENV = AIRFLOW_EXECUTION_CONTEXT.get_snowflake_environment_code()

SNOWFLAKE_PLATFORM = AIRFLOW_EXECUTION_CONTEXT.snowflake_platform

DBT_PROJECT_NAME = "global_core_model_customer"

SNOWFLAKE_DATABASE = "WORK"
SNOWFLAKE_WAREHOUSE = "WH_APP_CORE_GL"
SNOWFLAKE_USER_CODE = "APP_CORE_GL"
SNOWFLAKE_ROLE_CODE = "RLAP_CORE_GL"
SNOWFLAKE_SCHEMA = "GL_W"

AIRFLOW_PARAMS = {
    "full_refresh_flag": Param(
        False,
        title='Full refresh',
        type='boolean',
    ),
    "portfolio_days": Param(3, title='Portfolio Data Recovery Days', type="integer", minimum=0, maximum=365),
}

COSMOS_OPERATOR_ARGS = {"install_deps": True, "full_refresh": '{{ params["full_refresh_flag"] }}'}

PORTFOLIO_PARAMS = {"PORTFOLIO_DAYS": '{{ (dag_run.conf.get("portfolio_days", params.portfolio_days) | string) }}'}

TASKS_COMMON_ARGUMENTS = {
    "project": DBT_PROJECT_NAME,
    "user_code": SNOWFLAKE_USER_CODE,
    "database_code": SNOWFLAKE_DATABASE,
    "role_code": SNOWFLAKE_ROLE_CODE,
    "warehouse_code": SNOWFLAKE_WAREHOUSE,
    "operator_args": COSMOS_OPERATOR_ARGS,
    "schema": SNOWFLAKE_SCHEMA,
}


m7_missing_in_rec = []
m7_tag_if_rec = []
if SNOWFLAKE_ENV == "REC":
    m7_missing_in_rec.append("source:customer_m7.interface__contract__m7")
    m7_missing_in_rec.append("source:customer_m7.interface__customer__m7")
    m7_tag_if_rec.append("tag:m7")

with DAG(
    dag_id="global_core_model_customer_dags_daily_run_transformations__m7",
    schedule_interval=None,
    start_date=datetime(2024, 11, 21),
    catchup=False,
    max_active_runs=1,
    params=AIRFLOW_PARAMS,
    tags=["global-core-model-customer-dbt", "ccm-dbt"],
    on_failure_callback=[notify_pager_duty(group="data.ccm")],
) as dag:
    with TaskGroup("test_sources", tooltip="Run tests on sources") as section_test:

        test_sources_m7 = DbtTaskGroupSnowflake(
            group_id="test_sources_m7",
            select=["source:customer_m7", "source:event_m7"],
            exclude=["tag:safe"],
            test_behavior="after_each",
            **TASKS_COMMON_ARGUMENTS,
        )

        test_sources_m7

    with TaskGroup("alerting", tooltip="Send warning email") as section_alerting:

        notify = PythonOperator(
            task_id="get_test_results",
            dag=dag,
            trigger_rule="all_done",
            python_callable=email,
            op_kwargs={
                "sql_file": "sql/get_test_results.sql",
                "user_code": SNOWFLAKE_USER_CODE,
                "platform": SNOWFLAKE_PLATFORM,
                "db_code": SNOWFLAKE_DATABASE,
                "schema": SNOWFLAKE_SCHEMA,
                "env": SNOWFLAKE_ENV,
                "territory": "m7",
            },
        )
        notify

    with TaskGroup("load_transco", tooltip="Run load transcos") as section_transco:

        seed_transco = DbtTaskGroupSnowflake(
            group_id="transcos",
            select=["tag:transco"],
            exclude=["tag:safe"],
            database_code="DWH",
            schema="WW",
            project="global_core_model_customer",
            user_code=SNOWFLAKE_USER_CODE,
            role_code=SNOWFLAKE_ROLE_CODE,
            warehouse_code=SNOWFLAKE_WAREHOUSE,
            operator_args=COSMOS_OPERATOR_ARGS,
            dbt_vars={"country": "m7"},
        )

        seed_transco

    with TaskGroup("stagings_snapshots", tooltip="Run snapshots") as section_snapshots:

        run_customer_snapshots = DbtTaskGroupSnowflake(
            group_id="run_customer_snapshots",
            select=["resource_type:snapshot,tag:customer,tag:m7"],
            exclude=["tag:safe"] + m7_tag_if_rec,
            **TASKS_COMMON_ARGUMENTS,
        )

        stagings_task = DbtTaskGroupSnowflake(
            group_id="stagings",
            select=["tag:staging,tag:customer,tag:m7"],
            exclude=["tag:safe"] + m7_tag_if_rec,
            **TASKS_COMMON_ARGUMENTS,
        )

        run_contract_snapshots = DbtTaskGroupSnowflake(
            group_id="run_contract_snapshots",
            select=["resource_type:snapshot,tag:contract,tag:m7"],
            exclude=["tag:safe"] + m7_tag_if_rec,
            **TASKS_COMMON_ARGUMENTS,
        )

        run_stagings_task_product = DbtTaskGroupSnowflake(
            group_id="run_stagings_task_product",
            select=["tag:staging,tag:product,tag:m7"],
            exclude=["tag:safe"] + m7_tag_if_rec,
            **TASKS_COMMON_ARGUMENTS,
        )

        run_product_snapshots = DbtTaskGroupSnowflake(
            group_id="run_snapshot_contract_commercial_product",
            select=["resource_type:snapshot,tag:product,tag:m7"],
            exclude=["tag:safe"] + m7_tag_if_rec,
            **TASKS_COMMON_ARGUMENTS,
        )

        (
            run_customer_snapshots
            >> stagings_task
            >> run_contract_snapshots
            >> run_stagings_task_product
            >> run_product_snapshots
        )

    with TaskGroup("data_marts", tooltip="Run dbt pipeline") as section_marts:

        marts_task = DbtTaskGroupSnowflake(
            group_id="marts",
            select=["tag:marts,tag:customer"],
            exclude=["tag:REFERENTIAL", "tag:poc_thoughtspot", "tag:safe"],
            dbt_vars={"country": "m7"},
            project="global_core_model_customer",
            database_code="DWH",
            schema="GL",
            user_code=SNOWFLAKE_USER_CODE,
            role_code=SNOWFLAKE_ROLE_CODE,
            warehouse_code=SNOWFLAKE_WAREHOUSE,
            operator_args=COSMOS_OPERATOR_ARGS,
        )

        portfolio_task = DbtTaskGroupSnowflake(
            group_id="portfolio_agg",
            select=["tag:portfolio_agg"],
            exclude=["tag:safe"],
            project="global_core_model_customer",
            database_code="DWH",
            schema="GL",
            user_code=SNOWFLAKE_USER_CODE,
            role_code=SNOWFLAKE_ROLE_CODE,
            warehouse_code=SNOWFLAKE_WAREHOUSE,
            operator_args=COSMOS_OPERATOR_ARGS,
            env_vars=PORTFOLIO_PARAMS,
        )

        marts_task >> portfolio_task

    section_test >> section_alerting >> section_transco >> section_snapshots >> section_marts
