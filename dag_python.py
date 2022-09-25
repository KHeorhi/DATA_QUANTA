from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from configuration import dag_config_1
from modify import line_task

#PARAMETERS
pg_conn_name='quanta_conn'
database='quanta'


with DAG(
        'sync_db_dag', 
         description='syncing metadata',
         schedule_interval= '@once',
         start_date=datetime(2022, 7, 12),
         catchup=False
         ) as dag:

    s0 = DummyOperator(
        task_id='download_csv_1'
    )

    union_branch = DummyOperator(
        task_id='union_branch'
    )

    t6 = PostgresOperator(
        task_id='add_fnc_ddl',
        postgres_conn_id=pg_conn_name,
        sql='sql/fnc_create_ddl.sql',
        database=database,
        )

    t7 = PostgresOperator(
        task_id='execute_ddl',
        postgres_conn_id=pg_conn_name,
        sql='select etl.fnc_create_ddl();',
        database=database,
        )

    scoup=line_task(dag_config_1)

s0 >> scoup >> union_branch >> t6 >> t7