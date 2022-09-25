from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from configuration import dag_config_1
from modify import line_task

#PARAMETERS
pg_conn_name='quanta_conn'
database='quanta'
name_foreign_server='foreign_demo'
host="'127.0.0.1'"
port="'5433'"
dbname="'demo'"
l_user='postgres'
f_user="'postgres'"
pasword="'123'"

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
        sql='select etl.fnc_create_ddl(%(name_foreign_server)s, %(host)s, %(port)s, %(dbname)s, %(l_user)s,%(f_users)s, %(pasword)s);',
        parameters={"name_foreign_server":name_foreign_server, "host":host, "port":port, "dbname":dbname, "l_user":l_user, "f_users":f_user, "pasword":pasword},
        database=database,
        )

    scoup=line_task(dag_config_1)

s0 >> scoup >> union_branch >> t6 >> t7