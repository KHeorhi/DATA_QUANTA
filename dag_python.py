from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from exemp import DataSourceToCSV, DataSourceFromCSV
from configuration import dag_config


with DAG(
        'sync_db_dag', 
         description='syncing metadata',
         schedule_interval= '@once',
         start_date=datetime(2022, 7, 12),
         catchup=False) as dag:

    #download datasource to csv file
    t1 = DataSourceToCSV(
        task_id='data_from_db_to_csv',
        sql=dag_config['sql_from'],
        csv_file_path=dag_config['csv_file_path']
    )

    t2= PostgresOperator(
        task_id='create_source_tb',
        postgres_conn_id=dag_config['pg_conn_name'],
        sql=dag_config['sql_to'],
        database=dag_config['pg_db'],
        ##parameters={"schema": 'etl', "table_name": "table_params", "col_name": "table_schema varchar, table_name varchar, column_name varchar"},
        #runtime_parameters={'statement_timeout': '3000ms'},
        #provide_context=True
    )
    t3 = DataSourceFromCSV(
        task_id='data_from_csv_to_source_tb',
        csv_file_path=dag_config['csv_file_path']
    )

    t4 = PostgresOperator(
        task_id='add_fnc_ddl',
        postgres_conn_id=dag_config['pg_conn_name'],
        sql='sql/fnc_create_ddl.sql',
        database=dag_config['pg_db']
    )

    t5 = PostgresOperator(
        task_id='execute_ddl',
        postgres_conn_id=dag_config['pg_conn_name'],
        sql='select fnc_create_ddl();',
        database=dag_config['pg_db']
    )


t1 >> t2 >> [t3, t4] >> t5