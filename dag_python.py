from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from exemp import DataSourceToCSV, DataSourceFromCSV
from configuration import dag_config, tabel_name


with DAG(
        'sync_db_dag', 
         description='syncing metadata',
         schedule_interval= '@once',
         start_date=datetime(2022, 7, 12),
         catchup=False
         ) as dag:

    s0 = DummyOperator(
        task_id='download_csv'
    )
    #download datasource to csv file
    t1 = DataSourceToCSV(
        task_id='data_from_source_db_to_csv',
        sql=dag_config['sql_source_from'],
        csv_file_path=tabel_name['csv_path'],
        csv_file_name=tabel_name['structure_tb']
    )

    t2 = DataSourceToCSV(
        task_id='data_from_view_db_to_csv',
        sql=dag_config['sql_view'],
        csv_file_path=tabel_name['csv_path'],
        csv_file_name=tabel_name['view_tb']
    )

    t3 = DataSourceToCSV(
        task_id='data_from_function_db_to_csv',
        sql=dag_config['sql_functions'],
        csv_file_path=tabel_name['csv_path'],
        csv_file_name=tabel_name['functions_tb']
    )

    t4 = DataSourceToCSV(
        task_id='data_from_grants_db_to_csv',
        sql=dag_config['sql_grants'],
        csv_file_path=tabel_name['csv_path'],
        csv_file_name=tabel_name['grants_tb']
    )

    t5 = DataSourceToCSV(
        task_id='data_from_key_db_to_csv',
        sql=dag_config['sql_key'],
        csv_file_path=tabel_name['csv_path'],
        csv_file_name=tabel_name['key_tb']
    )

    #create tb
    t11= PostgresOperator(
        task_id='create_source_tb',
        postgres_conn_id=dag_config['pg_conn_name'],
        sql=dag_config['sql_create_source_tb'],
        database=dag_config['pg_db'],
        ##parameters={"schema": 'etl', "table_name": "table_params", "col_name": "table_schema varchar, table_name varchar, column_name varchar"},
        #runtime_parameters={'statement_timeout': '3000ms'},
        #provide_context=True
    )
    
    t21= PostgresOperator(
        task_id='create_view_tb',
        postgres_conn_id=dag_config['pg_conn_name'],
        sql=dag_config['sql_create_view_tb'],
        database=dag_config['pg_db'],
        
    )
    
    #insert data from csv file to tb
    t12 = DataSourceFromCSV(
        task_id='data_from_csv_to_source_tb',
        csv_file_path=tabel_name['csv_path'],
        csv_file_name=tabel_name['structure_tb'],
    )

    t22 = DataSourceFromCSV(
        task_id='data_from_csv_to_view_tb',
        csv_file_path=tabel_name['csv_path'],
        csv_file_name=tabel_name['view_tb'],
    )

    t7 = PostgresOperator(
        task_id='add_fnc_ddl',
        postgres_conn_id=dag_config['pg_conn_name'],
        sql='sql/fnc_create_ddl.sql',
        database=dag_config['pg_db']
    )

    t6 = PostgresOperator(
        task_id='execute_ddl',
        postgres_conn_id=dag_config['pg_conn_name'],
        sql='select fnc_create_ddl();',
        database=dag_config['pg_db']
    )


s0 >>[t1, t2, t3, t4, t5]
t1 >> t11 >> t12
t2 >> t21 >> t22 
#t12 >> [t3, t4] >> t5