from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task_group
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

    union_branch = DummyOperator(
        task_id='union_branch'
    )

    t6 = PostgresOperator(
        task_id='add_fnc_ddl',
        postgres_conn_id=dag_config['pg_conn_name'],
        sql='sql/fnc_create_ddl.sql',
        database=dag_config['pg_db']
        )

    t7 = PostgresOperator(
        task_id='execute_ddl',
        postgres_conn_id=dag_config['pg_conn_name'],
        sql='select etl.fnc_create_ddl();',
        database=dag_config['pg_db']
        )
    with TaskGroup('scoup_task') as scoup:
        with TaskGroup('group1') as group1:
            t1 = DataSourceToCSV(
                task_id='data_from_source_db_to_csv',
                sql=dag_config['sql_source_from'],
                csv_file_path=tabel_name['csv_path'],
                csv_file_name=tabel_name['structure_tb']
            )
            t11= PostgresOperator(
            task_id='create_source_tb',
            postgres_conn_id=dag_config['pg_conn_name'],
            sql=dag_config['sql_create_source_tb'],
            database=dag_config['pg_db'],
            ##parameters={"schema": 'etl', "table_name": "table_params", "col_name": "table_schema varchar, table_name varchar, column_name varchar"},
            #runtime_parameters={'statement_timeout': '3000ms'},
            #provide_context=True
            )
            t12 = DataSourceFromCSV(
            task_id='data_from_csv_to_source_tb',
            csv_file_path=tabel_name['csv_path'],
            csv_file_name=tabel_name['structure_tb'],
            target_table='table_params'
            )
            t1 >> t11 >> t12
        
        with TaskGroup('group2') as group2:
            t2 = DataSourceToCSV(
                task_id='data_from_view_db_to_csv',
                sql=dag_config['sql_view'],
                csv_file_path=tabel_name['csv_path'],
                csv_file_name=tabel_name['view_tb']
                )
            t21= PostgresOperator(
                task_id='create_view_tb',
                postgres_conn_id=dag_config['pg_conn_name'],
                sql=dag_config['sql_create_view_tb'],
                database=dag_config['pg_db'],
                )
            t22 = DataSourceFromCSV(
                task_id='data_from_csv_to_view_tb',
                csv_file_path=tabel_name['csv_path'],
                csv_file_name=tabel_name['view_tb'],
                target_table='view_table_params'
                )
            t2 >> t21 >> t22 
        
        with TaskGroup('group3') as group3:
            t3 = DataSourceToCSV(
                task_id='data_from_function_db_to_csv',
                sql=dag_config['sql_functions'],
                csv_file_path=tabel_name['csv_path'],
                csv_file_name=tabel_name['functions_tb']
                )
            t31= PostgresOperator(
                task_id='create_function_tb',
                postgres_conn_id=dag_config['pg_conn_name'],
                sql=dag_config['sql_create_functions_tb'],
                database=dag_config['pg_db'],
                ##parameters={"schema": 'etl', "table_name": "table_params", "col_name": "table_schema varchar, table_name varchar, column_name varchar"},
                #runtime_parameters={'statement_timeout': '3000ms'},
                #provide_context=True
                )
            t32 = DataSourceFromCSV(
                task_id='data_from_csv_to_functions_tb',
                csv_file_path=tabel_name['csv_path'],
                csv_file_name=tabel_name['functions_tb'],
                target_table='functions_table_params'
                )
            t3 >> t31 >> t32
        with TaskGroup('group4') as group4:
            t4 = DataSourceToCSV(
                task_id='data_from_grants_db_to_csv',
                sql=dag_config['sql_grants'],
                csv_file_path=tabel_name['csv_path'],
                csv_file_name=tabel_name['grants_tb']
                )
            t41= PostgresOperator(
                task_id='create_grants_tb',
                postgres_conn_id=dag_config['pg_conn_name'],
                sql=dag_config['sql_create_grants_tb'],
                database=dag_config['pg_db'],
                )
            t42 = DataSourceFromCSV(
                task_id='data_from_csv_to_grants_tb',
                csv_file_path=tabel_name['csv_path'],
                csv_file_name=tabel_name['grants_tb'],
                target_table='grants_table_params'
                )
            t4 >> t41 >> t42

        with TaskGroup('group5') as group5:
            t5 = DataSourceToCSV(
                task_id='data_from_key_db_to_csv',
                sql=dag_config['sql_key'],
                csv_file_path=tabel_name['csv_path'],
                csv_file_name=tabel_name['key_tb']
                )
            t51= PostgresOperator(
                task_id='create_key_tb',
                postgres_conn_id=dag_config['pg_conn_name'],
                sql=dag_config['sql_create_key_tb'],
                database=dag_config['pg_db'],   
                )
            t52 = DataSourceFromCSV(
                task_id='data_from_csv_to_key_tb',
                csv_file_path=tabel_name['csv_path'],
                csv_file_name=tabel_name['key_tb'],
                target_table='key_table_params'
                )
            t5 >> t51 >> t52

        scoup_task = [group1, group2, group3, group4, group5]

s0 >> scoup >> union_branch >> t6 >> t7