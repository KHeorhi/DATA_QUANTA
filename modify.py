from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime
from exemp import DataSourceToCSV, DataSourceFromCSV

def line_task(config:dict, table:dict):
        with TaskGroup('scoup_task') as scoup:
                task = list()
                for group in list(config.keys()):
                        with TaskGroup(group_id=group) as group1:
                                t1 = DataSourceToCSV(
                                                task_id=config[group][0]['task_id'],
                                                sql=config[group][0]['sql'],
                                                csv_file_path=table['csv_path'],
                                                csv_file_name=table['structure_tb']
                                        )
                                t2= PostgresOperator(
                                        task_id=config[group][1]['task_id'],
                                        postgres_conn_id=config[group][1]['postgres_conn_id'],
                                        sql=config[group][1]['sql'],
                                        database=config[group][1]['database'],
                                        ##parameters={"schema": 'etl', "table_name": "table_params", "col_name": "table_schema varchar, table_name varchar, column_name varchar"},
                                        #runtime_parameters={'statement_timeout': '3000ms'},
                                        #provide_context=True
                                        )
                                t3 = DataSourceFromCSV(
                                        task_id=config[group][2]['task_id'],
                                        csv_file_path=table['csv_path'],
                                        csv_file_name=table['structure_tb'],
                                        target_table=config[group][2]['target_tables']
                                        )
                                t1>>t2>>t3
                        task.append(group1)
        return scoup