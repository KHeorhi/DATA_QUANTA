from airflow.models.baseoperator import BaseOperator
import psycopg2
from psycopg2 import Error
from requests import options
from configuration import db_from, db_to, dag_config
import csv


sql_ = """select table_schema, table_name, array_agg(column_name||' '||data_type) as column_name 
                            from information_schema.columns
                            where table_schema not in ('pg_catalog', 'information_schema', 'public')
                            group by table_schema, table_name
                            order by 1;"""

#csv_name = 'db'

class DataSourceToCSV(BaseOperator):

    def __init__(
            self,
            sql: str,
            csv_file_path,
            csv_file_name,
            *args,**kwargs):
        super(DataSourceToCSV, self).__init__(*args, **kwargs)
        self.sql = sql,
        self.csv_file_path = csv_file_path,
        self.csv_file_name = csv_file_name

    def execute(self, context):
        try:
            # Подключение к существующей базе данных
            connection = psycopg2.connect(user=db_from['user'],
                                          password=db_from['password'],
                                          #'host':'host.docker.internal',
                                          host=db_from['host'],
                                          port=db_from['port'],
                                          database=db_from['database']
                                          )

            # Курсор для выполнения операций с базой данных
            cursor = connection.cursor()
            # Выполнение SQL-запроса
            cursor.execute(dag_config['sql_from'])
            # Получить результат
            record = cursor.fetchall()
            
            n = ''.join(self.csv_file_path)            
            temp_file = f'{n}{self.csv_file_name}'
            
            with open(temp_file, mode='w') as f:
                writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_NONE)
                writer.writerow([i[0] for i in cursor.description])
                #writer.writerows(record)
                writer.writerows(record)

        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("Соединение с PostgreSQL закрыто")


class DataSourceFromCSV(BaseOperator):

    def __init__(
            self,
            csv_file_path: str,
            csv_file_name:str,
            *args,**kwargs):
        super().__init__(*args, **kwargs)
        
        self.csv_file_path = csv_file_path
        self.csv_file_name = csv_file_name

    def execute(self, context):
        try:
            # Подключение к существующей базе данных
            connection = psycopg2.connect(user=db_to['user'],
                                          password=db_to['password'],
                                          #'host':'host.docker.internal',
                                          host=db_to['host'],
                                          port=db_to['port'],
                                          database=db_to['database'],
                                          options=db_to['options'])

            # Курсор для выполнения операций с базой данных
            cursor = connection.cursor()

            n = ''.join(self.csv_file_path)            
            temp_file = f'{n}{self.csv_file_name}'

            with open(temp_file, mode='r') as f:
                reader = csv.reader(f)
                next(reader)
                cursor.copy_from(f, 'table_params', sep='|')
                connection.commit()
    
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("Соединение с PostgreSQL закрыто")