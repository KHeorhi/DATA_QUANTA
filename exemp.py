from airflow.models.baseoperator import BaseOperator
import psycopg2
from psycopg2 import Error
from configuration import db_config
import csv


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
            connection = psycopg2.connect(user=db_config['db_from']['user'],
                                          password=db_config['db_from']['password'],
                                          #'host':'host.docker.internal',
                                          host=db_config['db_from']['host'],
                                          port=db_config['db_from']['port'],
                                          database=db_config['db_from']['database']
                                          )

            # Курсор для выполнения операций с базой данных
            cursor = connection.cursor()
            # Выполнение SQL-запроса
            cursor.execute(''.join(self.sql))
            # Получить результат
            record = cursor.fetchall()
            
            n = ''.join(self.csv_file_path)            
            temp_file = f'{n}{self.csv_file_name}'
            
            with open(temp_file, mode='w') as f:
                writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_NONE, quotechar='', escapechar='\\')
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
            target_table:str,
            *args,**kwargs):
        super().__init__(*args, **kwargs)
        self.target_table = target_table
        self.csv_file_path = csv_file_path
        self.csv_file_name = csv_file_name

    def execute(self, context):
        try:
            # Подключение к существующей базе данных
            connection = psycopg2.connect(user=db_config['db_to']['user'],
                                          password=db_config['db_to']['password'],
                                          #'host':'host.docker.internal',
                                          host=db_config['db_to']['host'],
                                          port=db_config['db_to']['port'],
                                          database=db_config['db_to']['database'],
                                          options=db_config['db_to']['options'])

            # Курсор для выполнения операций с базой данных
            cursor = connection.cursor()

            n = ''.join(self.csv_file_path)            
            temp_file = f'{n}{self.csv_file_name}'

            with open(temp_file, mode='r') as f:
                reader = csv.reader(f)
                next(reader)
                cursor.copy_from(f, self.target_table, sep='|')
                connection.commit()
    
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("Соединение с PostgreSQL закрыто")