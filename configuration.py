db_from = {
               'user':'postgres',
               'password':'123',
               'host':'host.docker.internal',
               #'host':'localhost',
               'port':"5433",
               'database':"demo"              
}

db_to = {
            'user':'postgres',
            'password':'123',
            'host':'host.docker.internal',
            #'host':'localhost',
            'port':"5433",
            'database':"quanta",
            'options':"-c search_path=etl,public"
}

dag_config = {
    'pg_conn_name':'quanta_conn',
    'pg_db':'quanta',
    'sql_from': """select table_schema, table_name, array_agg(case
									when data_type = 'ARRAY' then column_name||' '||substr(udt_name,2)||' array'
									else column_name||' '||data_type
								 end) as column_name
         from information_schema.columns
         where table_schema not in ('pg_catalog', 'information_schema', 'public')
         group by table_schema, table_name
         order by 1;""",
    'schema':'etl',
    'table_name':'tale_params',
    'col_name': "table_schema varchar, table_name varchar, column_name varchar",
    'sql_to':'''CREATE TABLE IF NOT EXISTS etl.table_params (table_schema varchar, table_name varchar, column_name varchar);''',
    'csv_file_path':'/opt/airflow/dags/table'
}