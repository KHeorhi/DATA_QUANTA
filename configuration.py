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
    'sql_source_from': """select table_schema, table_name, array_agg(concat_ws(' ' , column_name, data_type, is_nullable)) 
                    from (select table_schema, table_name,  column_name,
                            case 
	                            when data_type in ('character','bit' ) then data_type||'('||character_maximum_length||')'
			                    when data_type in ('ARRAY') then substring(udt_name, 2)||' array'
			                else data_type
	                        end as data_type, 
	                        case 
		                        when is_nullable = 'NO' then 'NOT NULL'
		                        when is_nullable = 'YES' then ''
	                        end as is_nullable
                          from information_schema.columns
                          where table_schema not in ('pg_catalog', 'information_schema', 'public')
                            ) as one
                    group by table_schema, table_name;""",
    'schema':'etl',
    'table_name':'tale_params',
    'col_name': "table_schema varchar, table_name varchar, column_name varchar",
    'sql_create_source_tb':'''CREATE TABLE IF NOT EXISTS etl.table_params (table_schema varchar, table_name varchar, column_name varchar);''',
    'sql_view':"""select table_schema, table_name, view_definition
                  from information_schema.views
                  where table_schema not in ('pg_catalog', 'information_schema', 'public');""",
    'sql_create_view_tb':'''CREATE TABLE IF NOT EXISTS etl.view_table_params (table_schema varchar, table_name varchar, view_definition varchar);''',
    'sql_functions':'''select routine_schema, data_type, external_language, routine_definition, routine_type
                       from information_schema.routines
                       where specific_schema not in ('pg_catalog', 'information_schema', 'public');''',
    'sql_functions_tb':'''CREATE TABLE IF NOT EXISTS etl.functions_table_params (routine_schema varchar, data_type varchar, external_language varchar, routine_definition varchar, routine_type varchar);''',
    'sql_grants':''''''
}

tabel_name = {
    'csv_path':'/opt/airflow/dags/table/',
    'structure_tb':'structure_tb.csv',
    'view_tb':'view_tb.csv',
    'functions_tb':'functions_tb.csv',
    'grants_tb':'grants_tb.csv',
    'key_tb':'key_tb.csv'
}