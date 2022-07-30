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
    'sql_functions':'''select routine_schema, routine_name, data_type,external_language,routine_definition,routine_type, 'CREATE OR REPLACE FUNCTION '||routine_schema||'.'||routine_name||'() RETURNS '||data_type||' LANGUAGE '||lower(external_language)||' AS $$'||routine_definition||' $$;' as script
                       from information_schema.routines
                       where specific_schema not in ('pg_catalog', 'information_schema', 'public');''',
    'sql_create_functions_tb':'''CREATE TABLE IF NOT EXISTS etl.functions_table_params (routine_schema varchar, routin_name varchar, data_type varchar, external_language varchar, routine_definition varchar, routine_type varchar, script varchar);''',
    'sql_grants':'''select * 
                    from information_schema.column_privileges
                    where table_schema not in ('pg_catalog', 'information_schema', 'public')''',
    'sql_create_grants_tb':'''CREATE TABLE IF NOT EXISTS etl.grants_table_params (grantor varchar, grantee varchar,
                                                                           table_catalog varchar, table_schema varchar,
                                                                           table_name varchar, column_name varchar,
                                                                           privilege_type varchar, is_grantable varchar);''',
    'sql_key':'''SELECT
                        tc.table_schema, 
                        tc.constraint_name, 
                        tc.table_name, 
                        kcu.column_name, 
                        ccu.table_schema AS foreign_table_schema,
                        ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name,
                        CASE
                            when tc.constraint_type  = 'PRIMARY KEY' then 1
                            else 0
                        END as kind_key
                    FROM 
                        information_schema.table_constraints AS tc 
                        JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                        JOIN information_schema.constraint_column_usage AS ccu
                        ON ccu.constraint_name = tc.constraint_name
                        AND ccu.table_schema = tc.table_schema
                    WHERE tc.constraint_type in ('FOREIGN KEY', 'PRIMARY KEY')''',
    'sql_create_key_tb':'''CREATE TABLE IF NOT EXISTS etl.key_table_params (table_schema varchar, constraint_name varchar,
                                                                           table_name varchar, column_name varchar,
                                                                           foreign_table_schema varchar, foreign_table_name varchar,
                                                                           foreign_column_name varchar, kind_key integer);'''
}

tabel_name = {
    'csv_path':'/opt/airflow/dags/table/',
    'structure_tb':'structure_tb.csv',
    'view_tb':'view_tb.csv',
    'functions_tb':'functions_tb.csv',
    'grants_tb':'grants_tb.csv',
    'key_tb':'key_tb.csv'
}