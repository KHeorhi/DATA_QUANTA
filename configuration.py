db_from = {
               'user':'postgres',
               'password':'123',
               'host':'host.docker.internal',
               'port':"5433",
               'database':"demo"              
}

db_to = {
            'user':'postgres',
            'password':'123',
            'host':'host.docker.internal',
            'port':"5433",
            'database':"quanta",
            'options':"-c search_path=etl,public"
}

##подумать над view и tables. объединять по table и column

db_config = {
    'db_from':{
               'user':'postgres',
               'password':'123',
               'host':'host.docker.internal',
               'port':"5433",
               'database':"demo"              
            },
    'db_to':{
                'user':'postgres',
                'password':'123',
                'host':'host.docker.internal',
                'port':"5433",
                'database':"quanta",
                'options':"-c search_path=etl,public"
            }
}

dag_config_1 = {
                'group0': [
                          {
                            'task_id': 'data_from_source_db_to_csv',
                            'sql':"""select table_schema, table_name, array_agg(concat_ws(' ' , column_name, data_type, is_nullable)) 
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
                                      group by table_schema, table_name;;
                                  """,
                            'csv_file_path':'/opt/airflow/dags/table/',
                            'csv_file_name':'structure_tb.csv'
                          },
                          {
                            'task_id' : 'create_source_tb',
                            'postgres_conn_id':'quanta_conn',
                            'sql':'''CREATE TABLE IF NOT EXISTS etl.table_params (table_schema varchar, table_name varchar, column_name varchar);''',
                            'database':'quanta'
                          },
                          {
                            'task_id': 'data_from_csv_to_source_tb',
                            'target_table':'table_params',
                            'csv_file_path':'/opt/airflow/dags/table/',
                            'csv_file_name':'structure_tb.csv'
                          }
                ],
                        
                'group1':[
                          {
                            'task_id': 'data_from_view_db_to_csv',
                            'sql':"""select table_schema, table_name, view_definition
                                     from information_schema.views
                                     where table_schema not in ('pg_catalog', 'information_schema', 'public');
                                    """,
                            'csv_file_path':'/opt/airflow/dags/table/',
                            'csv_file_name':'view_tb.csv'
                          },
                          {
                            'task_id' : 'create_view_tb',
                            'postgres_conn_id':'quanta_conn',
                            'sql':'''CREATE TABLE IF NOT EXISTS etl.view_table_params (table_schema varchar, table_name varchar, view_definition varchar);''',
                            'database':'quanta'
                          },
                          {
                            'task_id': 'data_from_csv_to_view_tb',
                            'target_table':'view_table_params',
                            'csv_file_path':'/opt/airflow/dags/table/',
                            'csv_file_name':'view_tb.csv'
                          }
                ],
                'group2':[
                          {
                            'task_id': 'data_from_function_db_to_csv',
                            'sql':'''select routine_schema, routine_name, data_type,
                                            external_language,routine_definition,
                                            routine_type, 'CREATE OR REPLACE FUNCTION '||routine_schema||'.'||routine_name||'() RETURNS '||data_type||' LANGUAGE '||lower(external_language)||' AS $$'||routine_definition||' $$;' as script
                                     from information_schema.routines
                                     where specific_schema not in ('pg_catalog', 'information_schema', 'public');
                                  ''',
                            'csv_file_path':'/opt/airflow/dags/table/',
                            'csv_file_name':'functions_tb.csv'
                          },
                          {
                            'task_id' : 'create_function_tb',
                            'postgres_conn_id':'quanta_conn',
                            'sql':'''CREATE TABLE IF NOT EXISTS etl.functions_table_params (routine_schema varchar, routin_name varchar, data_type varchar, external_language varchar, routine_definition varchar, routine_type varchar, script varchar);''',
                            'database':'quanta'
                          },
                          {
                            'task_id': 'data_from_csv_to_functions_tb',
                            'target_table':'functions_table_params',
                            'csv_file_path':'/opt/airflow/dags/table/',
                            'csv_file_name':'functions_tb.csv'
                          }
                ],
                'group3':[
                          {
                            'task_id': 'data_from_grants_db_to_csv',
                            'sql':'''select * 
                                     from information_schema.column_privileges
                                     where table_schema not in ('pg_catalog', 'information_schema', 'public')
                                     ''',
                            'csv_file_path':'/opt/airflow/dags/table/',
                            'csv_file_name':'grants_tb.csv'
                          },
                          {
                            'task_id' : 'create_grants_tb',
                            'postgres_conn_id':'quanta_conn',
                            'sql':'''CREATE TABLE IF NOT EXISTS etl.grants_table_params (grantor varchar, grantee varchar,
                                                                           table_catalog varchar, table_schema varchar,
                                                                           table_name varchar, column_name varchar,
                                                                           privilege_type varchar, is_grantable varchar);''',
                            'database':'quanta'
                          },
                          {
                            'task_id': 'data_from_csv_to_grants_tb',
                            'target_table':'grants_table_params',
                            'csv_file_path':'/opt/airflow/dags/table/',
                            'csv_file_name':'grants_tb.csv'
                          }
                ],
                'group4':[
                          {
                            'task_id': 'data_from_key_db_to_csv',
                            'sql':'''with two as (select table_name, constraint_name, array_to_string(array_agg(column_name),',') as columns, 
                                                  case 
                                                    when constraint_type = 'PRIMARY KEY' then 'P'
                                                    when constraint_type = 'FOREIGN KEY' then 'F'
                                                    when constraint_type = 'UNIQUE' then 'U'
                                                    end as constraint_type
                                                  from ( select FORMAT('%s.%s', tc.table_schema, tc.table_name) AS table_name,
                                                          tc.constraint_name, kcu.column_name, tc.constraint_type
                                                          FROM information_schema.table_constraints tc
                                                          JOIN information_schema.key_column_usage AS kcu
                                                              ON tc.constraint_name = kcu.constraint_name
                                                              AND tc.table_schema = kcu.table_schema
                                                          WHERE constraint_type in ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY')
                                                        ) as one
                                                  group by table_name, constraint_name,constraint_type)
                                      select distinct two.table_name as table_name, two.constraint_name as constraint_name, two.columns as columns,
                                             two.constraint_type as constraint_type, format('%s.%s', ccu.table_schema, ccu.table_name) as foreign_table,
                                             array_to_string(array_agg(ccu.column_name), ',') as column_name
                                      from two
                                      JOIN information_schema.constraint_column_usage AS ccu
                                            ON ccu.constraint_name = two.constraint_name
                                      group by two.table_name, two.constraint_name, two.columns, two.constraint_type, foreign_table
                                      order by constraint_type desc
                                ''',
                            'csv_file_path':'/opt/airflow/dags/table/',
                            'csv_file_name':'key_tb.csv'
                          },
                          {
                            'task_id' : 'create_key_tb',
                            'postgres_conn_id':'quanta_conn',
                            'sql':'''CREATE TABLE IF NOT EXISTS etl.key_table_params (table_name varchar, constraint_name varchar, columns varchar,
                                                                            constraint_type varchar, foreign_table varchar, column_name varchar);
                                  ''',
                            'database':'quanta'
                          },
                          {
                            'task_id': 'data_from_csv_to_key_tb',
                            'target_table':'key_table_params',
                            'csv_file_path':'/opt/airflow/dags/table/',
                            'csv_file_name':'key_tb.csv'
                          }
                ],                        
}