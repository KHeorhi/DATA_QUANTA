/*CREATE OR REPLACE FUNCTION fnc_create_ddl()
RETURNS void
LANGUAGE 'plpgsql'
AS $$
DECLARE
    schemas_names record;
    tables_names record;
	colum record;
BEGIN
     FOR schemas_names in 
	 (
      select distinct table_schema 
      from etl.table_params
      )LOOP
	  	  	EXECUTE format('CREATE SCHEMA %s;', schemas_names.table_schema);
          FOR tables_names in 
			 (
               SELECT table_name
               FROM etl.table_params
               WHERE table_schema = schemas_names.table_schema
              )LOOP
                	EXECUTE format('CREATE TABLE %s'||'.'||'%s();', schemas_names.table_schema, tables_names.table_name);     
                FOR colum in (
                                SELECT unnest(string_to_array(btrim(column_name,'{","}'), '","')) as column_name
                                FROM etl.table_params
                                WHERE table_name = tables_names.table_name
                             )LOOP
                                execute format('ALTER TABLE %s'||'.'||'%s ADD COLUMN %s;', schemas_names.table_schema, tables_names.table_name, colum.column_name);
                             END LOOP;                           
              END LOOP;
      END LOOP;	
END;
$$;

select fnc_create_ddl();
*/

CREATE OR REPLACE FUNCTION fnc_create_ddl()
RETURNS void
LANGUAGE 'plpgsql'
AS $$
DECLARE
    schemas_names record;
    tables_names record;
	colum record;
BEGIN
     FOR schemas_names in 
	 (
      select distinct table_schema 
      from etl.table_params
      )LOOP
	  	  	EXECUTE format('CREATE SCHEMA IF NOT EXISTS %s;', schemas_names.table_schema);
          FOR tables_names in 
			 (
               SELECT table_name
               FROM etl.table_params
               WHERE table_schema = schemas_names.table_schema
              )LOOP
                	EXECUTE format('CREATE TABLE IF NOT EXISTS %s'||'.'||'%s();', schemas_names.table_schema, tables_names.table_name);     
                FOR colum in (
                                SELECT unnest(string_to_array(btrim(replace(column_name,'''', ''),'[]'),',')) as column_name
                                FROM etl.table_params
                                WHERE table_name = tables_names.table_name
                             )LOOP
                                execute format('ALTER TABLE %s'||'.'||'%s ADD COLUMN IF NOT EXISTS %s;', schemas_names.table_schema, tables_names.table_name, colum.column_name);
                             END LOOP;                           
              END LOOP;
      END LOOP;	
END;
$$;

/*select fnc_create_ddl();*/