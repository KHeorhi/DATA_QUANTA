CREATE OR REPLACE FUNCTION etl.fnc_create_ddl()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
declare
	schemas_names record;
    tables_names record;
	colum record;
    funcs cursor is select * from etl.functions_table_params;
    ex_func text;
    _views cursor is select * from etl.view_table_params;
    ex_view text;
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
               WHERE table_schema = schemas_names.table_schema and table_name not in (select table_name 
																					  from etl.view_table_params
																					 )
              )LOOP
                	EXECUTE format('CREATE TABLE IF NOT EXISTS %s'||'.'||'%s();', schemas_names.table_schema, tables_names.table_name);     
                FOR colum in (
                                SELECT unnest(string_to_array(btrim(replace(column_name,'''', ''),'[]'),',')) as column_name
                                FROM etl.table_params
                                WHERE table_name = tables_names.table_name and table_name not in (select table_name 
																					  from etl.view_table_params
																					 )
                             )LOOP
                                execute format('ALTER TABLE %s'||'.'||'%s ADD COLUMN IF NOT EXISTS %s;', schemas_names.table_schema, tables_names.table_name, colum.column_name);
                             END LOOP;                           
              END LOOP;
      END LOOP;
      FOR func in funcs loop
      	ex_func := func.script;
        execute ex_func;
      END LOOP;
      set search_path to bookings, public;--ошибка. необходимо подумать над заданием пути поиска через цикл возможно 
      for _view in _views loop
      	ex_view := 'CREATE OR REPLACE VIEW '||_view.table_schema||'.'||_view.table_name||' AS'||_view.view_definition;
        execute ex_view;      	
      end loop;
END;
$function$
;