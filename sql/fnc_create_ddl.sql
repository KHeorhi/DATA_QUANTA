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
    _keys cursor is select * from etl.key_table_params;
    key_one text;
    key_two text;
    key_three text;
    key_four text;
    key_five text;
   	key_six text;
   	key_ex text;
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
      set search_path to bookings, public;
      FOR _view in _views LOOP
      	ex_view := 'CREATE OR REPLACE VIEW '||_view.table_schema||'.'||_view.table_name||' AS'||_view.view_definition;
        execute ex_view;      	
      END LOOP;
      for _key in _keys loop
        key_one := 'ALTER TABLE ';
        key_two := ' ADD CONSTRAINT ';
        key_three := ' PRIMARY KEY ';
        key_four := ' FOREIGN KEY ';
        key_five := ' REFERENCES ';
        key_six := ' UNIQUE ';
      	IF _key.constraint_type = 'P'
          THEN 
       		key_ex := key_one||_key.table_name||key_two||_key.constraint_name||key_three||'('||_key.columns||');' ;
      	elsif _key.constraint_type = 'U'
      	  THEN 
      	 	key_ex := key_one||_key.table_name||key_two||_key.constraint_name||key_six||'('||_key.columns||');' ;
       	else
      		key_ex := key_one||_key.table_name||key_two||_key.constraint_name||key_four||'('||_key.columns||')'||key_five||_key.foreign_table||'('||_key.column_name||');';
        end if;
        execute key_ex;
      end loop;
END;
$function$
;