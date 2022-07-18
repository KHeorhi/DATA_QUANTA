CREATE OR REPLACE FUNCTION fnc_copy_structure_db_to_csv(
    path_to_dwnld text,
	check_delimiter text,
	name_table text
	)
    RETURNS void
    LANGUAGE 'plpgsql'
AS $$
	/*
	creator: Yury Kirpichenko 
	created: 2022-07-06
	example:select fnc_copy_structure_db_to_csv('D:\coding\Postgresql\table_ext\', '|', 'db.csv')
	*/
BEGIN
	execute format('COPY (select table_schema, table_name, array_agg(column_name||'' ''||data_type) as column_name
		  from information_schema.columns
		  where table_schema not in (''pg_catalog'', ''information_schema'', ''public'')
		  group by table_schema, table_name
		  order by 1
		 ) TO %s'||'%s DELIMITER %s CSV HEADER;', path_to_dwnld, name_table,check_delimiter);
END;
$$;

select fnc_copy_structure_db_to_csv($$'table/$$, $$'|'$$, $$db.csv'$$);