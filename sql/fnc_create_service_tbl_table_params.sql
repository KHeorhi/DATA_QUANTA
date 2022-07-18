CREATE OR REPLACE FUNCTION  fnc_create_service_tbl_table_params
(
	name_schema text,
	name_table text,
	-- нужно подумать реализацию через json
	name_col_vs_type text
)
RETURNS void
LANGUAGE plpgsql AS $$

/*

	creator: Yura Kirpichenko - 2022-07-06
 	example:
 	select fnc_create_service_tbl_table_params('etl', 'table_params', 'table_schema varchar, table_name varchar, column_name varchar');

*/

BEGIN
	EXECUTE format('CREATE TABLE %s'||'.'||'%s (%s);', name_schema, name_table, name_col_vs_type);
END;
$$;