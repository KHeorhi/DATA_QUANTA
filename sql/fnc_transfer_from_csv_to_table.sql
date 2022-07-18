CREATE OR REPLACE FUNCTION fnc_transfer_from_csv_to_table
(
	csv_path text,
	schema_names text,
	table_names text,
	check_delimiter text
)
RETURNS void
LANGUAGE plpgsql
AS $$

/* creator: Yura Kirpichenko 
   created: 2022-07-06
   example:
   		  select fnc_transfer_from_csv_to_table(\'D:\table_structure.csv\', 'etl', 'table_params', \'|'\);
*/

BEGIN
	execute format('COPY %s'||'.'||'%s FROM %s DELIMITER %s CSV HEADER;', schema_names, table_names, csv_path, check_delimiter);
END;
$$;