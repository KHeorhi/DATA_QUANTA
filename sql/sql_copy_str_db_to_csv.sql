select table_schema, 
       table_name, array_agg(column_name||'' ''||data_type) as column_name
    from information_schema.columns
	where table_schema not in (''pg_catalog'', ''information_schema'', ''public'')
	group by table_schema, table_name
	order by 1;