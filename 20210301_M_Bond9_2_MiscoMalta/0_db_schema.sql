--Check DB schema V9.2
select table_name, column_name, ordinal_position
FROM information_schema.columns
where table_schema = 'public'
order by table_name, ordinal_position
