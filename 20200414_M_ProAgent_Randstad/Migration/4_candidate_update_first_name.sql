--UPDATE FIRST NAME WITH double byte space characters
update candidate
set first_name = replace(first_name, '氏名', N'　')
, first_name_kana = replace(first_name_kana, 'フリガナ', N'　')
where external_id ilike 'CDT%'
and deleted_timestamp is NULL