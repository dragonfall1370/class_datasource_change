declare @NewLineChar char(1) = char(10);

select
	x.CLNT_ID as external_id
	--, cast('-10' as int) as 'user_account_id'
	, trim(isnull(x.USER_ID, '')) as UserEmail
	, 'comment' as category
	, 'company' as type
	, cast(x.ENTERED as datetime) as insert_timestamp
	, concat(
		nullif(concat('Due: ', FORMAT(cast(x.DUE as datetime), 'dd-MMM-yyyy H:mm:ss', 'en-gb')), concat('Due: ', null))
		, nullif(
			concat(@NewLineChar, 'Completed: '
			, case(x.COMPLETED)
				when '1' then 'Yes'
				when '0' then 'No'
				end
			)
			, concat(@NewLineChar, 'Completed: '))
		, nullif(concat(@NewLineChar, 'Summary: ', trim(isnull(x.SUMMARY, ''))), concat(@NewLineChar, 'Summary: '))
		, nullif(concat(@NewLineChar, 'Description:', @NewLineChar, trim(isnull(x.DESCRIPTION, ''))), concat(@NewLineChar, 'Description:', @NewLineChar))
	) as content

from CALLBACKS_DATA_TABLE x
where x.CLNT_ID is not null
and cast(x.CLNT_ID as varchar(10)) in (select [company-externalId] from VC_Com)
--and cast(x.ENTERED as datetime) > dateadd(month, -6, getdate())
order by cast(x.ENTERED as datetime)