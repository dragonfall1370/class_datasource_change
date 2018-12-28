select convert(varchar(50), convert(date, replace(trim(isnull('2010-9', '')), '-', '/')), 111)

select convert(date, replace(trim(isnull('2010-09', '')), '-', ''), 112)

declare @aDateStr nvarchar(max) = '1990'

select
iif(len(trim(isnull(@aDateStr, ''))) = 0
	, ''
	, concat(
		',"dateRangeTo":"'
		, iif(len(trim(isnull(@aDateStr, ''))) = 4
			, convert(varchar(50), convert(date, concat(trim(isnull(@aDateStr, '')), '/01/01'), 111))
			, iif(len(trim(isnull(@aDateStr, ''))) = 7
				, convert(varchar(50), convert(date, concat(replace(trim(isnull(@aDateStr, '')), '-', '/'), '/01')), 111)
				, iif(len(trim(isnull(@aDateStr, ''))) = 10
					, convert(varchar(50), convert(date, replace(trim(isnull(@aDateStr, '')), '-', '/'), 111))
					, convert(varchar(50), getdate(), 111)
				)
			)
		)
		, '"'
	)
)

select
iif(len(trim(isnull(@aDateStr, ''))) = 0
	, convert(varchar(50), getdate(), 111)
	, iif(len(trim(isnull(@aDateStr, ''))) = 4
		, convert(date, concat(@aDateStr, '-01-01'))
		, iif(len(trim(isnull(@aDateStr, ''))) = 7
			, convert(date, concat(@aDateStr, '-01'))
			, iif(len(trim(isnull(@aDateStr, ''))) = 10
				, convert(date, trim(isnull(@aDateStr, '')))
				, convert(varchar(50), getdate(), 111)
			)
		)
	)
)

