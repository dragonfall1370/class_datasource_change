declare @chars4trim varchar(10) = char(10) + char(13) + char(9) + '%?*,. '
declare @dummyNote varchar(max) = ''

drop table if exists VC_Job

;with
TmpTab1 as (
	select
	x.JOBS_ID as JobId
	, isnull(y.[contact-companyId], '_DefCom000') as ComId
	, isnull(cast(x.CONT_ID as varchar), '_DefCon000') as ConId
	, iif(len(trim(@chars4trim from isnull(x.TITLE, 'No Job Title'))) > 0
		, trim(@chars4trim from isnull(x.TITLE, 'No Job Title'))
		, 'No Job Title'
	) as Title
	, row_number() over(partition by y.[contact-companyId], trim(@chars4trim from isnull(x.TITLE, '')) order by x.JOBS_ID) rn
	from
	JOBS_DATA_TABLE x
	left join VC_Con y on cast(x.CONT_ID as varchar) = y.[contact-externalId]
)

--select * from TmpTab1

, TmpTab2 as (
	select
	x.JobId
	, x.ComId
	, x.ConId
	, iif(rn = 1,
		iif(x.Title = 'No Job Title'
			, concat('[', x.Title, ']')
			, x.Title
		)
		, iif(x.Title = 'No Job Title'
			, concat('[', x.Title, ' ', replicate('0', 2 - len(cast(rn as varchar))), rn, ']')
			, concat(x.Title, ' [', replicate('0', 2 - len(cast(rn as varchar))), rn, ']')
		)
	) as Title
	from TmpTab1 x
)

--select * from TmpTab2
--where len(Title) = 0

, DocTmpTab as (
	select
	x.JobId
	, string_agg(y.Name, ',') as Docs
	from TmpTab2 x
	left join VC_DocsIdx y on x.JobId = y.JobID
	where len(trim(isnull(y.Name, ''))) > 0
	group by x.JobId
)

--select * from DocTmpTab

select

x.ConId as [position-contactId]

, x.JobId as [position-externalId]

, x.Title as [position-title]

, trim(@chars4trim from isnull(cast(y.positions as varchar(10)), '1')) as [position-headcount]

, trim(@chars4trim from isnull(y.user_id, '')) as [position-owners]

, case(lower(trim(@chars4trim from isnull(y.type, 'Permanent'))))
	when lower('Permanent') then 'PERMANENT'
	when lower('Contract') then 'CONTRACT'
	when lower('Permanent Part-time') then 'PERMANENT'
	when lower('Temp and Perm') then 'PERMANENT'
	when lower('Temporary') then 'TEMPORARY'
	when lower('Temporary Part-time') then 'TEMPORARY'
end as [position-type]

, 'GBP' as [position-currency]

, trim(@chars4trim from isnull(cast(y.salary as varchar(20)), '0')) as [position-actualSalary]

--, trim(@chars4trim from isnull(y.rate, '')) as [position-payRate]

--, trim(@chars4trim from isnull(y.cont_id, '')) as [position-contractLength]

--, convert(varchar(10), isnull(cast(y.STARTING as datetime), dateadd(day, -7, getdate())), 120) as [position-startDate1]
, convert(varchar(10), dateadd(day, -7, getdate()), 120) as [position-startDate]

--, convert(varchar(10), isnull(cast(y.ENDING as datetime), dateadd(day, -7, getdate())), 120) as [position-endDate]
, convert(varchar(10), dateadd(day, -7, getdate()), 120) as [position-endDate]

, isnull(z.Docs, '') as [position-document]

--, trim(@chars4trim from isnull(y.fee, '')) as [position-otherDocument]
, concat(
	nullif(
		concat(
			'Entered: '
			, FORMAT(dateadd(day, -2, cast(y.entered as datetime)), 'dd-MMM-yyyy H:mm:ss', 'en-gb')
			, char(10)
		)
		, concat('Entered: ', char(10))
	)
	, nullif(
		concat('Salary to: '
			, FORMAT(y.SALARYHIGH, '#,#', 'en-gb')
			, ' GBP'
			, char(10)
		)
		, concat('Salary to: ', ' GBP', char(10))
	)
	, nullif(
		concat('Fee: '
			, FORMAT(FEE, '#', 'en-gb')
			, '%'
			, char(10)
		)
		, concat('Fee: ', '%', char(10))
	)
) as [position-note]

, trim(@chars4trim from isnull(y.desc_internal, '')) as [position-publicDescription]

, trim(@chars4trim from isnull(y.desc_internal, '')) as [position-internalDescription]

into VC_Job

from
TmpTab2 x
left join JOBS_DATA_TABLE y on x.JobId = y.JOBS_ID
left join DocTmpTab z on x.JobId = z.JobId

select * from VC_Job
--where [position-contactId] not in (select [contact-externalId] from VC_Con)
order by [position-externalId]

--select count(*) from JOBS_DATA_TABLE where ENTERED is null and FEE is null and SALARYHIGH is null