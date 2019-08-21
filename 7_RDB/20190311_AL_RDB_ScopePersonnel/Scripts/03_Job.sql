declare @chars4trim varchar(10) = char(10) + char(13) + char(9) + '%?*,. '
declare @dummyNote varchar(max) = ''
declare @NewLineChar as char(1) = char(10);

drop table if exists VC_Job

;with
TmpTab1 as (
	select
	JobId
	, isnull(y.[contact-companyId], '_DefCom000') as ComId
	, isnull(cast(x.ClientContactId as varchar), '_DefCon000') as ConId
	, iif(len(trim(@chars4trim from isnull(x.JobTitle, 'No Job Title'))) > 0
		, trim(@chars4trim from isnull(x.JobTitle, 'No Job Title'))
		, 'No Job Title'
	) as Title
	, row_number() over(partition by y.[contact-companyId], trim(@chars4trim from isnull(x.JobTitle, '')) order by x.ClientContactId) rn
	from
	RF_Job_Complete x
	left join VC_Con y on cast(x.ClientContactId as varchar) = y.[contact-externalId]
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

--, DocTmpTab as (
--	select
--	x.JobId
--	, string_agg(y.Name, ',') as Docs
--	from TmpTab2 x
--	left join VC_DocsIdx y on x.JobId = y.JobID
--	where len(trim(isnull(y.Name, ''))) > 0
--	group by x.JobId
--)

--select * from DocTmpTab

select

x.ConId as [position-contactId]

, x.JobId as [position-externalId]

, x.Title as [position-title]

, trim(@chars4trim from isnull(cast(y.NoOfPlaces as varchar(10)), '1')) as [position-headcount]

, trim(@chars4trim from isnull(y.users_createdemailaddress, '')) as [position-owners]

, case(lower(trim(@chars4trim from isnull(y.EmploymentTypes_Description, 'Permanent'))))
	when lower('Permanent') then 'PERMANENT'
	when lower('Contract') then 'CONTRACT'
	when lower('Temp') then 'TEMPORARY'
end as [position-type]

, 'GBP' as [position-currency]

, cast(y.salary as numeric(19,2)) as [position-actualSalary]

--, trim(@chars4trim from isnull(y.rate, '')) as [position-payRate]

--, trim(@chars4trim from isnull(y.cont_id, '')) as [position-contractLength]

, convert(varchar(10), isnull(cast(y.startdate as datetime), dateadd(day, -7, getdate())), 120) as [position-startDate]
--, convert(varchar(10), dateadd(day, -7, getdate()), 120) as [position-startDate]

--, convert(varchar(10), isnull(cast(y.ENDING as datetime), dateadd(MONTH, 12, getdate())), 120) as [position-endDate]
, convert(varchar(10), dateadd(MONTH, 12, getdate()), 120) as [position-endDate]

--, isnull(z.Docs, '') as [position-document]

--, trim(@chars4trim from isnull(y.fee, '')) as [position-otherDocument]
, concat(
	concat('External ID: ', x.JobId)
	, nullif(concat(@NewLineChar, 'Job Ref. No: ', isnull(y.jobrefno, '')), concat(@NewLineChar, 'Job Ref. No: '))
	, nullif(concat(@NewLineChar, 'Archived: '
		, case(upper(trim(isnull(y.archived, ''))))
			when 'Y' then 'Yes'
			else null
		end)
		, concat(@NewLineChar, 'Archived: ')
	)
	, nullif(concat(@NewLineChar, 'Commision percentage: ', nullif(trim(isnull(FORMAT(y.commissionperc, 'N'), '0.00')), '0.00')), concat(@NewLineChar, 'Commision percentage: '))
	, nullif(concat(@NewLineChar, 'Hours per week: ', nullif(trim(isnull(FORMAT(y.hoursperweek, 'N'), '0.00')), '0.00')), concat(@NewLineChar, 'Hours per week: '))
	, nullif(concat(@NewLineChar, 'Max Basic Salary: ', nullif(trim(isnull(FORMAT(y.maxbasic, 'N'), '0.00')), '0.00')), concat(@NewLineChar, 'Max Basic Salary: '))
	, nullif(concat(@NewLineChar, 'Min Basic Salary: ', nullif(trim(isnull(FORMAT(y.minbasic, 'N'), '0.00')), '0.00')), concat(@NewLineChar, 'Min Basic Salary: '))
	, nullif(concat(@NewLineChar, 'Placement Fee: ', nullif(trim(isnull(FORMAT(y.placementfee, 'N'), '0.00')), '0.00')), concat(@NewLineChar, 'Placement Fee: '))
	, nullif(concat(@NewLineChar, 'Published: '
		, case(upper(trim(isnull(y.published, ''))))
			when 'Y' then 'Yes'
			when 'N' then 'No'
			else null
		end)
		, concat(@NewLineChar, 'Published: ')
	)
	, nullif(concat(@NewLineChar, 'Sector: ', trim(isnull(y.sectors_sectorname, ''))), concat(@NewLineChar, 'Sector: '))
	, nullif(concat(@NewLineChar, 'Status: ', isnull(y.statusid, '')), concat(@NewLineChar, 'Status: '))
	, nullif(concat(@NewLineChar, 'Interview Address: ', trim(isnull(y.interviewaddress, ''))), concat(@NewLineChar, 'Interview Address: '))
	, nullif(concat(@NewLineChar, 'Location: ', trim(isnull(y.locations_description, ''))), concat(@NewLineChar, 'Location: '))
	, nullif(concat(@NewLineChar, 'Position: ', trim(isnull(y.attributes_positiondescription, ''))), concat(@NewLineChar, 'Position: '))
	, nullif(concat(@NewLineChar, 'Work address:', @NewLineChar, trim(isnull(y.workaddress, ''))), concat(@NewLineChar, 'Work address:', @NewLineChar))
	, nullif(
		concat('·················································'
			, char(10)
			, 'Notes:'
			, char(10)
			, '·················································'
			, char(10)
			, y.notes
		)
		, concat('·················································'
			, char(10)
			, 'Notes:'
			, char(10)
			, '·················································'
			, char(10)
		)
	)
	--, nullif(
	--	concat(
	--		'Entered: '
	--		, FORMAT(dateadd(day, -2, cast(y.entered as datetime)), 'dd-MMM-yyyy H:mm:ss', 'en-gb')
	--		, char(10)
	--	)
	--	, concat('Entered: ', char(10))
	--)
	--, nullif(
	--	concat('Salary to: '
	--		, FORMAT(y.SALARYHIGH, '#,#', 'en-gb')
	--		, ' GBP'
	--		, char(10)
	--	)
	--	, concat('Salary to: ', ' GBP', char(10))
	--)
	--, nullif(
	--	concat('Fee: '
	--		, FORMAT(FEE, '#', 'en-gb')
	--		, '%'
	--		, char(10)
	--	)
	--	, concat('Fee: ', '%', char(10))
	--)
) as [position-note]

, trim(@chars4trim from isnull(y.PublishedJobDescription, '')) as [position-publicDescription]

--, trim(@chars4trim from isnull(y.PublishedJobDescription, '')) as [position-internalDescription]

into VC_Job

from
TmpTab2 x
left join RF_Job_Complete y on x.JobId = y.JobId
--left join DocTmpTab z on x.JobId = z.JobId

select * from VC_Job
--where [position-contactId] not in (select [contact-externalId] from VC_Con)
order by [position-externalId]

--select count(*) from JOBS_DATA_TABLE where ENTERED is null and FEE is null and SALARYHIGH is null