with cte_merge_jobs as (
select *, 
'PERMANENT' type
-- ROW_NUMBER() OVER(PARTITION BY field18, CONVERT(nvarchar(max), field12) ORDER BY field37) rn
from [Permanent Placement Table]
UNION ALL
select *, 
'CONTRACT' type
-- ROW_NUMBER() OVER(PARTITION BY field18, CONVERT(nvarchar(max), field12) ORDER BY field37) rn
from [Temporary Booking Table]
),
cte_permanent_extra as (
	select 
	ppt.field2 job_id,
	pte.field16 created_on,
	pte.field15 job_owner,
	tbl_con.con_email job_owner_email
	from [Permanent Placement Table] ppt
	left join [Permanent Table Extra] pte on ppt.field2 = pte.field1
	left join tbl_con on pte.field15 = tbl_con.con_username
-- 	group by ppt.field2, pte.field16
),
cte_temporary_extra as (
	select 
	tbt.field2 job_id,
	bte.field16 created_on,
	bte.field15 job_owner,
	tbl_con.con_email job_owner_email
	from [Temporary Booking Table] tbt
	left join [Booking Table Extra] bte on tbt.field2 = bte.field1
	left join tbl_con on bte.field15 = tbl_con.con_username
-- 	group by ppt.field2, pte.field16
),
-- Generate default contacts for jobs
cte_contact_company as (
select
case
	when c.field2 is null and cd.field2 is null then 1
	else c.field2
end contact_id,
case
	when cd.field2 is null then 1
	else cd.field2
end company_id
from cte_merge_jobs cm
left join [Company Contact Database] c on cm.field18 = c.field2
left join [Company Database] cd on cm.field2A = cd.field2
where c.field2 is null or cd.field2 = 1
),
cte_distinct_company as (
select
distinct company_id, 
contact_id
from cte_contact_company
),
cte_default_company as (
select 
company_id,
contact_id,
case
	when contact_id is null then ROW_NUMBER() OVER(PARTITION BY contact_id ORDER BY company_id) + 1
	else 1
end rn
from cte_distinct_company
),
-- Jobs
cte_jobs as (
	select
	type,
	cm.field2 job_id,
	COALESCE(c.field2, dc.rn) contact_id,
	dc.company_id,
	cm.field1 company_name,
	case
		when type = 'permanent' then format(cte_permanent_extra.created_on, 'yyyy-MM-dd')
		else format(cte_temporary_extra.created_on, 'yyyy-MM-dd')
	end job_open_date,
	case 
		when type = 'permanent' then cte_permanent_extra.job_owner
		else cte_temporary_extra.job_owner
-- 		need mapping for job owners in temporary table
	end job_owner,
	case 
		when type = 'permanent' then cte_permanent_extra.job_owner_email
		else cte_temporary_extra.job_owner_email
-- 		need mapping for job owners in temporary table
	end job_owner_email,
	case
		when cm.field12 is null then 'No job title'
-- 		when cm.rn > 1 then concat(cm.field12, ' - ', cm.rn)
		when len(cast(cm.field12 as nvarchar(max))) > 400 then 
																												case
																													when CHARINDEX(char(10), cast(cm.field12 as nvarchar(max))) > 0 then concat(replace(LEFT(cast(cm.field12 as nvarchar(max)), CHARINDEX(char(10), cast(cm.field12 as nvarchar(max)))), char(10), ''), '(For more information go to internal job description)')
																													else concat(left(cast(cm.field12 as nvarchar(max)), CHARINDEX(',', cast(cm.field12 as nvarchar(max))) - 1), ' (For more information go to internal job description)')
																												end
		else cm.field12
	end job_title,
	case
		when len(cast(cm.field12 as nvarchar(max))) > 400 then cm.field12
		else cm.field14 
	end internal_job_description,
	cm.field15 skills,
	concat(
		case
			when cd.field2 is null then concat('Company ID: 1', char(10))
			else concat('Company ID: ', cd.field2, char(10))
		end,
		CASE 
			WHEN cm.field6 is not null THEN
				CASE 
					WHEN RIGHT(RTRIM(cm.field6), 1) IN (',', '.') THEN concat('Street1: ', SUBSTRING(RTRIM(cm.field6),1,LEN(RTRIM(cm.field6)) - 1), char(10)) ELSE concat('Street1: ', cm.field6, char(10)) 
				END
		END,
		CASE 
			WHEN cm.field7 is not null THEN 
				CASE 
					WHEN RIGHT(RTRIM(cm.field7), 1) IN (',', '.') THEN concat('Street2: ', SUBSTRING(RTRIM(cm.field7),1,LEN(RTRIM(cm.field7)) - 1), char(10)) ELSE concat('Street2: ', cm.field7, char(10)) 
				END
		END,
		CASE WHEN cm.field30 is not null THEN concat('Town: ',cm.field30, char(10)) END,
		CASE WHEN cm.field8 is not null THEN concat('County: ', cm.field8, char(10)) END,
		CASE WHEN cm.field31 is not null THEN concat('Country: ', replace(cm.field31, 'United Kingdon', 'United Kingdom'), char(10)) END,
		concat(
			'Website: ',
			CASE
				WHEN len(CONVERT(NVARCHAR(MAX), cm.field32)) < 10 THEN ''
				WHEN CHARINDEX('#/www', cm.field32) <> 0 THEN replace(replace(LEFT(CONVERT(NVARCHAR(MAX), cm.field32), CHARINDEX('/',cm.field32,9)-1), '///', '//'), '#', '')
				WHEN CHARINDEX('#http://www.http', cm.field32) <> 0 THEN LEFT(CONVERT(NVARCHAR(MAX), cm.field32), CHARINDEX('#',cm.field32,9)-1)
				WHEN len(CONVERT(NVARCHAR(MAX), cm.field32)) > 100 THEN CASE
																																WHEN charindex('http://#https', cm.field32) <> 0 THEN replace(LEFT(CONVERT(NVARCHAR(MAX), cm.field32), CHARINDEX('/',cm.field32,18)-1), 'http://#', '')
																																WHEN charindex('#http', cm.field32) <> 0 THEN replace(LEFT(CONVERT(NVARCHAR(MAX), cm.field32), CHARINDEX('/',cm.field32,10)-1), '#', '')
																																ELSE LEFT(CONVERT(NVARCHAR(MAX), cm.field32), CHARINDEX('/',cm.field32,9)-1)
																														 END
				WHEN charindex('https', cm.field32) = 0 THEN substring(RTRIM(CONVERT(NVARCHAR(MAX), cm.field32)), charindex('http', CONVERT(NVARCHAR(MAX), cm.field32)), LEN(RTRIM(CONVERT(NVARCHAR(MAX), cm.field32))) - charindex('http', CONVERT(NVARCHAR(MAX), cm.field32)))
				ELSE substring(RTRIM(CONVERT(NVARCHAR(MAX), cm.field32)), charindex('https', CONVERT(NVARCHAR(MAX), cm.field32)), LEN(RTRIM(CONVERT(NVARCHAR(MAX), cm.field32))) - charindex('https', CONVERT(NVARCHAR(MAX), cm.field32)))
			END
		)
	) note
	
	from cte_merge_jobs cm
	left join cte_permanent_extra on cm.field2 = cte_permanent_extra.job_id
	left join cte_temporary_extra on cm.field2 = cte_temporary_extra.job_id
	left join [Company Contact Database] c ON cm.field18 = c.field2
	left join [Company Database] cd on cm.field2A = cd.field2
	left join cte_default_company dc ON COALESCE(cd.field2, '1') = dc.company_id
),
cte_jobs_distinct as (
	select *,
	ROW_NUMBER() OVER(PARTITION BY company_id, contact_id, CONVERT(nvarchar(max), job_title) ORDER BY job_id) rn
	from cte_jobs
)

select
job_id "position-externalId",
contact_id "position-contactId",
-- company_id,
-- rn,
job_open_date "position-startDate",
job_owner_email "position-owners",
case
	when rn <> 1 then concat(job_title, ' - ', rn) 
	else job_title
end "position-title",
type "position-type",
internal_job_description "position-internalDescription",
note "position-note"
-- skills

from cte_jobs_distinct