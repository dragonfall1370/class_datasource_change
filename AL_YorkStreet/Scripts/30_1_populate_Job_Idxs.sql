drop table if exists #VCJobIdxsTemp1

select VacancyID as JobId
, trim(isnull(Title, '')) as JobTitle
, row_number() over(partition by trim(isnull(Title, '')) order by CreationDate desc) as RowNum

into #VCJobIdxsTemp1

from Vacancies

drop table if exists #VCJobIdxsTemp2

select JobId, iif(RowNum = 1, JobTitle, concat(JobTitle, '(', RowNum, ')')) as JobTitle

into #VCJobIdxsTemp2

from #VCJobIdxsTemp1 cit1

drop table if exists #VCJobIdxsTemp1

drop table if exists #VCJobIdxsTemp3

select
cit2.JobId
, string_agg(u.Email, ',') as OwnerEmails

into #VCJobIdxsTemp3

from #VCJobIdxsTemp2 cit2
left join [Ownership] o on cit2.JobId = o.RecordID
left join RecordTypes rt on o.RecordTypeID = rt.RecordTypeID
left join Users u on o.OwnerID = u.UserID
where lower(trim(isnull(rt.Description, ''))) = lower('Vacancies')
group by cit2.JobId

drop table if exists [dbo].[VCJobIdxs]

select 

cit2.*
, cit3.OwnerEmails

into [dbo].[VCJobIdxs]

from #VCJobIdxsTemp2 cit2
left join #VCJobIdxsTemp3 cit3 on cit2.JobId = cit3.JobId

-- delete temp table
drop table if exists #VCJobIdxsTemp2
drop table if exists #VCJobIdxsTemp3

select * from VCJobIdxs

