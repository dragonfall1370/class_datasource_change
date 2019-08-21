--DUPLICATION REGCONITION
with dup as (select ActivityID, Title, row_number() over(partition by lower(title) order by ActivityID desc) as rn
	from JobMaster)

--JOB DOCUMENTS
, DocumentsRow as (select distinct ActivityID
	, right(FileServerLocation,CHARINDEX('\',reverse(FileServerLocation))-1) as NewFile
	, row_number() over(partition by right(FileServerLocation,CHARINDEX('\',reverse(FileServerLocation))-1) order by ActivityID desc) as  rn
	from ActivityDocs
	where lower(right(FileServerLocation,CHARINDEX('.',reverse(FileServerLocation)))) in ('.pdf','.doc','.docx','.xls','.xlsx','.rtf','.msg','.txt','.htm','.html'))

--RENAME DOCUMENTS WITH ROW NUMBERS
, Documents as (select ActivityID
	, case when rn > 1 then concat(left(NewFile,CHARINDEX('.',NewFile)-1),'_',rn-1,right(NewFile,CHARINDEX('.',reverse(NewFile))))
		else NewFile end as NewFile
	from DocumentsRow)

, JobDocuments as (select ActivityID, string_agg(NewFile,',') as JobDocuments
	from Documents
	group by ActivityID)

--MAIN SCRIPT
select concat('TF',j.ActivityID) as 'position-externalId'
, concat('TF',j.ContactID) as 'position-contactId'
, case when rn > 1 then concat(dup.Title,' - ',dup.rn)
	else dup.Title end as 'position-title'
, u.Email as 'position-owners'
, convert(date,convert(varchar(10),j.DateEntered),112) as 'position-startDate'
, case when isnumeric(j.SalaryFrom) = 1 then j.SalaryFrom
	else NULL end as 'position-actualSalary'
, case when j.PermanentRqd = 'X' then 'PERMANENT'
	when j.ContractRqd = 'X' then 'CONTRACT'
	when j.TemporaryRqd = 'X' then 'TEMPORARY'
	else 'PERMANENT' end as 'position-type'
, j.PosAvailable as 'position-headcount'
, d.JobDocuments as 'position-document'
, concat_ws(char(10),concat('Job External ID: ',j.ActivityID)
	, coalesce('Reason: ' + nullif(j.Reason,''),NULL)
	, coalesce('Location: ' + nullif(j.Location,''),NULL)
	, coalesce('Start Date: ' + nullif(convert(varchar(20),convert(datetime,j.StartDate,112)),''),NULL)
	, coalesce('PostCode: ' + nullif(j.PostCode,''),NULL)
	, coalesce('Current State: ' + nullif(j.CurrentState,''),NULL)
	, coalesce('Inactive Date: ' + nullif(convert(varchar(10),j.InactiveDate,112),''),NULL)
	, coalesce('Salary To: ' + nullif(convert(varchar(max),j.SalaryTo),''),NULL)
	, coalesce('Interviewer: ' + nullif(j.Interviewer,''),'')
	, coalesce('Interviewer Position: ' + nullif(j.InterviewerPosition,''),NULL)
	, coalesce('Interview Location: ' + nullif(j.InterviewLocation,''),NULL)
	, coalesce('User Combo 1: ' + nullif(j.UserCombo1,''),NULL)
	, coalesce('Transport: ' + nullif(j.Transport,''),NULL)
	, coalesce('Comments: ' + nullif(j.Comments,''),NULL)
	, coalesce('Duties: ' + nullif(j.Duties,''),NULL)
	) as 'position-note'
from JobMaster j
left join Users u on u.ConsultantID = j.ConsultantID
left join dup on dup.ActivityID = j.ActivityID
left join JobDocuments d on d.ActivityID = j.ActivityID
where j.ContactID in (select ContactID from ClientContacts where Status = 'A') --1558 rows
order by j.ActivityID