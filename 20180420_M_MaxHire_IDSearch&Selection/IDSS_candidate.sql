with 
--CANDIDATE EMAIL ESCAPE WRONG FORMAT AND DUPLICATION
ContactEmails as (select cid, concat_ws(',',nullif(email,''), nullif(email2,'')) as CandidateEmails
	from People
	where (email like '%_@_%.__%' or email2 like '%_@_%.__%')
	and DeleteFlag = 0 and RoleType = 0)

, ContactEmailSplit as (select distinct cid, CandidateEmails, value as FinalCandEmail
	from ContactEmails
	cross apply string_split(CandidateEmails,',')
	where CandidateEmails <> '')

, dup as (select cid, FinalCandEmail, row_number() over (partition by FinalCandEmail order by cid asc) as rn 
	from ContactEmailSplit)

, CandEmail as (select cid
	, case when rn > 1 then concat(rn,'_',FinalCandEmail)
	else FinalCandEmail end as FinalCandEmail
	, row_number() over (partition by cid order by cid asc) as rn_mail
	from dup)

--CANDIDATE DOCUMENTS
, Documents as (select cid
	, string_agg(concat(docs_id,'.',FileExt),',') as Documents 
	from docs
	where DeleteFlag = 0
	and docs_id not in (select Resume_ID from people where RoleType = 0 and DeleteFlag = 0)
	group by cid)

, OriginalResume as (select p.cid, 
	concat(docs.docs_id,'.',docs.FileExt) as CandResume
	from people p
	left join docs on docs.docs_id = p.Resume_ID
	where docs.doctypes_id = 1
	and p.Resume_ID > 0 and p.Resume_ID is not NULL)

--MAIN SCRIPT	
select concat('IDSS',p.cid) as 'candidate-externalId'
	, case when p.first is NULL or p.first = '' then 'Firstname'
		else first end as 'candidate-firstName'
	, case when last is NULL or last = '' then 'Lastname'
		else last end as 'candidate-lastName'
	, case when MiddleName is NULL or MiddleName = '' then NULL
		else MiddleName end as 'candidate-middleName'
	, case when CandEmail.rn_mail = 1 then CandEmail.FinalCandEmail 
		else concat(p.cid,'_candidate@noemail.com') end as 'candidate-email'
	, nullif(p.CellPhone,'') as 'candidate-phone'
	, nullif(p.CellPhone,'') as 'candidate-mobile'
	, nullif(p.homephone,'') as 'candidate-homePhone'
	, 'GBP' as 'candidate-currency' --update: 04042018

--Candidate Employer
	, nullif(e.title,'') as 'candidate-jobTitle1'
	, case when e.id > 0 then c.compname
		else nullif(e.CompanyName,'') end as 'candidate-employer1'
	, concat(coalesce('Company: ' + case when e.id > 0 then c.compname else nullif(e.CompanyName,'') end + char(10),'')
		, coalesce('Job title: ' + nullif(e.title,''),'')
		) as 'candidate-workHistory'
	, nullif(left(p.HomePage,255),'') as 'candidate-linkedln'
	, concat(--'Candidate External ID: ',p.cid,char(10)
		--, coalesce('Nickname / Preferred name: ' + nullif(p.NickName,'') + char(10),'')
		--, coalesce('Available To Start: ' + nullif(convert(varchar(10),p.AvailableToStart,103),'') + char(10),'') --Update to remove some fields on 04042018
		coalesce('Home Email: ' + ce2.FinalCandEmail + char(10),'')
		, coalesce('* Contact notes: ' + char(10) + nullif(convert(nvarchar(max),p.notes),''),'')
		) as 'candidate-note'
	, nullif(case when ore.CandResume = '.' then NULL else ore.CandResume end,'') as 'candidate-resume'
	--, stuff(coalesce(',' + nullif(case when ore.CandResume = '.' then NULL else ore.CandResume end,''),'') + coalesce(',' + nullif(dc.Documents,''),''),1,1,'') as 'candidate-document' | Full documents
from People p
left join Employment e on p.Employment_Id = e.Employment_id
left join Company c on e.id = c.id --join to get correct company name
left join CandEmail on CandEmail.cid = p.cid and CandEmail.rn_mail = 1
left join CandEmail ce2 on ce2.cid = p.cid and ce2.rn_mail = 2
left join OriginalResume ore on ore.cid = p.cid --original resume
left join Documents dc on dc.cid = p.cid
where p.RoleType = 0
and p.DeleteFlag = 0
--and p.cid = 3064 mr_kitson@yahoo.co.uk
order by p.cid --IDSS2820

--select email, email2 from People where cid = 3374

--TO REMOVE JOB TYPE
update candidate set desired_job_type_json = replace(desired_job_type_json,'{"desiredJobTypeId":1}','') where id = 76093