with 
--CONTACT EMAIL ESCAPE WRONG FORMAT AND DUPLICATION
ContactEmails as (select p.cid, concat_ws(',',nullif(e.Contactemail,''), nullif(e.ContactEmail2,'')) as ContactEmails
	from People p
	left join Employment e on e.cid = p.cid
	where (e.Contactemail like '%_@_%.__%' or ContactEmail2 like '%_@_%.__%')
	and p.DeleteFlag = 0 and p.RoleType = 1 and e.DeleteFlag = 0)

, ContactEmailSplit as (select distinct cid, ContactEmails, value as FinalContactEmail
	from ContactEmails
	cross apply string_split(ContactEmails,',')
	where ContactEmails <> '')

, dup as (select cid, FinalContactEmail, row_number() over (partition by FinalContactEmail order by cid asc) as rn 
	from ContactEmailSplit)

, FinalContactEmail as (select distinct cid, STRING_AGG(case when rn > 1 then concat(rn,'_',FinalContactEmail)
	else FinalContactEmail end,',') as FinalContactEmail
	from dup
	group by cid) --4560 contact_id for test purposes

/* select * from ContactEmails
where cid = 13794 James@wavehealthcare.co.uk */

/* select * from ContactEmailSplit
where FinalContactEmail = 'diane.sutherland@fishawack.com' */

--COMPANY ID LINKS INVALID ID FROM EMPLOYMENT
, dupComp as (SELECT id, compname, ROW_NUMBER() OVER(PARTITION BY lower(compname) ORDER BY id ASC) AS rn
		FROM company
		where DeleteFlag = 0)

, EmploymentUniqueComp as (select distinct CompanyName
	from Employment
	where id is NULL or id = 0)

, CompInvalidID as (select dc.id, euc.CompanyName
	from EmploymentUniqueComp euc
	inner join dupComp dc on dc.compname = euc.CompanyName
	where dc.rn = 1)

--COMPANY ID LINKS FROM EMPLOYMENT
, EmploymentComp as (select distinct CompanyName
	from Employment
	where (id is NULL or id = 0)
	and CompanyName <> ''
	and Employment_id in (select Employment_id from People where RoleType = 1 and DeleteFlag = 0)
	and CompanyName not in (select compname from company where DeleteFlag = 0))

	, ContactComp as (select 10000 + row_number() over (order by CompanyName asc) as rn, CompanyName
	from EmploymentComp
	where CompanyName <> '')

--CONTACT DOCUMENTS
, Documents as (select cid
	, string_agg(concat(docs_id,'.',FileExt),',') as Documents 
	from docs
	where DeleteFlag = 0
	and docs_id not in (select Resume_ID from people where RoleType = 1 and DeleteFlag = 0)
	group by cid)

, OriginalResume as (select p.cid, 
	case when Resume_ID is not NULL or Resume_ID <> '' then concat(docs.docs_id,'.',docs.FileExt) 
		else NULL end as CandResume
	from people p
	left join docs on docs.docs_id = p.Resume_ID
	and p.Resume_ID > 0)

--MAIN SCRIPT	
select concat('IDSS',p.cid) as 'contact-externalId'
	, case when first is NULL or first = '' then 'Firstname'
		else first end as 'contact-firstName'
	, case when last is NULL or last = '' then 'Lastname'
		else last end as 'contact-lastName'
	, case when (e.id = 0 or e.id is NULL) and e.CompanyName in (select CompanyName from ContactComp) then concat('IDSS',cc.rn)
		when (e.id = 0 or e.id is NULL) and e.CompanyName in (select CompanyName from CompInvalidID) then concat('IDSS',cii.id)
		when e.id <> 0 and e.id not in (select id from Company where DeleteFlag =0) then 'IDSS9999999'
		when e.id <> 0 and e.id in (select id from Company where DeleteFlag =0) then concat('IDSS',e.id)
		else 'IDSS9999999' end as 'contact-companyId'
	, stuff(coalesce(',' + nullif(e.cphone,''),'') + coalesce(',' + nullif(e.cphone2,''),''),1,1,'') as 'contact-phone'
	, concat('IDSS',e.id) as company
	, e.CompanyName
	, fcm.FinalContactEmail as 'contact-email'
	, e.title as 'contact-jobTitle'
	, concat('Contact External ID: ',p.cid,char(10)
		, coalesce('Nickname / Preferred name: ' + nullif(p.NickName,'') + char(10),'')
		, coalesce('Original Company Name : ' + nullif(e.Companyname,'') + char(10),'')
		, coalesce('Contact notes: ' + nullif(convert(nvarchar(max),p.notes),''),'')
		) as 'contact-note'
	, stuff(coalesce(',' + nullif(case when ore.CandResume = '.' then NULL else ore.CandResume end,''),'') + coalesce(',' + nullif(dc.Documents,''),''),1,1,'') as 'contact-document'
from People p
left join Employment e on p.Employment_Id = e.Employment_id
left join CompInvalidID cii on cii.CompanyName = e.CompanyName --some companies exist but having invalid ID in Employment
left join ContactComp cc on cc.CompanyName = e.CompanyName --companies do not exist in Company
left join FinalContactEmail fcm on fcm.cid = p.cid
left join Documents dc on dc.cid = p.cid
left join OriginalResume ore on ore.cid = p.cid --original resume
where p.RoleType = 1
and p.DeleteFlag = 0