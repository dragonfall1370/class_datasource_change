---CANDIDATE PRIMARY EMAIL
with dup as (select Person_Number, replace(replace(replace(replace(E_Contact_Address,'!',''),'''',''),':',''),'"','') as E_Contact_Address
	, ROW_NUMBER() OVER(PARTITION BY replace(replace(replace(replace(E_Contact_Address,'!',''),'''',''),':',''),'"','') ORDER BY Person_Number ASC) AS rn
	from Person_E_Contact
	where Primary_Ind = 1
	and E_Contact_Address <> ''
	and E_Contact_Address like '%_@_%.__%')

-->> NO candidate having > 1 email
--select Person_Number, count(*) from dup
--group by Person_Number
--having count(*) > 1

---CANDIDATE OTHER EMAIL
, dupOther as (select Person_Number, replace(replace(replace(replace(E_Contact_Address,'!',''),'''',''),':',''),'"','') as E_Contact_Address
	, ROW_NUMBER() OVER(PARTITION BY replace(replace(replace(replace(E_Contact_Address,'!',''),'''',''),':',''),'"','') ORDER BY Person_Number ASC) AS rn
	from Person_E_Contact
	where Primary_Ind = 0
	and E_Contact_Address <> ''
	and E_Contact_Address like '%_@_%.__%')

---CANDIDATE OTHER EMAIL JOIN
, OtherEmails as (select Person_Number, STRING_AGG(case when rn > 1 then concat(rn,'_',E_Contact_Address) else E_Contact_Address end,', ') as OtherEmails
	from dupOther
	group by Person_Number)

---CANDIDATE PRIMARY PHONE
, PrimaryPhone as (select Person_Number, Telephone_Number, row_number() over(partition by Person_Number order by Telephone_Number desc) as rn
	from Person_Telephone
	where Primary_Ind = 1)
	
---CANDIDATE PHONES
, AllPhones as (select Person_Number, STRING_AGG(Telephone_Number,',') as Phones
	from Person_Telephone
	where Primary_Ind = 0
	group by Person_Number)

---CANDIDATE LOCATION
, CandidateLocation as (select Person_Number
	, replace(replace(replace(Address_Block,char(10),''),char(13),''),'  ',' ') as AddressBlock
	, Municipality
	from Person_Location
	where Primary_Ind = 1)

---CANDIDATE DOCUMENTS
, CandDocument as (select CandidateNumber, Foldername, Filename, PrimaryCV, row_number() over(partition by CandidateNumber order by PrimaryCV desc) as rn
	from AFR_CandidateCSV2)

, Documents as (select CandidateNumber, STRING_AGG(concat(Foldername,'_',replace(replace(Filename,'?',''),',','')),',') within group (order by rn asc) as Documents
	from CandDocument
	where CandidateNumber > 0
	group by CandidateNumber)

---MAIN SCRIPT
select concat('AFR',p.Person_Number) as 'candidate-externalId'
, coalesce(nullif(p.Name_First,''),concat('Firstname - ', p.Person_Number)) as 'candidate-firstName'
, coalesce(nullif(p.Name_Last,''),'Lastname') as 'candidate-lastName'
, case when dup.rn = 1 then dup.E_Contact_Address
	when dup.rn > 1 then concat(dup.rn,'_',dup.E_Contact_Address)
	else concat('candidate_',p.Person_Number,'@noemail.com') end as 'candidate-email'
, pr.Telephone_Number as 'candidate-phone'
--, cl.AddressBlock as 'candidate-address' | updated req on 28082018
, a6.Location as 'candidate-address' --CUSTOM SCRIPT: update locationName = locationAddress (common location)
, u.Email as 'candidate-owners'
, nullif(a1.PrimaryEmployer,'') as 'candidate-Employer1'
, nullif(a1.PositionTitle,'') as 'candidate-jobTitle1'
, left(nullif(a1.Linkedin_URL,''),255) as 'candidate-linkedln'
, cast (p.Pay_Amt as decimal(10, 0)) as 'candidate-desiredSalary'
--, left(p.Pay_Amt,charindex('.',p.Pay_Amt)-1) as 'candidate-desiredSalary' | old script
, 'EUR' as 'candidate-currency'
, d.Documents as 'candidate-resume'
, concat_ws(char(10),concat('Candidate External ID - CN#: ', p.Person_Number)
	, iif(p.Comment = '' or p.Comment is NULL,NULL,concat('Comment: ', p.Comment))
	, coalesce('All email addresses: ' + dup.E_Contact_Address + coalesce(', ' + nullif(o.OtherEmails,''),''),'')
	, concat('All phone numbers addresses: ', pr.Telephone_Number, ', ', ph.Phones)
	, iif(a1.InternalInterviewer = '' or a1.InternalInterviewer is NULL,NULL,concat('Internal Interviewer: ', a1.InternalInterviewer))
	, iif(p.Recruiter_Interview_DT is NULL,NULL,concat('Internal Interview Date: ', convert(varchar(20),p.Recruiter_Interview_DT,120))) 
	--CUSTOM SCRIPT: Append "INTERNAL INTERVIEW" in Activities
	, iif(p.Owned_By_Person_Number = '' or p.Owned_By_Person_Number is NULL,NULL,concat('Entered by: ', u.Firstname, ' ', u.Lastname, ' - ', u.Email)) --CUSTOM SCRIPT: created by
	, iif(p.Created_DTTM is NULL,NULL,concat('Entered date: ', convert(varchar(20),p.Created_DTTM,120))) --CUSTOM SCRIPT: Append "Entered date" in Activities
	) as 'candidate-note'
from Person p
left join dup on dup.Person_Number = p.Person_Number
left join OtherEmails o on o.Person_Number = p.Person_Number
left join Documents d on d.CandidateNumber = p.Person_Number
left join PrimaryPhone pr on pr.Person_Number = p.Person_Number
left join AllPhones ph on ph.Person_Number = p.Person_Number
--left join CandidateLocation cl on cl.Person_Number = p.Person_Number | already removed from new requirement
left join AFR_User u on u.UserID = p.Owned_By_Person_Number
left join AFR_CandidateCSV1 a1 on a1.CandidateNumber = p.Person_Number
left join AFR_CandidateCSV6 a6 on a6.CandidateNumber = p.Person_Number
order by p.Person_Number