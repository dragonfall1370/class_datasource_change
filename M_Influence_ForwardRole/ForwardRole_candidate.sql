--CANDIDATE MAIL DUPLICATION
with EmailDupRegconition as (select UniqueID, ltrim(replace(replace(replace(replace(replace(replace(replace(Email,': ',''),':',''),'   ',''),'/ ',''),'/',''),' ',''),'**invalidemail**','')) as Email
	, ROW_NUMBER() OVER(PARTITION BY Email ORDER BY UniqueID ASC) AS rn 
	from Candidates
	where Email <> ''
	and Email like '%_@_%.__%')

, CandidateEmail as (select UniqueID, rn
	, case when rn = 1 then Email else concat(rn,'_',Email) end as Email
	from EmailDupRegconition)

--CANDIDATE WORKMAIL DUPLICATION
, WorkEmailDupRegconition as (select UniqueID, ltrim(replace(replace(replace(replace(replace(replace(replace(EmailAddress2,': ',''),':',''),'   ',''),'/ ',''),'/',''),' ',''),'**invalidemail**','')) as EmailAddress2
	, ROW_NUMBER() OVER(PARTITION BY EmailAddress2 ORDER BY UniqueID ASC) AS rn 
	from Candidates
	where EmailAddress2 <> ''
	and EmailAddress2 like '%_@_%.__%')

, CandidateWorkEmail as (select UniqueID, rn
	, case when rn = 1 then EmailAddress2 else concat(rn,'_',EmailAddress2) end as EmailAddress2
	from WorkEmailDupRegconition)

--MAIN SCRIPT
select concat('FR',c.UniqueID) as 'candidate-externalId'
, coalesce(nullif(c.Forename,''),concat('Firstname - ', c.UniqueID)) as 'candidate-firstName'
, coalesce(nullif(c.Surname,''),concat('Lastname - ', c.UniqueID)) as 'candidate-lastName'
, concat_ws(', ',nullif(c.Telephone,''),nullif(c.Mobile,'')) as 'candidate-phone'
, case
	when c.Email is not NULL and ce.UniqueID is not null then ce.Email
	else concat('candidate_',c.UniqueID,'@noemail.com') end as 'candidate-email'
, nullif(cwe.EmailAddress2,'') as 'candidate-workEmail'
, nullif(c.Address,'') as 'candidate-address'
, nullif(c.AddressLine6,'') as 'candidate-city'
, nullif(c.Postcode,'') as 'candidate-zipCode'
, iif(c.PositionCode is NULL or c.PositionCode = '',NULL,iif(ct.Description = '<HIDE>',NULL,ct.Description)) as 'candidate-jobTitle1'
, nullif(c.CurrentEmployer,'') as 'candidate-Employer1'
, nullif(c.WorkTelephone,'') as 'candidate-workPhone'
, nullif(c.Mobile,'') as 'candidate-mobile'
, case c.MainConsultant
	when 'AH' then 'alison@forwardrolerecruitment.com'
	when 'AM' then 'adam@forwardrolerecruitment.com'
	when 'ARAL' then 'arif@forwardrole.com'
	when 'BJ' then 'brian@forwardrole.com'
	when 'BRTO' then 'brad@forwardrole.com'
	when 'BS' then 'becky@forwardrole.com'
	when 'CP' then 'camilla@forwardrole.com'
	when 'CT' then 'chris@forwardrolerecruitment.com'
	when 'DAHA' then 'danielle@forwardrolerecruitment.com'
	when 'DAHY' then 'danh@forwardrole.com'
	when 'DANO' then 'david@forwardrolerecruitment.com'
	when 'DETO' then 'desi@forwardrole.com'
	when 'DM' then 'dan@forwardrole.com'
	when 'DS' then 'dominic@forwardrolerecruitment.com'
	when 'EMME' then 'emma@forwardrole.com'
	when 'GRBO' then 'grant@forwardrole.com'
	when 'GW' then 'guy@forwardrole.com'
	when 'HB' then 'henna@forwardrolerecruitment.com'
	when 'HC' then 'helen@forwardrole.com'
	when 'IL' then 'ian@forwardrolerecruitment.com'
	when 'IZKH' then 'izzy@forwardrole.com'
	when 'JH' then 'jack@forwardrolerecruitment.com'
	when 'JOPE' then 'josh@forwardrole.com'
	when 'JS' then 'jon@forwardrolerecruitment.com'
	when 'KM' then 'katrina@forwardrolerecruitment.com'
	when 'LK' then 'lucy@forwardrolerecruitment.com'
	when 'MABO' then 'mattb@forwardrole.com'
	when 'MD' then 'matt@forwardrole.com'
	when 'MIRH' then 'mike@forwardrole.com'
	when 'NAYO' then 'nathan@forwardrole.com'
	when 'PAMC' then 'patrick@forwardrole.com'
	when 'PAWE' then 'paddy@forwardrole.com'
	when 'PHST' then 'phill@forwardrole.com'
	when 'RADA' then 'rachel@forwardrole.com'
	when 'RAWH' then 'rachelw@forwardrole.com'
	when 'RYDO' then 'ryan@forwardrole.com'
	when 'SASH' then 'sam@forwardrolere.com'
	when 'SOPA' then 'sophie@forwardrole.com'
	when 'ST' then 'steve@forwardrole.com'
	when 'TOBY' then 'tom@forwardrole.com'
	when 'TP' then 'thea@forwardrolerecruitment.com'
	when 'WIVE' then 'will@forwardrolerecruitment.com'
	end as 'candidate-owners'
, case c.Title
		when 'Sir' then 'MR' 
		when 'Mrs' then 'MRS'
		when 'Miss' then 'MISS'
		when 'Ms' then 'MS'
		when 'Mr' then 'MR'
		else NULL end as 'candidate-title'
, convert(date,c.DateOfBirth,103) as 'candidate-dob'
, nullif(c.CurrentBasic,0) as 'candidate-currentSalary'
, case when c.Currency = '£' then 'GBP'
	when c.Currency = '€' then 'EUR'
	else 'GBP' end as 'candidate-currency'
, left(nullif(c.SocialNetSite1,''),255) as 'candidate-linkedIn'
, case when isnumeric(PayRate1) = 1 then PayRate1
	else NULL end as 'candidate-contractRate'
, stuff(coalesce(' '+ nullif(c.DocumentsNames001,''),'') + coalesce(',' + nullif(c.DocumentsNames002,''),'') 
	+ coalesce(',' + nullif(c.DocumentsNames005,''),'') + coalesce(',' + nullif(c.DocumentsNames006,''),''), 1, 1, '') as 'candidate-resume'
, concat_ws(char(10), concat('Candidate External ID: ', c.UniqueID)
	, iif(c.CreationDate is NULL,NULL,concat('Influence Creation Date: ', convert(varchar(10),c.CreationDate,120)))
	, iif(c.BusinessType = '' or c.BusinessType is NULL,NULL,concat('Business Type: ', c.BusinessType, ' - ', ct2.Description))
	, iif(c.Status = '' or c.Status is NULL,NULL,concat('Status: ', c.Status, ' - ', ct3.Description))
	, iif(c.SocialNetSite2 = '' or c.SocialNetSite2 is NULL,NULL,concat('Social Net Site 2: ', c.SocialNetSite2))
	) as 'candidate-note'
from Candidates c	left join CodeTables ct on ct.Code = c.PositionCode and ct.TabName = 'Role Codes'
					left join CandidateEmail ce on ce.UniqueID = c.UniqueID
					left join CodeTables ct2 on ct2.Code = c.BusinessType and ct2.TabName = 'Bus Type'
					left join CodeTables ct3 on ct3.Code = c.Status and ct3.TabName = 'Candidate Status'
					left join CandidateWorkEmail cwe on cwe.UniqueID = c.UniqueID
order by c.UniqueID

---SafeCVGenDate001 as insert timestamp for CV updates