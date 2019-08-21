with 
temp as (select *,replace(replace(contactexternalId,'NJFS','NJF1S'),'NJFGTP','NJF2GTP') as externalId
from importContact)

--RECOGNITE CONTACTS HAVE THE SAME EMAIL ADDRESSES
, tempContactEmail as (
select contactexternalId, externalId
	, case 
		when contactemail like '(%' then replace(contactemail,'(','')
		when contactemail like '%?%' then replace(contactemail,'?','')
		when contactemail like '%,-,%' then replace(contactemail,',-,',',')
		when contactemail like '%,or,%' then replace(contactemail,',or,',',')
		when contactemail like '/%' then replace(contactemail,'/','')
		when contactemail like ',%' then replace(contactemail,',','')
		when contactemail like '%,' then replace(contactemail,',','')
		when contactemail like '%,.%' then replace(contactemail,',.','.')
		when contactemail like '%,com%' then replace(contactemail,',com','.com')
		when contactemail like ': %' then replace(contactemail,': ','')
		when contactemail like '%:%' then replace(contactemail,':','')
		when contactemail like '%,,,%' then replace(contactemail,',,,',',')
		when contactemail like '%,,%' then replace(contactemail,',,',',')
		when contactemail like '%,,' then replace(contactemail,',,','')
		when contactemail like '%co,uk%' then replace(contactemail,'co,uk','co.uk')
		when contactemail like '%,private,e-mail%' then replace(contactemail,',private,e-mail','')
		when charindex(',',contactemail) < charindex('@',contactemail) then replace(contactemail,',','')
		when charindex(',',reverse(contactemail)) < charindex('@',reverse(contactemail)) then replace(contactemail,',','')
	else contactemail end as email
, ROW_NUMBER() OVER(PARTITION BY contactemail ORDER BY externalId ASC) AS rn
from temp
where contactemail <> '' or contactemail is not null)

, tempContactEmail1 as (
select contactexternalId
	, replace(replace(replace(replace(replace(email,'private,e-mail',''),',,',''),':',''),';',''),',net','.net') as email
	, ROW_NUMBER() OVER(PARTITION BY email ORDER BY externalId ASC) AS rn
from tempContactEmail
)

, ContactEmail as (select contactexternalId, 
iif(rn=1,email
 , case 
		when left(contactexternalId,4) like 'NJFS' then concat('NJFS_',email)
		when left(contactexternalId,4) like 'NJFC' then concat('NJFC_',email)
		else concat('NJFGTP_',email)
		end) as Email
from tempContactEmail1)
--select *--, replace(email,',,',',') as email1
--from ContactEmail --where Email like '%,%'-- and contactexternalId like 'NJFC%'

---------MAIN SCRIPT
select 
tc.contactcompanyId as 'contact-companyId'
, OriginalCompanyID as '(OriginalCompanyID)'
, OriginalCompanyName as '(OriginalCompanyName)'
, tc.contactexternalId as 'contact-externalId'
, contactfirstName as 'contact-firstName'
, contactlastName as 'contact-lastName'
, contactmiddleName as 'contact-middleName'
, contactlinkedin as 'contact-linkedin'
, contactjobTitle as 'contact-jobTitle'
, ce.Email as 'contact-email'
, contactskype as 'contact-skype'
, contactphone as 'contact-phone'--prior to mobile, then ddi, then workphone for contact's primary phone
, contactowners as 'contact-owners'
, contactdocument as 'contact-document'
, contactnote as 'contact-note'
from importContact tc left join ContactEmail ce on tc.contactexternalId = ce.contactexternalId