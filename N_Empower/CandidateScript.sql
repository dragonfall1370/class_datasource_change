with

--CANDIDATE DUPLICATE MAIL REGCONITION
 TempEmail as (select Contact_ContactId, iif(replace(Contact_Email,char(10),',') like '%,%',left(replace(Contact_Email,char(10),','),CHARINDEX(',',replace(Contact_Email,char(10),','))-1),Contact_Email) as Contact_Email
from v_Contact_AllFields)

, EmailDupRegconition as (SELECT Contact_ContactId ID,Contact_Email as Email,
 ROW_NUMBER() OVER(PARTITION BY Contact_Email ORDER BY Contact_ContactId ASC) AS rn 
from TempEmail
where Contact_Email like '%_@_%.__%')

--Remove ','
--, TempEmail as (select *, iif(Email1 like '%,%',left(Email1,CHARINDEX(',',Email1)-1),Email1) as Email
--from EmailDupRegconition)

--edit duplicating emails
, ContactEmail as (select ID, 
case 
when rn=1 then Email
else concat(rn,'_',(Email))
end as Email
from EmailDupRegconition)

---Document
, tempdocs as (
select EntityId, FileName 
from v_DocumentLibrary_AllFields
where Entity = 'Contact'
 union all 
select EntityId, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(right(FormattedCVFileRef1,(CHARINDEX('\',Reverse(FormattedCVFileRef1))-1)), ''), '')
			+ Coalesce(',' + NULLIF(right(CVFileRef,(CHARINDEX('\',Reverse(CVFileRef))-1)), ''), '')
			, 1, 1, '')) as FileName
from ContactConfigFields 
where FormattedCVFileRef1 is not null or CVFileRef is not null)

, docs as (SELECT EntityId ContactId,
     STUFF(
         (SELECT ',' + FileName
          from  tempdocs
          WHERE EntityId = da.EntityId
    order by EntityId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS documentName
FROM tempdocs as da
GROUP BY da.EntityId)

, loc as (select Contact_ContactId ContactId,Contact_HomeAddressStreet Street, Contact_HomeAddressCity City, Contact_HomeAddressCounty County
	, Contact_HomeAddressPostCode PostCode, Contact_HomeAddressCountry Country, Contact_HomeAddressTelNo TelNo
	,replace(replace(replace(replace(ltrim(Stuff(
			  Coalesce(' ' + NULLIF(ltrim(rtrim(Contact_HomeAddressStreet)), ''), '')
			+ Coalesce(', ' + NULLIF(ltrim(rtrim(Contact_HomeAddressCity)), ''), '')
			+ Coalesce(', ' + NULLIF(ltrim(rtrim(Contact_HomeAddressCounty)), ''), '')
			+ Coalesce(', ' + NULLIF(ltrim(rtrim(Contact_HomeAddressPostCode)), ''), '')
			+ Coalesce(', ' + NULLIF(ltrim(rtrim(Contact_HomeAddressCountry)), ''), '')
			+ Coalesce(' (TelNo. ' + NULLIF(ltrim(rtrim(Contact_HomeAddressTelNo)), '') + ')', '')
			, 1, 1, '')),char(10),', '),char(13),''),'  ',''),' ,',',') as 'homeAddress'
from v_Contact_AllFields)

-------------------------------------------------------------MAIN SCRIPT
select concat('EPW', a.Contact_ContactId) as 'candidate-externalId'
, iif(a.Contact_Christian = '' or a.Contact_Christian = '.',concat('NoFirstname-', a.Contact_ContactId),a.Contact_Christian) as 'candidate-firstName'
, iif(a.Contact_Surname = '' or a.Contact_Surname = '.',concat('NoLastName-', a.Contact_ContactId),a.Contact_Surname) as 'candidate-Lastname'
, iif(a.Contact_JobTitle = '-' or a.Contact_JobTitle is null,'',a.Contact_JobTitle) as 'candidate-jobTitle1'
, iif(a.Contact_Company = '' or a.Contact_Company is null,'',a.Contact_Company) as 'candidate-Employer1'
, Coalesce(Contact_MobileTelNo,Contact_WorkTelNo,Contact_HomeAddressTelNo,replace(Contact_WorkMobile,',','|'),'') as 'candidate-phone'
, iif(a.Contact_HomeAddressTelNo = '' or a.Contact_HomeAddressTelNo is null,'',a.Contact_HomeAddressTelNo) as 'candidate-homePhone'
, iif(a.Contact_WorkTelNo = '' or a.Contact_WorkTelNo is null,'',a.Contact_WorkTelNo) as 'candidate-workPhone'
, iif(a.Contact_MobileTelNo = '' or a.Contact_MobileTelNo is null,'',a.Contact_MobileTelNo) as 'candidate-mobile'
, convert(varchar(10),Contact_DateOfBirth ,120) as 'candidate-dob'
, upper(Contact_Title) as 'candidate-title'
, iif(a.Contact_OwnerUser = 'Ryan Oxlade','ryan.oxlade@empowermanagement.com.au','') as 'candidate-owners'
, ccf.SkypeNumber as 'candidate-skype'
, loc.homeAddress as 'candidate-address'
, replace(replace(ltrim(rtrim(Contact_HomeAddressCity)),'  ',''),' ,',',') as 'candidate-City'
, replace(replace(ltrim(rtrim(Contact_HomeAddressCounty)),'  ',''),' ,',',') as 'candidate-state'
, ltrim(rtrim(Contact_HomeAddressPostCode)) as 'candidate-zipCode'
, case 
	when homeAddress like '%Australia%' then 'AU'
	when homeAddress like '%United Kingdom%' then 'GB'
	when homeAddress like '%Bahrain%' then 'BH'
	when homeAddress like '%Sydney%' then 'AU'
	when homeAddress like '%Brisbane%' then 'AU'
	when homeAddress like '%NSW%' then 'AU'
	when homeAddress like '%QLD%' then 'AU'
	when homeAddress like '%New Zealand%' then 'NZ'
  else '' end as 'candidate-country'
, case 
	when a.Contact_EMail is not NULL and ce.ID is not null then ce.Email
	else concat('CandidateID-',a.Contact_ContactId,'@noemail.com') end as 'candidate-email'
, d.documentName as 'candidate-resume'
, right(convert(nvarchar(1000),ccf.ContactPhoto),(CHARINDEX('\',Reverse(convert(nvarchar(1000),ccf.ContactPhoto)))-1)) as 'candidate-photo'
, left(concat('Candidate External ID: EPW',a.Contact_ContactId, char(10)
	, iif(a.Contact_DateFirstRegistered is null,'',concat(char(10),'Date First Registered: ',a.Contact_DateFirstRegistered,char(10)))
	, iif(a.Contact_Email = '' or a.Contact_Email is null,'',concat(char(10),'Email(s): ',replace(Contact_Email,char(10),', '),char(10)))
	, iif(a.Contact_AlternativeEmailAddress1 = '' or a.Contact_AlternativeEmailAddress1 is null,'',concat(char(10),'Alternative Email: ',a.Contact_AlternativeEmailAddress1,char(10)))
	, iif(a.Contact_WorkMobile = '' or a.Contact_WorkMobile is null,'',concat(char(10),'Work Mobile: ',a.Contact_WorkMobile,char(10)))
	, iif(ccf.CurrentEmployer = '' or ccf.CurrentEmployer is null,'',concat(char(10),'Current Employer: ',ccf.CurrentEmployer,char(10)))
	, iif(con.Comments = '' or con.Comments is NULL,'',concat(char(10),'Comments: ',char(10),con.Comments))
	),32000) as 'candidate-note'
from v_Contact_AllFields a
	--left join Company comp on c.CompanyId = comp.CompanyId
	left join ContactEmail ce on a.Contact_ContactId = ce.ID
	--left join EmailDupRegconition edr on a.Contact_ContactId = edr.ID
	left join Contact con on a.Contact_ContactId = con.ContactId
	left join ContactConfigFields ccf on a.Contact_ContactId = ccf.EntityId
	left join docs d on a.Contact_ContactId = d.ContactId
	left join loc on a.Contact_ContactId = loc.ContactId
where Contact_ContactType in ('ClientCandidate','Candidate') --and a.Contact_EMail like '%info@amprojectpartners.com%'
--and a.Contact_ContactId = 8717
--select * from v_Contact_AllFields
