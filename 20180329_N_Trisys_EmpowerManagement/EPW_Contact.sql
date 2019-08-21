--Contact location: will be added to note
with
 --loc as (
	--select ID, ltrim(Stuff(
	--		  Coalesce(' ' + NULLIF(Adresse, ''), '')
	--		+ Coalesce(', ' + NULLIF(Ville, ''), '')
	--		+ Coalesce(', ' + NULLIF(ZipCodeDistance, ''), '')
	--		+ Coalesce(', ' + NULLIF(Etat, ''), '')
	--		, 1, 1, '')) as 'locationName'
	--from contacts)

 tempCompContact as (select distinct CompanyId, ContactId
from CompanyContact)

----------Contact Email
--check email format
, EmailDupRegconition as (SELECT Contact_ContactId ID,replace(Contact_Email,char(10),',') as Email,
 ROW_NUMBER() OVER(PARTITION BY Contact_Email ORDER BY Contact_ContactId ASC) AS rn 
from v_Contact_AllFields-- where Courriel <> ''
where Contact_Email like '%_@_%.__%')

--edit duplicating emails
, ContactEmail as (select ID, 
case 
when rn=1 then Email
else concat(rn,'_',(Email))
end as Email
from EmailDupRegconition)
--select * from ContactEmail where email like '%,%'

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

---------MAIN SCRIPT
--, main as (
select 
iif(c.CompanyId = '' or c.CompanyId is NULL,'EPW9999999',concat('EPW',c.CompanyId)) as 'contact-companyId'
, c.CompanyId as '(OriginalCompanyID)'
, comp.Name as '(OriginalCompanyName)'
, concat('EPW',cc.Contact_ContactId) as 'contact-externalId'
, iif(cc.Contact_Christian = '' or cc.Contact_Christian = '.',concat('NoFirstname-', cc.Contact_ContactId),cc.Contact_Christian) as 'contact-firstName'
, iif(cc.Contact_Surname = '' or cc.Contact_Surname = '.',concat('NoLastName-', cc.Contact_ContactId),cc.Contact_Surname) as 'contact-lastName'
, ce.Email as 'contact-email'
, right(convert(nvarchar(1000),ccf.ContactPhoto),(CHARINDEX('\',Reverse(convert(nvarchar(1000),ccf.ContactPhoto)))-1)) as 'contact-photo'
, d.documentName as 'contact-document'
, ltrim(coalesce(
			Stuff(
			  Coalesce(' ' + NULLIF(ltrim(rtrim(Contact_WorkTelNo)) , ''), '')
			+ Coalesce(',' + NULLIF(ltrim(rtrim(Contact_MobileTelNo)), ''), '')
			+ Coalesce(',' + NULLIF(Contact_HomeAddressTelNo, ''), '')
			, 1, 1, ''),Contact_WorkMobile)) as 'contact-phone'
--, Contact_MobileTelNo as mobile
--, Contact_WorkTelNo as work
--, Contact_HomeAddressTelNo as home
--, Contact_WorkMobile wm
--, cs.Num as 'contact-skype'
, iif(cc.Contact_JobTitle = '-' or cc.Contact_JobTitle is null,'',cc.Contact_JobTitle) as 'contact-jobTitle'
, left(
	concat('Contact External ID: EPW',cc.Contact_ContactId,char(10)
	, iif(cc.Contact_DateFirstRegistered is null,'',concat(char(10),'Date First Registered: ',cc.Contact_DateFirstRegistered,char(10)))
	, iif(comp.Name = '' or comp.Name is null,'',concat(char(10),'Company: ',comp.Name,char(10)))
	, iif(c.CompanyId = '' or c.CompanyId is null,'',concat(char(10),'Company ID: ',c.CompanyId,char(10)))
	, iif(cc.Contact_WorkMobile = '' or cc.Contact_WorkMobile is null,'',concat(char(10),'Work Mobile: ',cc.Contact_WorkMobile,char(10)))
	--, coalesce(char(10) + 'Contact Other Notes: ' + ps.Notes, '')),32000) as 'contact-note'
	, iif(cc.Contact_ContactType = '' or cc.Contact_ContactType is null,'',concat(char(10),'Contact Type: ',cc.Contact_ContactType,char(10)))
	, iif(con.Comments = '' or con.Comments is NULL,'',concat(char(10),'Comments: ',char(10),con.Comments))),32000) 
	as 'contact-note'
from v_Contact_AllFields cc left join tempCompContact c on cc.Contact_ContactId = c.ContactId
	left join Company comp on c.CompanyId = comp.CompanyId
	left join ContactEmail ce on cc.Contact_ContactId = ce.ID
	left join Contact con on cc.Contact_ContactId = con.ContactId
	left join ContactConfigFields ccf on cc.Contact_ContactId = ccf.EntityId
	left join docs d on cc.Contact_ContactId = d.ContactId
	--left join loc on cc.ID = loc.ID
where Contact_ContactType in ('ClientCandidate','Client')-- and cc.Contact_ContactId = 8666
--where CompanyOwner = ''
UNION ALL
select 'EPW9999999','','','EPW9999999','Default','Contact','','','','','','This is default contact from Data Import'
--)
--select * from main where [contact-phone] is not null