--OFFICE AND BUSINESS ADDRESS
with CompLocation as (select cmt.ContactServiceID, 
case when cdt.OfficeLocation is NULL and cmt.BusinessAddress is NULL then ''
when cdt.OfficeLocation is NULL and cmt.BusinessAddress is not NULL then cmt.BusinessAddress
when cmt.BusinessAddress is NULL and cdt.OfficeLocation is not NULL then cdt.OfficeLocation
else stuff((coalesce(',' + cdt.OfficeLocation, '') + coalesce(', ' + cmt.BusinessAddress, '') + coalesce(', ' + cdt.WorkAddressCountry,'')), 1, 1, '') end as LocationAddress
from ContactMainTable cmt
left join ContactDetailsTable cdt on cmt.ContactServiceID = cdt.ContactServiceID)

--DOCUMENT / ACTIVITY ATTACHMENTS
, CompanyAttachment as (select distinct ac.ContactID, replace(replace(act.Subject,'.txt','.doc'),',','') as FileName
	, max(act.ModifiedOn) as ModifiedOn --> same contact may have same Subject file, so to remove duplicated files by max ModifiedOn
	from ActivityContacts ac --> Remove comma in the file name
	left join ActivitiesTable act on act.ActivityID = ac.ActivityID
	where ac.ContactType = 2 and act.ActivityType = 2 --> ContactType 2 for Company, 1 for Contact, 3 for Job | ActivityType 2 is for File
	and (act.Subject like '%.pdf' or act.Subject like '%.doc%' or act.Subject like '%.xls%' or act.Subject like '%.rtf' or act.Subject like '%.html' or act.Subject like '%.txt')
	group by ac.ContactID, act.Subject)


, CompanyFiles as (SELECT
     ContactID,
     STUFF(
         (SELECT ',' + FileName
          from  CompanyAttachment
          WHERE ContactID = a.ContactID
		  order by ModifiedOn desc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS CompanyFiles
FROM CompanyAttachment as a
GROUP BY a.ContactID)

--DUPLICATION REGCONITION
, dup as (SELECT ContactServiceID, FullName, ROW_NUMBER() OVER(PARTITION BY FullName ORDER BY ContactServiceID ASC) AS rn 
FROM ContactMainTable where Type = 2 and IsDeletedLocally = 0)

--MAIN SCRIPT
select concat('RH',cmt.ContactServiceID) as 'company-externalId'
, left(iif(cmt.ContactServiceID in (select ContactServiceID from dup where dup.rn > 1)
	, iif(dup.FullName = '' or dup.FullName is NULL,concat('Default Company-',dup.ContactServiceID),concat(dup.FullName,'-DUPLICATE-',dup.ContactServiceID))
	, iif(cmt.FullName = '' or cmt.FullName is null,concat('Default Company-',cmt.ContactServiceID),cmt.FullName)),100) as 'company-name'
, cmt.FullName as '(OrginalCompanyName)'
, concat(right(cmt.CreatedBy,len(cmt.CreatedBy)-charindex('\',cmt.CreatedBy)),'@redheads.co.za') as 'company-owner'
, left(cmt.WebAddress,99) as 'company-website'
, cmt.WorkPhoneNum as 'company-phone'
, cmt.BusinessFaxNum as 'company-fax'
, cl.LocationAddress as 'company-locationName'
, cl.LocationAddress as 'company-locationAddress'
, cf.CompanyFiles as 'company-document'
, concat(concat('BCM Company ID: ',cmt.ContactServiceID),char(10)
	, concat('Created on: ',convert(varchar(10),cmt.CreatedOn,120),char(10))
	, iif(cmt.CreatedBy = '' or cmt.CreatedBy is NULL,'',concat('Initiated by: ',right(cmt.CreatedBy,len(cmt.CreatedBy)-charindex('\',cmt.CreatedBy)),char(10)))
	, iif(cdt.LeadSource = '' or cdt.LeadSource is NULL,'',concat('Lead source: ',cdt.LeadSource,char(10)))
	, iif(uf.UserField16 = '' or uf.UserField16 is NULL,'',concat('Client Priority: ',cast(UserField16 as nvarchar(max)),char(10)))
	, iif(uf.UserField17 = '' or uf.UserField17 is NULL,'',concat('Priority Comment: ',cast(UserField17 as nvarchar(max)),char(10)))
	, iif(uf.UserField3 = '' or uf.UserField3 is NULL,'',concat('Company Reg No.: ',cast(UserField3 as nvarchar(max)),char(10)))
	, iif(uf.UserField4 = '' or uf.UserField4 is NULL,'',concat('VAT Number: ',cast(UserField4 as nvarchar(max)),char(10)))
	, iif(uf.UserField7 = '' or uf.UserField7 is NULL,'',concat('Vendor Number of RedHeads: ',cast(UserField7 as nvarchar(max)),char(10)))
	, iif(uf.UserField13 = '' or uf.UserField13 is NULL,'',concat('Creditor Number used by Supplier: ',cast(UserField13 as nvarchar(max)),char(10)))
	, iif(uf.UserField14 = '' or uf.UserField14 is NULL,'',concat('Invoice Sending: ',cast(UserField14 as nvarchar(max)),char(10)))
	, iif(uf.UserField1 = '' or uf.UserField1 is NULL,'',concat('CAD / CAE system used: ',cast(UserField1 as nvarchar(max)),char(10)))
	, iif(uf.UserField2 = '' or uf.UserField2 is NULL,'',concat('CAD Notes: ',cast(UserField2 as nvarchar(max)),char(10)))
	, iif(uf.UserField11 = '' or uf.UserField11 is NULL,'',concat('Technologies Applied: ',cast(UserField11 as nvarchar(max)),char(10)))
	, iif(uf.UserField5 = '' or uf.UserField5 is NULL,'',concat('Mother Company: ',cast(UserField5 as nvarchar(max)),char(10)))
	, iif(uf.UserField6 = '' or uf.UserField6 is NULL,'',concat('Subsidiaries: ',cast(UserField6 as nvarchar(max)),char(10)))
	, iif(uf.UserField15 = '' or uf.UserField15 is NULL,'',concat('Former Names of Company: ',cast(UserField15 as nvarchar(max)),char(10)))
	, iif(uf.UserField12 = '' or uf.UserField12 is NULL,'',concat('Anti - Poaching Agreement: ',cast(UserField12 as nvarchar(max)),char(10)))
	, iif(uf.UserField10 = '' or uf.UserField10 is NULL,'',concat('Vacancy URL: ',cast(UserField10 as nvarchar(max)),char(10)))
	, iif(uf.UserField8 = '' or uf.UserField8 is NULL,'',concat('Competitors: ',cast(UserField8 as nvarchar(max)),char(10)))
	, iif(uf.UserField9 = '' or uf.UserField9 is NULL,'',concat('Cannot Poach From: ',cast(UserField9 as nvarchar(max))))
	) as 'company-note'
from ContactMainTable cmt
left join ContactDetailsTable cdt on cmt.ContactServiceID = cdt.ContactServiceID
left join dup on dup.ContactServiceID = cmt.ContactServiceID
left join CompLocation cl on cl.ContactServiceID = cmt.ContactServiceID
left join UserFields uf on uf.ContactServiceID = cmt.ContactServiceID
left join CompanyFiles cf on cf.ContactID = cmt.ContactServiceID
where cmt.Type = 2 and cmt.IsDeletedLocally = 0

UNION ALL

select 'RH9999999','Default company','','','','','','','','','This is default company from data import'