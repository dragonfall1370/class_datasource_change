with
--DUPLICATION REGCONITION --> no duplication name for Catalize
dup as (SELECT ContactServiceID, FullName, ROW_NUMBER() OVER(PARTITION BY FullName ORDER BY ContactServiceID ASC) AS rn 
FROM ContactMainTable where Type = 2 and IsDeletedLocally = 0)

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

--MAIN SCRIPT
select concat('CA',cmt.ContactServiceID) as 'company-externalId'
, cmt.FullName as 'company-name'
, case when right(cmt.CreatedBy,len(cmt.CreatedBy)-charindex('\',cmt.CreatedBy)) = 'Thomas' then 'thomasvandevyvere@catalize.be'
when right(cmt.CreatedBy,len(cmt.CreatedBy)-charindex('\',cmt.CreatedBy)) = 'katrien' then 'katriendebeil@catalize.be'
when right(cmt.CreatedBy,len(cmt.CreatedBy)-charindex('\',cmt.CreatedBy)) = 'helene' then 'helenevandeputte@catalize.be'
else 'thomasvandevyvere@catalize.be' end as 'company-owner'
, left(cmt.WebAddress,99) as 'company-website'
, cmt.WorkPhoneNum as 'company-phone'
, cmt.BusinessFaxNum as 'company-fax'
, cmt.BusinessAddress as 'company-locationName'
, cmt.BusinessAddress as 'company-locationAddress'
, cdt.WorkAddressCity as  'company-locationCity'
, cdt.WorkAddressState as 'company-locationState'
, cdt.WorkAddressZip as 'company-locationZipCode'
, case when cdt.WorkAddressCountry = 'België' then 'BE'
	when cdt.WorkAddressCountry = 'Nederland' then 'NL'
	when cdt.WorkAddressCountry = 'Verenigd Koningrijk' then 'GB'
	else '' end as 'company-locationCountry'
, cf.CompanyFiles as 'company-document'
, concat(concat('Catalize Company External ID: ',cmt.ContactServiceID),char(10)
	, concat('Created on: ',convert(varchar(10),cmt.CreatedOn,120),char(10))
	, iif(cmt.CreatedBy = '' or cmt.CreatedBy is NULL,'',concat('Initiated by: ',right(cmt.CreatedBy,len(cmt.CreatedBy)-charindex('\',cmt.CreatedBy)),char(10)))
	, iif(cdt.LeadSource = '' or cdt.LeadSource is NULL,'',concat('Lead source: ',cdt.LeadSource,char(10)))
	, iif(cmt.PostalAddress = '' or cmt.PostalAddress is NULL,'',concat('Postal Address: ',cmt.PostalAddress,char(10)))
	, iif(cast(cdt.ContactNotes as nvarchar(max)) = '' or cdt.ContactNotes is NULL,'',concat('Notes: ',cast(cdt.ContactNotes as nvarchar(max))))
	) as 'company-note'
from ContactMainTable cmt
left join ContactDetailsTable cdt on cmt.ContactServiceID = cdt.ContactServiceID
left join CompanyFiles cf on cf.ContactID = cmt.ContactServiceID
where cmt.Type = 2 and cmt.IsDeletedLocally = 0

UNION ALL

select 'CA9999999','Default company','','','','','','','','','','','','This is default company from data import'