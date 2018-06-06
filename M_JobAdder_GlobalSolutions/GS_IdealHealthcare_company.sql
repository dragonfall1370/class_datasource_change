---Combine all notes for Company
with AllNote as (select N.NoteID, N.Type, N.Source, N.Text, N.CreatedByUserID, N.DateCreated, U.Email, U.DisplayName
from Note N
left join [User] U on U.UserID = N.CreatedByUserID)

, CompNote as (select CN.CompanyID, CN.NoteID, N.Type, N.Source, N.Text
	, convert(varchar,N.DateCreated,120) as DateCreated, N.Email, N.DisplayName
	from CompanyNote CN
	left join AllNote N on CN.NoteID = N.NoteID)

, CompNoteFinal as (SELECT
     CompanyID,
     STUFF(
         (SELECT char(10) + 'Created Date: ' + convert(varchar,DateCreated,120) + ' || ' 
		 + 'Created by: ' + Email + ' - ' + DisplayName + ' || ' + Text
          from  CompNote
          WHERE CompanyID = a.CompanyID
		  order by DateCreated desc
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM CompNote as a
GROUP BY a.CompanyID)

----Combine note attachment for each category
, NoteAttach as (select NoteID, AttachmentID, concat(AttachmentID,'.original',
case when FileType = '' then ''
when FileType = 'application/msword' then '.doc'
when FileType = 'application/pdf' then '.pdf'
when FileType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' then '.xlsx'
when FileType = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' then '.docx'
when FileType = 'image/jpeg' then '.jpg'
when FileType = 'image/png' then '.png'
else '' end) as FileName
from NoteAttachment
where FileName like '%pdf' or FileName like '%doc%' or FileName like '%xls%' or FileName like '%rtf' or FileName like '%jpg' or FileName like '%png')

, CompNoteAttach as (select CN.CompanyID, CN.NoteID, NA.AttachmentID, NA.FileName
	from CompanyNote CN
	left join NoteAttach NA on CN.NoteID = NA.NoteID
	where FileName like '%pdf' or FileName like '%doc%' or FileName like '%xls%' or FileName like '%rtf')

, CompNoteAttaches as (SELECT
     CompanyID,
     STUFF(
         (SELECT ',' + FileName
          from  CompNoteAttach
          WHERE CompanyID = a.CompanyID
		  order by AttachmentID desc
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM CompNoteAttach as a
GROUP BY a.CompanyID)

---Combine all documents for each category
, CompDoc as (select CompanyID, replace(DocName,',','') as DocName from CompanyDocument)

, CompCombinedDocument as (SELECT
     CompanyID,
     STUFF(
         (SELECT ',' + DocName
          from  CompDoc
          WHERE CompanyID = a.CompanyID
		  and DocName like '%pdf' or DocName like '%doc%' or DocName like '%xls%' or DocName like '%rtf'
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM CompDoc as a
GROUP BY a.CompanyID)

, CompNoteAttach_Doc (CompanyID, Attach_Doc) as (select * from CompNoteAttaches
UNION select * from CompCombinedDocument)

, CompCombinedAttach_Doc as (SELECT
     CompanyID,
     STUFF(
         (SELECT ',' + Attach_Doc
          from  CompNoteAttach_Doc
          WHERE CompanyID = a.CompanyID
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM CompNoteAttach_Doc as a
GROUP BY a.CompanyID)

---Combined Company Address and Head Office address
, CombinedAddress as (SELECT
     CompanyID,
     STUFF(
         (SELECT char(10) + 'Name: ' + Name + ', ' + Line1 + ', ' + Line2 + ', ' + Suburb + ', ' 
		 + State + ', ' + Postcode + ', ' +  Country
          from  CompanyAddress
          WHERE CompanyID = a.CompanyID
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM CompanyAddress as a
GROUP BY a.CompanyID)

/* Check the Head Office of any company

, CompMainInfo as (select CompanyID, Name, Line1, Line2, Suburb, State, Postcode, Country
, Phone, Fax, Url 
from CompanyAddress
where Name = 'Head Office') 

*/

---Combined phone from Company Address
, CombinedPhone as (SELECT
     CompanyID,
     STUFF(
         (SELECT ',' + Phone
          from  CompanyAddress
          WHERE CompanyID = a.CompanyID
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM CompanyAddress as a
GROUP BY a.CompanyID)

---Company skill to be combined from Category and Subcategory
, CompCg_SCg as (select CS.CompanyID, CS.CategoryID, Cg.Name as CategoryName, CS.SubCategoryID, SCg.Name as SubCategoryName
from CompanySkill CS 
left join Category Cg on CS.CategoryID = Cg.CategoryID
left join SubCategory SCg on CS.SubCategoryID = SCg.SubCategoryID)

, CompCombinedSubCat as (SELECT
     CompanyID, CategoryID,
     STUFF(
         (SELECT ', ' + SubCategoryName
          from  CompCg_SCg
          WHERE CompanyID = a.CompanyID
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM CompCg_SCg as a
GROUP BY a.CompanyID, a.CategoryID)

, CompCategory as (select CCg.CompanyID, CCg.CategoryID, Cg.Name as CategoryName, CCg.URLList as SubCategories
from CompCombinedSubCat CCg
left join Category Cg on CCg.CategoryID = Cg.CategoryID)

, CompCombinedCategory as (SELECT
     CompanyID, CategoryID,
     STUFF(
         (SELECT char(10) + CategoryName + ': ' + SubCategories
          from  CompCategory
          WHERE CompanyID = a.CompanyID
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM CompCategory as a
GROUP BY a.CompanyID, CategoryID)

---Company custom field (Global Solutions)
, CustFieldValue as (select CompanyID, FieldID,
     STUFF(
         (SELECT ', ' + ValueText
          from  CompanyCustomField
          WHERE CompanyID = a.CompanyID
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM CompanyCustomField as a
GROUP BY a.CompanyID, a.FieldID)

--select * from CustFieldValue
--order by CompanyID

, CusField as (select CFV.CompanyID, CFV.FieldID, CF.Name as FieldName, CFV.URLList as CustomFieldValue 
from CustFieldValue CFV
left join CustomField CF on CFV.FieldID = CF.FieldID
where CF.EntityType = 'Company')

--select * from CusField
--order by CompanyID -> Different count due to the custom field was deleted, ID = 1

, CombinedCustFieldValue as (select CompanyID,
     STUFF(
         (SELECT char(10) + FieldName + ' - ' + CustomFieldValue
          from  CusField
          WHERE CompanyID = a.CompanyID
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS URLList
FROM CusField as a
GROUP BY a.CompanyID)

--select CompanyID, count(CompanyID) from CombinedCustFieldValue group by CompanyID having count(CompanyID) > 1

---Main script
select top 100 concat('IH',C.CompanyID) as 'company-externalId'
, C.Name as 'company-name'
, concat(iif(CA.Name = '' or CA.Name is NULL,'',concat(CA.Name,': ')),iif(CA.Line1 = '' or CA.Line1 is NULL,'',concat(CA.Line1,', ')),iif(CA.Suburb = '' or CA.Suburb is NULL,'',concat(CA.Suburb,', '))
	,iif(CA.State = '' or CA.State is NULL,'',concat(CA.State,', ')),iif(CA.Postcode = '' or CA.Postcode is NULL,'',CA.Postcode),iif(CA.Country = '' or CA.Country is NULL,'',concat(', ',CA.Country))) as 'company-locationName'
, concat(coalesce(CA.Name + ': ',''),coalesce(CA.Line1 + ', ',''),coalesce(CA.Line2 + ', ',''),coalesce(CA.Suburb + ', ','')
	,coalesce(CA.State + ', ',''),coalesce(CA.Postcode + ', ',''),coalesce(CA.Country,'')) as 'company-locationAddress'
, CA.Suburb as 'company-locationSuburb'
, CA.State as 'company-locationState'
, CA.Country
, case 
when CA.Country = 'Australia' then 'AU'
when CA.Country = 'Canada' then 'CA'
when CA.Country = 'China' then 'CN'
when CA.Country = 'Hong Kong' then 'HK'
when CA.Country = 'Indonesia' then 'ID'
when CA.Country = 'Malaysia' then 'MY'
when CA.Country = 'Philippines' then 'PH'
when CA.Country = 'Singapore' then 'SG'
when CA.Country = 'South Korea' then 'KR'
when CA.Country = 'Thailand' then 'TH'
when CA.Country = 'United Kingdom' then 'GB'
when CA.Country = 'United States' then 'US'
else '' end as 'company-locationCountry'
, CA.Postcode as 'company-locationZipCode'
, CP.URLList as 'company-phone'
, CA.Fax as 'company-fax'
, left(CA.Url,99) as 'company-website'
, C.OwnerUserID
, iif(U.Email like '%deleted%','',U.Email) as 'company-owners'
, left(concat('Company External ID: ',C.CompanyID,char(10)
	, iif(C.StatusID = '' or C.StatusID is NULL,'',concat('Status: ',CS.Name,char(10)))
	, iif(C.Summary = '' or C.Summary is NULL,'',concat('Summary: ',C.Summary,char(10)))
	, iif(C.PrimaryContactID = '' or C.PrimaryContactID is NULL,'',concat('Primary Contact ID-Name: ',C.PrimaryContactID,'-',CON.FullName,char(10)))
	, iif(C.LinkedInUrl = '' or C.LinkedInUrl is NULL,'',concat('LinkedIn Url: ',C.LinkedInUrl,char(10)))
	, iif(C.TwitterUrl = '' or C.TwitterUrl is NULL,'',concat('TwitterUrl: ',C.TwitterUrl,char(10)))
	, iif(C.GooglePlusUrl = '' or C.GooglePlusUrl is NULL,'',concat('GooglePlus Url: ',C.GooglePlusUrl,char(10)))
	, iif(C.FacebookUrl = '' or C.FacebookUrl is NULL,'',concat('FacebookUrl: ',C.FacebookUrl,char(10)))
	, iif(C.OwnerUserID = '' or C.OwnerUserID is NULL,'',concat('Company Owner: ',U.DisplayName,char(10)))
	, iif(CCCg.CategoryID = '' or CCCg.CategoryID is NULL,'',concat('Company skills: ',CCCg.URLList,char(10)))
	, iif(C.DateUpdated = '' or C.DateUpdated is NULL,'',concat('Last Updated: ',convert(varchar(10),C.DateUpdated,120),char(10)))
	, iif(CNF.URLList = '' or CNF.URLList is NULL,'',concat('Company Notes: ',CNF.URLList,char(10)))
	, iif(CNA.URLList = '' or CNA.URLList is NULL,'',concat('Company Note Attachments: ',CNA.URLList,char(10)))
	, iif(CCFV.URLList = '' or CCFV.URLList is NULL,'',concat('Company Custom Fields: ',replace(replace(replace(replace(CCFV.URLList,'&lt;','<'),'&gt;','>'),'&amp;','&'),'#x0D;',''))))
	,32000) as 'company-note'
, REPLACE(CCAD.URLList,'&amp;','&') as 'company-document'
from Company C
left join CombinedPhone CP on CP.CompanyID = C.CompanyID --company combined phones from companyAddress
left join CompanyAddress CA on CA.AddressID = C.PrimaryAddressID
left join Contact CON on CON.ContactID = C.PrimaryContactID --company primary contact info
left join CompanyStatus CS on CS.StatusID = C.StatusID --company status
left join CompCombinedAttach_Doc CCAD on CCAD.CompanyID = C.CompanyID --combined all note attachments & documents
left join [User] U on U.UserID = C.OwnerUserID --company ownerUserID
left join CompNoteAttaches CNA on CNA.CompanyID = C.CompanyID --company note attachment
--left join CompanySkill CSk on CSk.CompanyID = C.CompanyID --> remove this because it creates duplication
left join CompCombinedCategory CCCg on CCCg.CompanyID = C.CompanyID --company combined categories and subcategories
left join CompNoteFinal CNF on CNF.CompanyID = C.CompanyID
left join CombinedCustFieldValue CCFV on CCFV.CompanyID = C.CompanyID
order by C.CompanyID