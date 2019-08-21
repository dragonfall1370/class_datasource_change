--DUPLICATION RECOGNITION
with dup as (SELECT ClientID, ClientName, ROW_NUMBER() OVER(PARTITION BY ClientName ORDER BY ClientID ASC) AS rn 
FROM Client where Deleted = 0)

--COMPANY PHONE > Some companies have more than 1 phones - Default phone can be found from Client table
, Phones as (select CP.ClientID, Phone.PhoneID, Phone.PhoneTypeID, PhoneType.Description, Phone.PhoneNumber
	from Client_Phone CP
	left join Phone on Phone.PhoneID = CP.PhoneID
	left join PhoneType on PhoneType.PhoneTypeID = Phone.PhoneTypeID
	where Phone.PhoneTypeID <> 4)

, ClientPhone as (SELECT
     ClientID, 
     STUFF(
         (SELECT ', ' + Description + ': ' + PhoneNumber
          from  Phones
          WHERE ClientID = a.ClientID
		  order by PhoneTypeID asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS ClientPhone
FROM Phones as a
GROUP BY a.ClientID)

--COMPANY FAX > Phone Type = 4 will be Fax
, Fax as (select CP.ClientID, Phone.PhoneID, Phone.PhoneTypeID, PhoneType.Description, Phone.PhoneNumber
	from Client_Phone CP
	left join Phone on Phone.PhoneID = CP.PhoneID
	left join PhoneType on PhoneType.PhoneTypeID = Phone.PhoneTypeID
	where Phone.PhoneTypeID = 4) --> 4 is Fax

, ClientFax as (SELECT
     ClientID, 
     STUFF(
         (SELECT ', ' + Description + ': ' + PhoneNumber
          from Fax
          WHERE ClientID = a.ClientID
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS ClientFax
FROM Fax as a
GROUP BY a.ClientID)

--COMPANY ADDRESSES
, Addresses as (select CA.ClientID, CA.AddressID, A.AddressTypeID, AT.Description as AddressType, A.Street, A.Street2, A.CityId, City.Description as City
	, A.ProvinceStateId, PS.Description as Province, A.PostalZip, A.CountryId, CT.Description as Country
	from Client_Address CA
	left join Address A on A.AddressID = CA.AddressID
	left join AddressType AT on AT.AddressTypeID = A.AddressTypeID
	left join City on City.CityID = A.CityId
	left join ProvinceState PS on PS.ProvinceStateID = A.ProvinceStateId
	left join Country CT on CT.CountryId = A.CountryId
	where CA.Deleted = 0) --> 1 Client Address was deleted

, ClientAddresses as (SELECT
     ClientID,
     STUFF(
         (SELECT char(10) + coalesce(AddressType + ': ','') + 
		 stuff((coalesce(', ' + Street,'') + coalesce(', ' + Street2,'') + coalesce(', ' + City,'') + coalesce(', ' + Province,'') 
		 + coalesce(', ' + PostalZip,'') + coalesce(', ' + Country,'')),1,2,'')
          from  Addresses
          WHERE ClientID = a.ClientID
		  order by AddressTypeID asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS ClientAddresses
FROM Addresses as a
GROUP BY a.ClientID)

--CLIENT DOCUMENTS
, Documents as (select CD.ClientID, CD.DocumentID, D.DocumentFileName, D.DocumentTypeID, DT.Description
	from Client_Document CD
	left join Document D on D.DocumentID = CD.DocumentID
	left join DocumentType DT on DT.DocumentTypeID = D.DocumentTypeID
	where D.DocumentFileName like '%.pdf' or D.DocumentFileName like '%.rtf' or D.DocumentFileName like '%.doc%' 
	or D.DocumentFileName like '%.xls%' or D.DocumentFileName like '%.html')

--Deloitte - TOS (2015-2016).pdf | ID: 15 
--T&C (2015) - Signed.pdf

, ClientDocument as (SELECT
     ClientID,
     STUFF(
         (SELECT ', ' + DocumentFileName
          from  Documents
          WHERE ClientID = a.ClientID
		  order by DocumentTypeID asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS ClientDocument
FROM Documents as a
GROUP BY a.ClientID)

--CLIENT OWNERS
, MaxRecruiterEmailID as (select R.RecruiterID, max(E.EmailID) as MaxEmailID from Recruiter R
	left join Person_Email PE on PE.PersonID = R.PersonID
	left join Email E on E.EmailID = PE.EmailID
	where E.Deleted = 0
	group by R.RecruiterID)

, ClientOwners as (select RC.ClientID, RC.RecruiterID, MRE.MaxEmailID, Email.EmailAddress 
	from Recruiter_Client RC
	left join MaxRecruiterEmailID MRE on MRE.RecruiterID = RC.RecruiterID
	left join Email on Email.EmailID = MRE.MaxEmailID)

--MAIN SCRIPT
select
concat('MSC',C.ClientID) as 'company-externalId' --> MSC is abbreviation for Mindscope Cura
, C.ClientName as '(OriginalCompanyName)'
, iif(C.ClientID in (select ClientID from dup where dup.rn > 1)
	, iif(dup.ClientName = '' or dup.ClientName is NULL,concat('Client Name -',dup.ClientID),concat(dup.ClientName,'-DUPLICATE-',dup.ClientID))
	, iif(C.ClientName = '' or C.ClientName is null,concat('Client Name -',dup.ClientID),C.ClientName)) as 'company-name'
, replace(coalesce(AddressType + ': ','') + stuff((coalesce(', ' + Street,'') + coalesce(', ' + Street2,'') + coalesce(', ' + City,'')
	+ coalesce(', ' + Province,'') + coalesce(', ' + PostalZip,'') + coalesce(', ' + Country,'')),1,2,''),',,',',') as 'company-locationName'
, replace(coalesce(AddressType + ': ','') + stuff((coalesce(', ' + Street,'') + coalesce(', ' + Street2,'') + coalesce(', ' + City,'')
	+ coalesce(', ' + Province,'') + coalesce(', ' + PostalZip,'') + coalesce(', ' + Country,'')),1,2,''),',,',',') as 'company-locationAddress'
, A.City as 'company-locationCity'
, case 
	when A.Country like '%Argentina%' then 'AR'
	when A.Country like '%Australia%' then 'AU'
	when A.Country like '%Austria%' then 'AT'
	when A.Country like '%Bangladesh%' then 'BD'
	when A.Country like '%Belgium%' then 'BE'
	when A.Country like '%Bermuda%' then 'BU'
	when A.Country like '%Brazil%' then 'BR'
	when A.Country like '%Cameroon%' then 'CM'
	when A.Country like '%Canada%' then 'CA'
	when A.Country like '%Cayman Islands%' then 'KY'
	when A.Country like '%China%' then 'CN'
	when A.Country like '%Colombia%' then 'CO'
	when A.Country like '%Denmark%' then 'DK'
	when A.Country like '%Dominican Republic%' then 'DO'
	when A.Country like '%Ecuador%' then 'EC'
	when A.Country like '%Finland%' then 'FI'
	when A.Country like '%France%' then 'FR'
	when A.Country like '%Germany%' then 'DE'
	when A.Country like '%Ghana%' then 'GH'
	when A.Country like '%Greece%' then 'GR'
	when A.Country like '%Hong Kong%' then 'HK'
	when A.Country like '%Hungary%' then 'HU'
	when A.Country like '%India%' then 'IN'
	when A.Country like '%Indonesia%' then 'ID'
	when A.Country like '%Iran%' then 'IR'
	when A.Country like '%Ireland%' then 'IE'
	when A.Country like '%Italy%' then 'IT'
	when A.Country like '%Japan%' then 'JP'
	when A.Country like '%Jordan%' then 'JO'
	when A.Country like '%Lebanon%' then 'LB'
	when A.Country like '%Luxembourg%' then 'LU'
	when A.Country like '%Macao%' then 'MO'
	when A.Country like '%Malaysia%' then 'MY'
	when A.Country like '%Mexico%' then 'MX'
	when A.Country like '%Mongolia%' then 'MN'
	when A.Country like '%Nepal%' then 'NP'
	when A.Country like '%Netherlands%' then 'NL'
	when A.Country like '%New Zealand%' then 'NZ'
	when A.Country like '%Nigeria%' then 'NG'
	when A.Country like '%Pakistan%' then 'PK'
	when A.Country like '%Papua New Guinea%' then 'PG'
	when A.Country like '%Philippines%' then 'PH'
	when A.Country like '%Poland%' then 'PL'
	when A.Country like '%Portugal%' then 'PT'
	when A.Country like '%Qatar%' then 'QA'
	when A.Country like '%Romania%' then 'RO'
	when A.Country like '%Russia%' then 'RU'
	when A.Country like '%Saudi Arabia%' then 'SA'
	when A.Country like '%Singapore%' then 'SG'
	when A.Country like '%South Africa%' then 'ZA'
	when A.Country like '%South Korea%' then 'KR'
	when A.Country like '%Spain%' then 'ES'
	when A.Country like '%Sri Lanka%' then 'LK'
	when A.Country like '%Sweden%' then 'SE'
	when A.Country like '%Switzerland%' then 'CH'
	when A.Country like '%Taiwan%' then 'TW'
	when A.Country like '%Tanzania%' then 'TZ'
	when A.Country like '%Thailand%' then 'TH'
	when A.Country like '%Turkey%' then 'TR'
	when A.Country like '%United Arab Emirates%' then 'AE'
	when A.Country like '%United Kingdom%' then 'GB'
	when A.Country like '%United States%' then 'US'
	when A.Country like '%Venezuela%' then 'VE'
	else A.Country end as 'company-locationCountry'
, A.PostalZip as 'company-locationZipCode'
, A.Province as 'company-locationState'
, CP.ClientPhone as 'company-phone'
, CF.ClientFax as 'company-fax'
, left(C.WebAddress,99) as 'company-website'
, CO.EmailAddress as 'company-owners'
, CD.ClientDocument as 'company-document'
, concat(concat('Client external ID: ', C.ClientID,char(10))
	, iif(C.IndustryTypeID is NULL,'',concat('*Industry type: ',IT.Description,char(10)))
	, iif(C.DivisionID is NULL,'',concat('*Division: ',D.Description,char(10)))
	, iif(C.DepartmentID is NULL,'',concat('*Department: ',DE.Description,char(10)))
	, iif(CA.ClientAddresses = '' or ClientAddresses is NULL,'',concat('*Client addresses: ',char(10),replace(CA.ClientAddresses,',,',','),char(10),char(10)))
	, iif(cast(Note.NoteText as nvarchar(max)) = '' or Note.NoteText is NULL,'',concat('*Client Profile: ',Note.NoteText))
	) as 'company-note'
from Client C
left join dup on dup.ClientID = C.ClientID
left join ClientPhone CP on CP.ClientID = C.ClientID
left join IndustryType IT on IT.IndustryTypeID = C.IndustryTypeID
left join Note on Note.NoteID = C.ProfileNoteID --> most of notes are client profile notes
left join Addresses A on A.AddressID = C.DefaultAddressID --> select default client address as shown address
left join ClientAddresses CA on CA.ClientID = C.ClientID --> one client may have multiple addresses
left join ClientDocument CD on CD.ClientID = C.ClientID
left join ClientOwners CO on CO.ClientID = C.ClientID
left join Division D on D.DivisionID = C.DivisionID
left join Department DE on DE.DepartmentID = C.DepartmentID
left join ClientFax CF on CF.ClientID = C.ClientID
where C.Deleted = 0

UNION ALL

select 'MSC9999999','','Default Company - Captiare','','','','','','','','','','','','This is default company from data import'