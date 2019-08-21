--Contact location: will be added to note
with
-------------------Get contacts works for more than 1 companies
 dupContacts as (select *
, replace(replace(replace(replace(coalesce(nullif(Email,''),Home_Email),' ',''),';;',';'),';',','),':','') as ContEmail
, replace(replace(replace(replace(Home_Email,' ',''),';;',';'),';',','),':','') as HomeEmail1
, ROW_NUMBER() OVER(PARTITION BY Contact_Unique_ID ORDER BY Site_Unique ASC) AS rn
from ContactManagementContacts)
--select Contact_Unique_ID,Site_Unique,rn from dupContacts where rn>1 order by Contact_Unique_ID

, Contact as (
select *
, iif(rn=1,Contact_Unique_ID,concat(Contact_Unique_ID,'_',Site_Unique))as contactID
, iif(right(HomeEmail1,1) in ('.',','),left(HomeEmail1,len(HomeEmail1)-1),iif(left(HomeEmail1,1) = ',',right(HomeEmail1,len(HomeEmail1)-1),HomeEmail1)) as HomeEmail
, iif(right(ContEmail,1) in ('.',','),left(ContEmail,len(ContEmail)-1),iif(left(ContEmail,1) = ',',right(ContEmail,len(ContEmail)-1),ContEmail)) as ContactEmail
from dupContacts)
--select * from contact

----------Contact Email
--check email format
, EmailDupRegconition as (SELECT contactID,ContactEmail Email,
 ROW_NUMBER() OVER(PARTITION BY ContactEmail ORDER BY contactID ASC) AS rn 
from Contact
where ContactEmail like '%_@_%.__%')
--select * from EmailDupRegconition

--edit duplicating emails
, ContactEmail as (select contactID, rn
, iif(rn=1,Email,iif(Email not like '%,%',concat(rn,'_',Email),concat(rn,'_',replace(Email,',',concat(',',rn,'_'))))) as Email
from EmailDupRegconition)
--select * from ContactEmail where email like '%,%'

, loc as (
	select contactID,Contact_Unique_ID, Home_Address_Hse_No,Home_Address_Line_1,Home_Address_Line_2,Home_Address_Line_3
			,Home_Address_Line_4,Home_Address_Line_5,Home_Address_Line_6,Home_Postcode
			, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(Home_Address_Hse_No, ''), '')
			+ Coalesce(' ' + NULLIF(Home_Address_Line_1, ''), '')
			+ Coalesce(', ' + NULLIF(Home_Address_Line_2, ''), '')
			+ Coalesce(', ' + NULLIF(Home_Address_Line_3, ''), '')
			+ Coalesce(', ' + NULLIF(Home_Address_Line_4, ''), '')
			+ Coalesce(', ' + NULLIF(Home_Address_Line_5, ''), '')
			+ Coalesce(', ' + NULLIF(Home_Address_Line_6, ''), '')
			+ Coalesce(', ' + NULLIF(Home_Postcode, ''), '')
			, 1, 1, '')) as 'locationName'
	from Contact)

----------------Users
, Users as (select concat(Users_Name,STMP_Account) as UserName, concat(iif(User_Email like '%@%',User_Email,''),Sync_Mailbox_Name) as UserEmail
from UserProfiles)


---------MAIN SCRIPT
--, main as (
select 
iif(cc.Site_Unique = '' or cc.Site_Unique is NULL,'GP9999999',concat('GP',cc.Site_Unique)) as 'contact-companyId'
, cc.Site_Unique as '(OriginalCompanyID)'
, c.Organisation as '(OriginalCompanyName)'
, concat('GP',cc.contactID) as 'contact-externalId'
, iif(cc.Forename = '' or cc.Forename is NULL,concat('NoFirstname-', cc.Contact_Unique_ID),cc.Forename) as 'contact-firstName'
, iif(cc.Surname = '' or cc.Surname is NULL,concat('NoLastName-', cc.Contact_Unique_ID),cc.Surname) as 'contact-lastName'
, ce.Email as 'contact-email'
, ltrim(Stuff(Coalesce(' ' + NULLIF(cc.Telephone, ''), '')
              + Coalesce(',' + NULLIF(cc.DDI, ''), '')
                , 1, 1, '') ) as 'contact-phone'--combine telephone and DDI
--, coalesce(nullif(cc.DDI,' '),iif(Mobile = '',Personal_Mobile,iif(Personal_Mobile in (Mobile,'',' '),Mobile,concat(Mobile,',',Personal_Mobile))),Home_Telephone) as 'contact-phone'
, iif(cc.Mobile = '',cc.Personal_Mobile,iif(cc.Personal_Mobile in (cc.Mobile,'',' '),cc.Mobile,concat(cc.Mobile,',',cc.Personal_Mobile))) as 'contact-mobile'--NEED INJECTING
, cc.Role_Description as 'contact-jobTitle'
, case cc.Title
		when 'SIR' then 'MR' 
		when 'Mrs' then 'MRS'
		when 'Miss' then 'MISS'
		when 'Ms' then 'MS'
		when 'Mr' then 'MR'
		else '' end as 'contact-title' --NEED INJECTING
, cc.HomeEmail as 'Personal Email' --NEED INJECTING
, cc.Home_Telephone as Homephone -- NEED INJECTING
--, iif(CHARINDEX(':',cc.Date_of_Birth)<>0,concat(substring(cc.Date_of_Birth,6,2),'/',substring(cc.Date_of_Birth,9,2),'/',left(cc.Date_of_Birth,4)),cc.Date_of_Birth) as 'contact_dob'--NEED INJECTING
, iif(cc.Date_of_Birth = '','',iif(CHARINDEX(':',cc.Date_of_Birth)<>0,concat(substring(cc.Date_of_Birth,9,2),'/',substring(cc.Date_of_Birth,6,2),'/',left(cc.Date_of_Birth,4)),concat(substring(cc.Date_of_Birth,4,2),'/',left(cc.Date_of_Birth,2),'/',right(cc.Date_of_Birth,4)))) as 'contact_dob'--mm/dd/yyyy
, u.UserEmail as 'contact-owners'--Use company manager as contact owner
, left(
	concat('Contact Original ID: GP',cc.Contact_Unique_ID,char(10)
	, concat(char(10),'Contact External ID: ',cc.contactID,char(10))
	, iif(cc.Constructed_Salut = '','',concat(char(10),'Preferred Name: ',cc.Constructed_Salut,char(10)))
	, iif(c.Organisation = '','',concat(char(10),'Company: ',c.Organisation,char(10)))
	--, iif(cont_mobnum = '' or cont_mobnum is NULL,'',concat(char(10),'Mobile Phone: ',replace(cont_mobnum,' ',''),char(10)))
	, iif(cc.Marital_Status = '','',concat(char(10),'Marital Status: ',cc.Marital_Status,char(10)))
	--, iif(loc.locationName = '' or loc.locationName is NULL,'',concat(char(10),'Home Address: ',replace(replace(loc.locationName,',,',','),', ,',','),char(10)))
	, iif(cc.Fax = '','',concat(char(10),'Fax: ',cc.Fax,char(10)))
	, iif(cc.Web_address = '','',Concat(char(10), 'Web_address: ', cc.Web_address, char(10)))
	, iif(cc.Importance_in_Org = '','',Concat(char(10), 'Importance in Org: ', cc.Importance_in_Org, char(10)))
	, iif(cc.Enquiry_Source = '','',Concat(char(10), 'Enquiry Source: ', cc.Enquiry_Source, char(10)))
	, iif(cc.Link_Type_Code = '','',Concat(char(10), 'Link Type Code: ', cc.Link_Type_Code, char(10)))
	, iif(cc.Mailshot_YN = '','',Concat(char(10), 'Mailshot Y/N: ', cc.Mailshot_YN, char(10)))
	, iif(cc.New_Business_Rating = '','',Concat(char(10), 'New Business Rating: ', cc.New_Business_Rating, char(10)))
	, iif(cc.Next_Call_Date = '','',Concat(char(10), 'Next Call Date: ', cc.Next_Call_Date, char(10)))
	, iif(cc.Next_Call_Time = '','',Concat(char(10), 'Next Call Time: ', cc.Next_Call_Time, char(10)))
	, iif(cc.Position_Code = '','',Concat(char(10), 'Position Code: ', cc.Position_Code, char(10)))
	, iif(cc.Reports_to = '' or cc.Reports_to = 0,'',Concat(char(10), 'Reports to Contact''s ID: ', cc.Reports_to, char(10)))
	, iif(cc.Sales_Representative = '','',Concat(char(10), 'Sales Representative: ', cc.Sales_Representative, char(10)))
	, iif(cc.Social_Net_Site_1 = '','',Concat(char(10), 'Social Net Site 1: ', cc.Social_Net_Site_1, char(10)))
	, iif(cc.Upper_Forename = '','',Concat(char(10), 'Upper Forename: ', cc.Upper_Forename, char(10)))
	, iif(cc.Upper_Surname = '','',Concat(char(10), 'Upper Surname: ', cc.Upper_Surname, char(10)))
	, iif(cc.User_Alphas_001 = '','',Concat(char(10), 'User Alphas 1: ', cc.User_Alphas_001, char(10)))
	, iif(cc.User_Alphas_002 = '','',Concat(char(10), 'User Alphas 2: ', cc.User_Alphas_002, char(10)))
	, iif(CHARINDEX(':',cc.Creation_Date)<>0,concat(char(10), 'Creation Date: ', substring(cc.Creation_Date,6,2),'/',substring(cc.Creation_Date,9,2),'/',left(cc.Creation_Date,4),char(10)),concat(char(10), 'Creation Date: ',cc.Creation_Date,char(10)))
	, iif(cc.Creating_User = '','',Concat(char(10), 'Creating User: ', cc.Creating_User, char(10)))
	, iif(CHARINDEX(':',cc.Amendment_Date)<>0,concat(char(10), 'Amendment Date: ', substring(cc.Amendment_Date,6,2),'/',substring(cc.Amendment_Date,9,2),'/',left(cc.Amendment_Date,4),char(10)),concat(char(10), 'Amendment Date: ',cc.Amendment_Date,char(10)))
	, iif(cc.Amending_User = '','',Concat(char(10), 'Amending User: ', cc.Amending_User, char(10)))
	, iif(cc.Main_Site_Unique = '','',concat(char(10),'Main Site Unique ID: ',cc.Main_Site_Unique,char(10))))
	,32000) 
	as 'contact-note'
from Contact cc
	--ContactID id left join ContactManagementContacts cc on id.Contact_Unique_ID = cc.Contact_Unique_ID and id.Site_Unique = cc.Site_Unique
	left join ContactManagementSites c on cc.Site_Unique = c.Site_Unique_Id
	left join ContactEmail ce on cc.contactID = ce.contactID
	left join Users u on cc.Account_Manager = u.UserName
	left join loc on cc.ContactID = loc.ContactID
--where cc.Site_Unique = 853
--where cc.cont_ref = 777--email2 <> ''--cc.id = 30427055
UNION ALL
select 'GP9999999','','','GP9999999','Default','Contact','','','','','','','','','','This is default contact from Data Import'
--)
--select * from main where [contact-phone] is not null