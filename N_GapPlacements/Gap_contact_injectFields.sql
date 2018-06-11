with
 dupContacts as (select *
, replace(replace(replace(replace(coalesce(nullif(Email,''),Home_Email),' ',''),';;',';'),';',','),':','') as ContEmail
, replace(replace(replace(replace(Home_Email,' ',''),';;',';'),';',','),':','') as HomeEmail1
, ROW_NUMBER() OVER(PARTITION BY Contact_Unique_ID ORDER BY Site_Unique ASC) AS rn
from ContactManagementContacts)

, Contact as (
select *
, iif(rn=1,Contact_Unique_ID,concat(Contact_Unique_ID,'_',Site_Unique))as contactID
, iif(right(HomeEmail1,1) in ('.',','),left(HomeEmail1,len(HomeEmail1)-1),iif(left(HomeEmail1,1) = ',',right(HomeEmail1,len(HomeEmail1)-1),HomeEmail1)) as HomeEmail
, iif(right(ContEmail,1) in ('.',','),left(ContEmail,len(ContEmail)-1),iif(left(ContEmail,1) = ',',right(ContEmail,len(ContEmail)-1),ContEmail)) as ContactEmail
from dupContacts)

, temp as (
select 
concat('GP',cc.contactID) as ContactExternalId
, iif(cc.Mobile = '',cc.Personal_Mobile,iif(cc.Personal_Mobile in (cc.Mobile,'',' '),cc.Mobile,concat(cc.Mobile,',',cc.Personal_Mobile))) as ContactMobile--NEED INJECTING
, cc.Role_Description as 'contact-jobTitle'
, case cc.Title
		when 'SIR' then 'MR' 
		when 'Mrs' then 'MRS'
		when 'Miss' then 'MISS'
		when 'Ms' then 'MS'
		when 'Mr' then 'MR'
		else '' end as ContactTitle --NEED INJECTING
, cc.HomeEmail as personalemail --NEED INJECTING
, cc.Home_Telephone as Homephone -- NEED INJECTING
, iif(cc.Date_of_Birth = '','',iif(CHARINDEX(':',cc.Date_of_Birth)<>0,concat(substring(cc.Date_of_Birth,9,2),'/',substring(cc.Date_of_Birth,6,2),'/',left(cc.Date_of_Birth,4)),concat(substring(cc.Date_of_Birth,4,2),'/',left(cc.Date_of_Birth,2),'/',right(cc.Date_of_Birth,4)))) as 'contact_dob'--mm/dd/yyyy
, iif(cc.Date_of_Birth = ' ' or cc.Date_of_Birth like '%2019%','',iif(CHARINDEX(':',cc.Date_of_Birth)<>0,concat(left(cc.Date_of_Birth,4),'-',substring(cc.Date_of_Birth,9,2),'-',substring(cc.Date_of_Birth,6,2)),concat(right(cc.Date_of_Birth,4),'-',substring(cc.Date_of_Birth,4,2),'-',left(cc.Date_of_Birth,2)))) as birthdate--yyyy-mm-dd
from Contact cc	left join ContactManagementSites c on cc.Site_Unique = c.Site_Unique_Id)

select ContactExternalId, personalemail,Homephone
from temp where personalemail <> ''

