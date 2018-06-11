--Contact location: will be added to note
with tempLoc as(select cc.ClientContactId, cc.ContactPersonId,cc.ClientId, A.AddressId, A.Building, A.Street, A.District, A.City, A.PostCode,
						A.CountyValueId, lv.ValueName as County, A.CountryValueId, lv1.ValueName as Country
from ClientContacts cc left join Address a on cc.ContactPersonId = a.ObjectId
						left join ListValues lv on a.CountyValueId = lv.ListValueId
						left join ListValues lv1 on a.CountryValueId = lv1.ListValueId
 where a.AddressId is not null)

, loc as (
	select tl.ClientContactId, tl.ContactPersonId,
	ltrim(rtrim(concat(iif(tl.Building = '' or tl.Building is NULL,'',concat(tl.Building,', '))
	, iif(tl.Street = '' or tl.Street is NULL,'',concat(tl.Street,', '))
	, iif(tl.District = '' or tl.District is NULL,'',concat(tl.District,', '))
	, iif(tl.City = '' or tl.City is NULL,'',concat(tl.City,', '))
	, iif(tl.County = '' or tl.County is NULL,'',concat(tl.County,', '))
	, iif(tl.Country = '' or tl.Country is NULL,'',concat(tl.Country,', '))
	, iif(tl.Postcode = '' or tl.Postcode is NULL,'',tl.Postcode)))) as 'locationName'
	from tempLoc as tl)
------------
, combinedLoc1 as (SELECT ContactPersonId, 
     STUFF(
         (SELECT '; ' + locationName
          from  loc
          WHERE ContactPersonId = l.ContactPersonId
    order by ContactPersonId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS locationName
FROM loc as l
GROUP BY l.ContactPersonId)
---------------
, combinedLoc as (select cc.ClientContactId, cl1.LocationName
from CombinedLoc1 cl1 left join ClientContacts cc on cl1.ContactPersonId = cc.ContactPersonId)
--select * from combinedLoc
----------Contact Email
, TempPrimaryEmail as (select cc.ClientContactId, ContactPersonId, PrimaryEmailAddressPhoneId, p.Num as Email,
 ROW_NUMBER() OVER(PARTITION BY cc.contactPersonId ORDER BY cc.clientcontactID ASC) AS rn
from ClientContacts cc left join Phones p on cc.PrimaryEmailAddressPhoneId = p.PhoneId)
, PrimaryEmail as (select ClientContactId, ContactPersonId,
case 
when rn=1 then Email
else concat(rn,'_',ltrim(rtrim(Email)))
end as Email
from TempPrimaryEmail)
--where Email is not null and Email <> '')

, TempOfficeEmail as (select cc.ClientContactId, ContactPersonId, p.CommunicationTypeId, p.Num as OfficeEmail,
 ROW_NUMBER() OVER(PARTITION BY cc.contactPersonId ORDER BY cc.clientcontactID ASC) AS rn
from ClientContacts cc left join Phones p on cc.ContactPersonId = p.ObjectID
where p.CommunicationTypeId = 85)

--select * from TempOfficeEmail
, OfficeEmail as (select ClientContactId, ContactPersonId, OfficeEmail, rn
from TempOfficeEmail where rn=1)
--select * from OfficeEmail						
--where OfficeEmail is not null and OfficeEmail <> '')

, CombinedContactEmail as (select pe.ClientContactId, pe.ContactPersonId, pe.Email, oe.OfficeEmail
 from PrimaryEmail pe left join OfficeEmail oe on pe.ClientContactId = oe.ClientContactId)
--  select * from CombinedContactEmail
, ContactEmail1 as (select ClientContactId, ContactPersonId, coalesce(Email, OfficeEmail) as Email
  from CombinedContactEmail)
--select * from ContactEmail1

--check email format
, EmailDupRegconition as (SELECT clientContactID, ContactPersonId, Email, 
 ROW_NUMBER() OVER(PARTITION BY Email ORDER BY clientContactID ASC) AS rn 
from ContactEmail1
where Email like '%_@_%.__%')
--edit duplicating emails
, ContactEmail as (select ClientContactId, ContactPersonId,
case 
when rn=1 then Email
else concat(rn,'_',(Email))
end as Email
from EmailDupRegconition)

--ContactName:
, TempName as (select cc.ClientContactId, cc.ContactPersonId, p.PersonName as Firstname, p.Surname as LastName,
 ROW_NUMBER() OVER(PARTITION BY cc.contactPersonId ORDER BY cc.clientcontactID ASC) AS rn
from ClientContacts cc left join Person p on cc.ContactPersonId = p.PersonID)
--select * from TempName

, ContactName as (select ClientContactId, ContactPersonId, FirstName,
case 
when rn=1 then LastName
else concat(ltrim(rtrim(LastName)),'_',ClientContactId)
end as Lastname
from TempName)
----Gender
, gender as (select cc.clientContactID, cc.ContactPersonId, p.GenderValueId, lv.ValueName
from clientContacts cc left join Person p on cc.ContactPersonId = p.PersonID
    left join ListValues lv on lv.ListValueId = p.GenderValueId)

, ContactLinkedIn as (select cc.ClientContactId, cc.ContactPersonId, p.CommunicationTypeId, p.Num
from ClientContacts cc left join Phones p on cc.ContactPersonId = p.ObjectID
where (p.CommunicationTypeId = 89 or p.CommunicationTypeId = 90) and p.Num like '%linkedin%')

, ContactSkype as (select cc.ClientContactId, cc.ContactPersonId, p.CommunicationTypeId, p.Num
from ClientContacts cc left join Phones p on cc.ContactPersonId = p.ObjectID
where p.CommunicationTypeId = 90 and p.Num like '%skype')

--Contact Phone as Primary
, tempContactPhone as (select cc.ClientContactId, cc.ContactPersonId, p.CommunicationTypeId, replace(p.Num,' ','') as Num
from ClientContacts cc left join Phones p on cc.ContactPersonId = p.ObjectID
where p.CommunicationTypeId = 79)

, ContactPhone as (SELECT ContactPersonId, 
     STUFF(
         (SELECT ',' + Num
          from  tempContactPhone
          WHERE ContactPersonId = tcm.ContactPersonId
    order by ContactPersonId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS 'contactphone'
FROM tempContactPhone as tcm
GROUP BY tcm.ContactPersonId)
--select * from ContactPhone order by ContactPersonId

--Contact Mobile
, tempContactMobile as (select cc.ClientContactId, cc.ContactPersonId, p.CommunicationTypeId, p.Num
from ClientContacts cc left join Phones p on cc.ContactPersonId = p.ObjectID
where p.CommunicationTypeId = 83)

, ContactMobile as (SELECT ContactpersonId, 
     STUFF(
         (SELECT ',' + Num
          from  tempContactMobile
          WHERE ContactpersonId = tcm.ContactpersonId
    order by ContactpersonId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS 'contactmobile'
FROM tempContactMobile as tcm
GROUP BY tcm.ContactpersonId)
------Contact Office phone
, tempOfficePhone as (select cc.ClientContactId, cc.ContactPersonId, p.CommunicationTypeId, replace(p.Num,' ','') as Num
from ClientContacts cc left join Phones p on cc.ContactPersonId = p.ObjectID
where p.CommunicationTypeId = 87)

, OfficePhone as (SELECT ContactpersonId, 
     STUFF(
         (SELECT ',' + Num
          from  tempOfficePhone
          WHERE ContactpersonId = toff.ContactpersonId
    order by ContactpersonId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS 'officephone'
FROM tempOfficePhone as toff
GROUP BY toff.ContactpersonId)

----Contact's Comment
, tempComment as (select aa.ApplicantActionId, aag.ClientContactId, aag.ApplicantId, aag.StatusDescription, aa.Notes, aag.CVSentDate, aag.ApplicantFileAs, aag.ClientFileAs, aag.ClientContactFileAs,
 aag.JobRefNo, aag.JobTitle, aag.ConsultantUsername, aag.StatusDate, aag.CreatedUserName, aag.CreatedOn, CV.CVRefNo, aa.JobId, j.StartDate, et.Description
from ApplicantActions aa left join VW_APPLICANT_ACTION_GRID aag on aa.ApplicantActionId = aag.ApplicantActionId
							left join CV on aa.CVId = CV.CVId
							left join Jobs j on aa.JobId = j.JobId
							left join EmploymentTypes et on j.EmploymentTypeId = et.EmploymentTypeId)
, ContactComment as (SELECT
     ClientContactId,
     STUFF(
         (SELECT '<hr>' + 'Created date: ' + convert(varchar(20),CreatedOn,120) + char(10) + 'Created by: ' + CreatedUserName + char(10)
		  + coalesce('Relates to job: ' + JobTitle + char(10), '') + coalesce('Job Ref No. ' + JobRefNo + char(10), '')
		  + coalesce('Employment type: ' + Description + char(10), '') + coalesce('Job Start date: ' + convert(varchar(20),StartDate,120) + char(10), '')
		  + coalesce('Relates to candidate: ' + ApplicantFileAs + char(10), '') + coalesce('Relates to company: ' + ClientFileAs + char(10), '')
		  + coalesce('CV Ref No. ' + CVRefNo + char(10), '') + coalesce('CV Sent date: ' + convert(varchar(20),CVSentDate,120) + char(10), '')
		  + coalesce('Status: ' + StatusDescription + char(10), '') + coalesce('Status date:' + convert(varchar(20),StatusDate,120) + char(10), '')
		  + coalesce('Consultant: ' + Consultantusername + char(10), '') + iif(Notes = '' or Notes is null,'',concat('Comment content: ',Notes))
          from  tempComment
          WHERE ClientContactId = tcmt.ClientContactId
		  order by CreatedOn desc
          FOR XML PATH (''),TYPE).value('.','nvarchar(MAX)')
          , 1, 4, '')  AS AllComment
FROM tempComment as tcmt
GROUP BY tcmt.ClientContactId)

---------MAIN SCRIPT
--, main as (
select 
iif(cc.ClientID = '' or cc.ClientID is NULL,'MP9999999',concat('MP',cc.ClientID)) as 'contact-companyId'
, cc.ClientID as '(OriginalCompanyID)'
, c.Company as '(OriginalCompanyName)'
, concat('MP',cc.ClientContactID) as 'contact-externalId'
, iif(cn.FirstName = '' or cn.FirstName is NULL,concat('NoFirstname-', cn.clientContactID),cn.FirstName) as 'contact-firstName'
, iif(cn.LastName = '' or cn.LastName is NULL,concat('NoLastName-', cn.clientContactID),cn.LastName) as 'contact-lastName'
, ce.Email as 'contact-email'
, coalesce(replace(cp.contactphone,'/ ',','),replace(offp.officephone,'/ ',',')) as 'contact-phone'
--, cp.contactphone as 'contact-phone'
, cs.Num as 'contact-skype'
, cli.Num as 'contact-linkedin'
, cc.JobTitle as 'contact-jobTitle'
, left(ctc.AllComment,32000) as 'contact-comment'
, left(
	concat('Contact External ID: MP',cc.ClientContactId,char(10)
	, iif(gd.ValueName = '' or gd.ValueName is NULL,'',concat(char(10),'Gender: ',gd.ValueName,char(10)))
	, iif(ps.TitleValueId = '' or ps.TitleValueId is NULL,'',concat(char(10),'Title: ',lv.ValueName,char(10)))
	, iif(cc.PositionValueId = '' or cc.PositionValueId is NULL,'',concat(char(10),'Position: ',lv1.ValueName,char(10)))
	, iif(ce.Email = '' or ce.Email is NULL,'',concat(char(10),'Email: ',ce.Email,char(10)))
	, iif(cp.contactphone = '' or cp.contactphone is NULL,'',concat(char(10),'Primary Phone(s): ',cp.contactphone,char(10)))
	, iif(ctm.contactmobile = '' or ctm.contactmobile is NULL,'',concat(char(10),'Mobile(s): ',ctm.contactmobile,char(10)))
	, iif(offp.ContactPersonId = '' or offp.ContactPersonId is NULL,'',concat(char(10),'Office Phone(s): ',offp.ContactPersonId,char(10)))
	, iif(cl.locationName = '' or cl.locationName is NULL,'',concat(char(10),'Personal Address: ',cl.locationName,char(10)))
	--, coalesce(char(10) + 'Contact Other Notes: ' + ps.Notes, '')),32000) as 'contact-note'
	, iif(ps.Notes = '' or ps.Notes is NULL,'',concat(char(10),'Other Notes: ',ps.Notes))),32000) 
	as 'contact-note'
from ClientContacts cc
	left join Clients c on cc.ClientID = c.ClientID
	left join Person ps on cc.ContactPersonId = ps.PersonID
	left join Gender gd on cc.ClientContactID = gd.ClientContactID
	left join ContactEmail ce on cc.ClientContactID = ce.ClientContactID
	left join combinedLoc cl on cc.ClientContactID = cl.ClientContactID
	left join ContactName cn on cc.ClientContactID = cn.ClientContactID
	left join ContactLinkedIn cli on cc.ClientContactId = cli.ClientContactId
	left join ContactSkype cs on cc.ClientContactId = cs.ClientContactId
	left join ContactMobile ctm on cc.ContactPersonId = ctm.ContactPersonId
	left join ContactPhone cp on cc.ContactPersonId = cp.ContactPersonId
	left join OfficePhone offp on cc.ContactPersonId = offp.ContactPersonId
	left join ContactComment ctc on cc.ClientContactId = ctc.ClientContactId
	left join ListValues lv on ps.TitleValueId = lv.ListValueId
	left join ListValues lv1 on cc.PositionValueId = lv1.ListValueId

UNION ALL
select 'MP9999999','','','MP9999999','Default','Contact','','','','','','','This is default contact from Data Import'
--)
--select * from main where [contact-phone] is not null