with Phone as (
select Contact_ID as 'ContactID' from TblContactTelLink group by Contact_ID having count(Contact_ID) > 1 ),
-------
ManCon as (select tblConsultant.ContactID as 'ContactID', tblContact.LastName + ' ' + tblContact.FirstName as 'Name', tblEmailAddress.EmailAddress as 'Mail' 
from tblConsultant left join tblContact on tblConsultant.ContactID = tblContact.ContactID
left join tblEmailAddress on tblEmailAddress.ContactID = tblConsultant.ContactID),
-------
EmailLink as (select a.ContactID, a.Type, a.EmailAddress, ROW_NUMBER() over(partition by a.ContactID
order by a.ContactID asc) as 'EmailLink' from tblEmailAddress a),
------
Email as (Select EmailLink.ContactID, EmailLink.Type, EmailLink.EmailAddress from EmailLink where EmailLink.EmailLink = 1),
------
PriMail as (select Email.ContactID as 'ContactID', Email.EmailAddress as 'Mail' 
from Email where Email.Type = '3' or Type = '1'),
-------
dupmailpara as (select iif(Email.TYPE='3' or Email.TYPE='1',PriMail.mail,'') as 'email', tblContact.ContactID,
ROW_NUMBER() over(partition by PriMail.mail
order by tblContact.ContactID asc) as 'EmailNum'
from tblContact
left join Email on tblContact.ContactID = Email.ContactID
left join PriMail on tblContact.ContactID = PriMail.ContactID),
------
dupmail as (select iif(dupmailpara.EmailNum = 2,concat('2-',dupmailpara.email),dupmailpara.email) as 'mail', dupmailpara.ContactID
from dupmailpara),
-------
TelLink as (select TblContactTelLink.Contact_ID, TblContactTelLink.tel_type_id, TblContactTelLink.CntryCode, TblContactTelLink.StdCode,
TblContactTelLink.PhoneNo, ROW_NUMBER() over(partition by TblContactTelLink.Contact_ID
order by TblContactTelLink.tel_type_id asc) as 'TelLink' from TblContactTelLink),
------
PhoneLink as (select TelLink.Contact_ID, TelLink.tel_type_id, CntryCode,StdCode,PhoneNo from TelLink where TelLink.TelLink = 1),
------
phonenum as (select Contact_iD as 'ContactID', concat(CntryCode, StdCode, PhoneNo) as 'PhoneNumber'
from TblContactTelLink where tel_type_id = '6')
------




select tblContact.ContactID as 'contact-externalId',
iif(tblContactClientLinkTable.ClientID='' or tblContactClientLinkTable.ClientID is null,'0',tblContactClientLinkTable.ClientID) as 'contact-companyId',
iif(tblContact.LastName = '' or tblContact.LastName is null,'No Name', tblContact.LastName) as 'contact-lastName',
iif(tblContact.MiddleName = '' or tblContact.MiddleName is null,'',tblContact.MiddleName) as 'contact-middleName',
iif(tblContact.FirstName = '' or tblContact.FirstName is null,'No Name',tblContact.FirstName) as 'contact-firstName',
iif(Email.ContactID = dupmail.ContactID,dupmail.mail,'') as 'contact-email',
case when (TblContact.ContactID = PhoneLink.Contact_ID) then iif(phonenum.PhoneNumber is null,'',phonenum.PhoneNumber)
else '' end as 'contact-phone',
iif(tblContactClientLinkTable.ManConsultantID = ManCon.ContactID,ManCon.Mail,'') as 'contact-owners',
iif(tblContactClientLinkTable.Position = '' or tblContactClientLinkTable.Position is null,'',tblContactClientLinkTable.Position) as 'contact-jobTitle',
concat('Contact External ID: ',tblContact.ContactID,char(13)+char(10),nullif(concat('Note:',tblContactClientLinkTable.strNotes),'Note:  ')) as 'contact-Note'
------ add contact external id to brief note ------

from tblContact
left join PhoneLink on tblContact.ContactID = PhoneLink.Contact_ID
left join Email on tblContact.ContactID = Email.ContactID
left join tblContactClientLinkTable on tblContact.ContactID = tblContactClientLinkTable.ContactID
left join phonenum on tblContact.ContactID = phonenum.ContactID
left join phone on tblContact.ContactID = phone.ContactID
left join ManCon on tblContactClientLinkTable.ManConsultantID = ManCon.ContactID
left join PriMail on tblContact.ContactID = PriMail.ContactID
left join dupmail on tblContact.ContactID = dupmail.ContactID
where tblContact.IsAClientContact = '1'

