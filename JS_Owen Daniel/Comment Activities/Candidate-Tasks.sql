with ManCon as (select tblConsultant.ContactID as 'ContactID', tblContact.LastName + ' ' + tblContact.FirstName as 'Name', tblEmailAddress.EmailAddress as 'Mail' 
from tblConsultant left join tblContact on tblConsultant.ContactID = tblContact.ContactID
left join tblEmailAddress on tblEmailAddress.ContactID = tblConsultant.ContactID)


select a.ContactID as external_id,
concat('Creator: ',ManCon.Mail,(char(13)+char(10)),
'Subject: ',b.Subject,(char(13)+char(10)),
'Note: ', b.Notes) as 'content',
iif(b.Startdate is null,'',b.Startdate) as insert_timestamp,
'comment' as 'category', 
'candidate' as 'type', 
-10 as 'user_account_id'
from tblLogCandidate a left join tblTask b on a.ID = b.intID
left join ManCon on b.Owner = ManCon.ContactID
where a.type = 0

