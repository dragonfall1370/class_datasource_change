with ManCon as (select tblConsultant.ContactID as 'ContactID', tblContact.LastName + ' ' + tblContact.FirstName as 'Name', tblEmailAddress.EmailAddress as 'Mail' 
from tblConsultant left join tblContact on tblConsultant.ContactID = tblContact.ContactID
left join tblEmailAddress on tblEmailAddress.ContactID = tblConsultant.ContactID)


select tblClient.ClientID as external_id,
iif(tblLogClient.ID = tblExternalEventLog.ID,concat('Creator: ',ManCon.Mail,(char(13)+char(10)),'Subject: ',tblExternalEventLog.EventDescription,'Note: ',tblExternalEventLog.Notes),'') as 'content',
iif(tblExternalEventLog.LogDate is null,'',tblExternalEventLog.LogDate) as insert_timestamp,
'comment' as 'category', 
'candidate' as 'type', 
-10 as 'user_account_id'
from tblClient
left join tblLogClient on tblClient.ClientID = tblLogClient.ClientID
left join tblExternalEventLog on tblLogClient.ID = tblExternalEventLog.ID
left join ManCon on tblExternalEventLog.Owner = ManCon.ContactID