with ManCon as (select tblConsultant.ContactID as 'ContactID', tblContact.LastName + ' ' + tblContact.FirstName as 'Name', tblEmailAddress.EmailAddress as 'Mail' 
from tblConsultant left join tblContact on tblConsultant.ContactID = tblContact.ContactID
left join tblEmailAddress on tblEmailAddress.ContactID = tblConsultant.ContactID)


select tblVacancy.VacancyRef as external_id,
iif(tblLogVacancy.ID = tblExternalEventLog.ID,concat('Creator: ',ManCon.Mail,(char(13)+char(10)),'Subject: ',tblExternalEventLog.EventDescription,'Note: ',tblExternalEventLog.Notes),'') as 'Subject',
iif(tblExternalEventLog.LogDate is null,'',convert(varchar(50), tblExternalEventLog.LogDate, 111)) as insert_timestamp,
'comment' as 'category', 
'candidate' as 'type', 
-10 as 'user_account_id'
from tblVacancy
left join tblLogVacancy on ltrim(rtrim(tblVacancy.VacancyRef)) = ltrim(rtrim(tblLogVacancy.VacancyRef))
left join tblExternalEventLog on tblLogVacancy.ID = tblExternalEventLog.ID
left join ManCon on tblExternalEventLog.Owner = ManCon.ContactID