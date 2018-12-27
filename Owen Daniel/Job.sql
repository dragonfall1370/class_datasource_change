with EmailLink as (select a.ContactID, a.Type, a.EmailAddress, ROW_NUMBER() over(partition by a.ContactID
order by a.ContactID asc) as 'EmailLink' from tblEmailAddress a),
------
Email as (Select tblContact.ContactID, EmailLink.Type, EmailLink.EmailAddress from tblContact
left join EmailLink on tblContact.ContactID = EmailLink.ContactID where EmailLink.EmailLink = 1 and IsACandidate = 1),
------
ManCon as (select tblConsultant.ContactID as 'ContactID', tblContact.LastName + ' ' + tblContact.FirstName as 'Name', tblEmailAddress.EmailAddress as 'Mail' 
from tblConsultant left join tblContact on tblConsultant.ContactID = tblContact.ContactID
left join tblEmailAddress on tblEmailAddress.ContactID = tblConsultant.ContactID),
------
Status as (select tblVacancy.ClientID,tblVacancy.StatusEN, jlc_tblVacancyStatus.StatusText, jlc_tblVacancyStatus.StatusID 
from tblVacancy left join jlc_tblVacancyStatus on tblVacancy.StatusEN = jlc_tblVacancyStatus.StatusID)
------
/*titlenum as (select iif(tblVacancy.ClientContactID='' or tblVacancy.ClientContactID is null,'0',tblVacancy.ClientContactID) as 'ContactID',
iif(tblVacancy.PositionID = tblPosition.PositionID,tblPosition.Description,'') as 'title',
iif(tblVacancy.CreDate = '' or tblVacancy.CreDate is null,'',convert(datetime,left(tblVacancy.CreDate,11))) as 'StartDate',
ROW_NUMBER() over(partition by tblVacancy.ClientContactID, tblPosition.Description, tblVacancy.CreDate
order by tblVacancy.ContactID asc) as 'titlenum'
from tblVacancy left join tblPosition on tblVacancy.PositionID = tblPosition.PositionID*/



select tblVacancy.VacancyRef as 'position-externalId',
iif(tblVacancy.ClientContactID='' or tblVacancy.ClientContactID is null,'0',tblVacancy.ClientContactID) as 'position-contactId',
iif(tblVacancy.PositionID = tblPosition.PositionID,tblPosition.Description,'') as 'position-title',
tblVacancy.NumVacancies as 'position-headcount',
iif(tblVacancy.ManConsultantID = ManCon.ContactID,ManCon.Mail,'') as 'position-owners',
nullif(concat('Job Description: ',tblVacancy.strJobDesc,(char(13)+char(10))),'Job Description: ')
+ (char(13)+char(10)) + nullif(concat('Duties: ',tblVacancy.strDuties),'Duties: ')  as 'position-publicDescription',
iif(tblVacancy.VacancyRef = tblDocument.VacancyRef, tblDocument.FileName,'') as 'position-document',
iif(tblVacancy.CreDate = '' or tblVacancy.CreDate is null,'',convert(datetime,left(tblVacancy.CreDate,11))) as 'position-startDate',
case when (tblVacancy.StatusEN in ('1','2','3')) then convert(datetime,dateadd(dd,-1, cast(getdate() as date)),11) else '2019-10-29 00:00:00.000' end as 'position-endDate'

----- add close date, if the status is closed, change the close date become yesterday----

from tblVacancy
left join tblPosition on tblVacancy.PositionID = tblPosition.PositionID
left join ManCon on tblVacancy.ManConsultantID = ManCon.ContactID
left join tblDocument on tblVacancy.VacancyRef = tblDocument.VacancyRef