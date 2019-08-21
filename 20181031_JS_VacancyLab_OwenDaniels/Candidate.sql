with EmailLink as (select a.ContactID, a.Type, a.EmailAddress, ROW_NUMBER() over(partition by a.ContactID
order by a.ContactID asc) as 'EmailLink' from tblEmailAddress a),
------
Email as (Select tblContact.ContactID, EmailLink.Type, EmailLink.EmailAddress from tblContact
left join EmailLink on tblContact.ContactID = EmailLink.ContactID where EmailLink.EmailLink = 1 and IsACandidate = 1),
------
homemail as (select c.ContactID as 'ContactID', b.ID as 'id', b.Description as 'type',
a.EmailAddress as 'email' from tblCandidate c left join Email a on c.ContactID=a.ContactID
left join tblEmailType b on a.Type = b.id where a.Type = '1' or a.Type = '2'),
------
workmail as (select c.ContactID as 'ContactID', b.ID as 'id', b.Description as 'type',
a.EmailAddress as 'email' from tblCandidate c left join Email a on c.ContactID=a.ContactID
left join tblEmailType b on a.Type = b.id where a.Type = '3'),
------
dupmailpara as (select homemail.email as 'email', tblCandidate.ContactID,
ROW_NUMBER() over(partition by homemail.email
order by tblCandidate.ContactID asc) as 'EmailNum'
from tblcandidate left join tblcontact on tblcandidate.ContactID = tblContact.ContactID
left join Email on tblContact.ContactID = Email.ContactID
left join homemail on tblContact.ContactID = homemail.ContactID),
------
dupmail as (select iif(dupmailpara.EmailNum = 2,concat('2-',dupmailpara.email),dupmailpara.email) as 'mail', dupmailpara.ContactID
from dupmailpara),
------
HomeAdd as (select tblContactAddress.ContactId as 'ContactID', concat(tblAddress.Building, ', ', tblAddress.Town, ', ', tblAddress.PostalCode, ', ', tblAddress.Country) as 'Address', tblAddress.PostalCode as 'ZipCode'
  from tblContactAddress left join tblAddress on tblContactAddress.AddressId = tblAddress.ID
  where tblAddress.Type = '1'),
------
WorkPhone as (select Contact_iD as 'ContactID', concat(CntryCode, StdCode, PhoneNo) as 'PhoneNumber'
from TblContactTelLink where tel_type_id = '2'),
------
MobilePhone as (select Contact_iD as 'ContactID', concat(CntryCode, StdCode, PhoneNo) as 'PhoneNumber'
from TblContactTelLink where tel_type_id = '6'),
------
ManCon as (select tblConsultant.ContactID as 'ContactID', tblContact.LastName + ' ' + tblContact.FirstName as 'Name', tblEmailAddress.EmailAddress as 'Mail' 
from tblConsultant left join tblContact on tblConsultant.ContactID = tblContact.ContactID
left join tblEmailAddress on tblEmailAddress.ContactID = tblConsultant.ContactID),
------
LastJour as (select tblCandidate.ContactID, logdate from tblCandidate left join tblLogCandidate on tblCandidate.ContactID = tblLogCandidate.ContactID
left join tblExternalEventLog on tblLogCandidate.ID = tblExternalEventLog.ID),
------
TopValue as (select LastJour.ContactID, LastJour.logdate, ROW_NUMBER() over(partition by LastJour.ContactID
order by logdate desc) as 'LastJournal' from LastJour),
------
LastJournal as (select TopValue.ContactID, TopValue.LogDate from TopValue where TopValue.LastJournal = 1),
------
MaritalStt as (select tblCandidate.ContactID, 
case when (MaritalStatusID=107) then 'Married'
when MaritalStatusID=108 then 'Single'
when MaritalStatusID=109 then 'Unknown'
when MaritalStatusID=664 then 'Separated'
when MaritalStatusID=681 then 'Divorced' else 'Unknown' end as 'Status' from tblCandidate),
------
docdup as 
(select CandidateID as 'CandidateID', Filename as 'FileName' from tblDocument where CandidateID is not null),
------
docname as
(SELECT CandidateID as 'CandidateID',
    STUFF((SELECT DISTINCT ', ' + FileName
           FROM docdup a 
           WHERE a.CandidateID = b.CandidateID
          FOR XML PATH('')), 1, 2, '') as 'FileName'
FROM docdup b
GROUP BY CandidateID)
------





----Main Script------
select
tblCandidate.ContactID as 'candidate-externalid',
iif(tblContact.Greeting='' or tblContact.Greeting is null,'',tblContact.Greeting) as 'candidate-title',
iif(tblContact.FirstName='' or tblContact.FirstName is null,'No Name',tblContact.FirstName) as 'candidate-firstName',
tblContact.MiddleName as  'candidate-middleName',
iif(tblContact.LastName='' or tblContact.LastName is null,'No Name',tblContact.LastName) as 'candidate-Lastname',
iif(tblCandidate.ContactID=tblContact.ContactID and Email.TYPE in (1,2),dupmail.mail,'') as 'candidate-email',
iif(tblCandidate.ContactID=tblContact.ContactID and Email.Type='3',workmail.email,'') as 'candidate-workEmail',
case when (tblCandidate.Sex = '0') then 'MALE'
when tblCandidate.Sex = '1' then 'FEMALE' else '' end as 'candidate-gender',
iif(tblCandidate.DateOfBirth='' or tblCandidate.DateOfBirth is null,'',convert(varchar(50), tblCandidate.DateOfBirth, 111)) as 'candidate-dob',
iif(tblCandidate.ContactID = HomeAdd.ContactID, HomeAdd.address,'') as 'candidate-address',
case when (tblCandidate.NationalityID in ('665', '26')) then 'GB' 
when tblCandidate.NationalityID = '671' then 'ZA'
when tblCandidate.NationalityID = '672' then 'IN' else '' end as 'candidate-citizenship',
case when tblAddress.Country = 'UK' then 'GB'
when (tblAddress.Country = CTCode.Country_Name or tblAddress.Country = CTCode.Country_Code)
then CTCode.Country_Code else '' end as 'candidate-Country',
iif(tblAddress.Town = '' or tblAddress.Town is null,'',tblAddress.Town) as 'candidate-city',
iif(tblCandidate.ContactID = jlc_tblSocialAddress.ContactID,jlc_tblSocialAddress.SocialAddress,'') as 'candidate-linkedln',
iif(tblCandidate.ContactID = WorkPhone.ContactID, WorkPhone.PhoneNumber,'') as 'candidate-mobile',
iif(tblCandidate.ContactID = MobilePhone.ContactID, MobilePHone.PhoneNumber,'') as 'candidate-phone',
iif(tblCandidate.ManConsultantID = ManCon.ContactID,ManCon.Mail,'') as 'candidate-owners',
iif(tblCandidate.ContactID = HomeAdd.ContactID, HomeAdd.ZipCode,'') as 'candidate-zipCode',
concat('Candidate External ID: ',tblCandidate.ContactID,(char(13)+char(10)),
nullif(('Note: ' + tblCandidate.strNotes + (char(13)+char(10))),('Note: ' + (char(13)+char(10)))),
'Last Journal: ', LastJournal.LogDate,(char(13)+char(10)),
'Candidate Reference: ',tblCandidate.CandidateRef,(char(13)+char(10)),
'Registered Date: ', tblContact.CreDate,(char(13)+char(10)),
nullif(('GDPR Status: ' + jlc_tblGDPRStatus.Description + (char(13)+char(10))),('GDPR Status: ' + (char(13)+char(10)))),
nullif(concat('Marital Status: ', MaritalStt.Status),'Marital Status: '),
(char(13)+char(10)),
nullif(concat('Comment: ',tblCandidate.StrNotes),'Comment: ')
) as 'candidate-note'
---- candidate external id ------


from tblCandidate
left join tblContact on tblCandidate.ContactID = tblContact.ContactID
left join dupmail on tblCandidate.ContactID = dupmail.ContactID
left join workmail on tblCandidate.ContactID = workmail.ContactID
left join HomeAdd on tblCandidate.ContactID = HomeAdd.ContactID
left join tblAddress on tblCandidate.ContactID = tblAddress.ID
left join CTCode on tblAddress.Country = CTCode.Country_Name
left join jlc_tblSocialAddress on tblCandidate.ContactID = jlc_tblSocialAddress.ContactID
left join WorkPhone on tblCandidate.ContactID = WorkPhone.ContactID
left join MobilePhone on tblCandidate.ContactID = MobilePhone.ContactID
left join tblContactClientLinkTable on tblCandidate.ContactID = tblContactClientLinkTable.ContactID
left join ManCon on ManCon.ContactID = tblCandidate.ManConsultantID
left join LastJournal on tblCandidate.ContactID = LastJournal.ContactID
left join jlc_tblGDPRStatus on tblContact.GDPRStatus = jlc_tblGDPRStatus.ID
left join MaritalStt on tblCandidate.ContactID = MaritalStt.ContactID
left join docname on tblCandidate.ContactID = docname.CandidateID
left join Email on tblCandidate.ContactID = Email.ContactID
where tblcandidate.ContactID = 8350