-- Activities Comments
-- Correspondence

declare @NewLineChar as char(2) = char(13) + char(10)
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar

select distinct
x.CorrespondenceID,
iif(x.CandidateID <> 0, x.CandidateID, null) as CanExtId
, iif(x.ContactID <> 0, x.ContactID, null) as ConExtId
, iif(x.CompanyID <> 0, x.CompanyID, null) as ComExtId
, iif(x.VacancyID <> 0, x.VacancyID, null) as JobExtId
, x.CreationDate as insert_timestamp
, concat(
upper('Correspondence')
, @NewLineChar
, replicate('-', len('Correspondence'))
, @NewLineChar
, trim(@NewLineChar from concat(
	iif(len(trim(isnull(x.Title, ''))) > 0, 'Title: ' + trim(isnull(x.Title, '')), '')
	, @NewLineChar + 'Creator: ' + trim(isnull(x.CreatorDisplayName, ''))
	, iif(x.CompanyID <> 0,  @NewLineChar + 'Company: ' + concat(isnull(com.Name, ''), ' (External ID: ', x.CompanyID, ')'), '')
	, iif(x.ContactID <> 0,  @NewLineChar + 'Contact: ' + concat(isnull(con.FirstName, ''), ' ', isnull(con.Surname, ''), ' (External ID: ', con.ContactID, ')'), '')
	, iif(x.VacancyID <> 0,  @NewLineChar + 'Vacancy: ' + concat(isnull(v.Title, ''), ' (External ID: ', x.VacancyID, ')'), '')
	, iif(x.CandidateID <> 0,  @NewLineChar + 'Candidate: ' + concat(isnull(can.FirstName, ''), ' ', isnull(can.Surname, ''), ' (External ID: ', can.CandidateID, ')'), '')
	, iif(len(trim(isnull(convert(varchar(50), x.DateSent, 111), ''))) > 0,  @NewLineChar + 'Sent Date: ' + trim(isnull(convert(varchar(50), x.DateSent, 111), '')), '')
	, iif(len(trim(isnull(convert(varchar(50), x.EmailDate, 111), ''))) > 0,  @NewLineChar + 'Email Date: ' + trim(isnull(convert(varchar(50), x.EmailDate, 111), '')), '')
	, @NewLineChar + 'Is Outgoing: ' + iif(x.OutGoing = 1, 'Yes', 'No')
	, @NewLineChar + 'Completed: ' + iif(x.Completed = 1, 'Yes', 'No')
	, iif(len(trim(isnull(x.EmailSubject, ''))) > 0, 'Email Subject: ' + trim(isnull(x.EmailSubject, '')), '')
	, iif(len(trim(isnull(cast(x.FromAddress as nvarchar(max)), ''))) > 0,  @NewLineChar + 'From Address: ' + trim(isnull(cast(x.FromAddress as nvarchar(max)), '')), '')
	, iif(len(trim(isnull(cast(x.ToAddress as nvarchar(max)), ''))) > 0,  @NewLineChar + 'To Address: ' + trim(isnull(cast(x.ToAddress as nvarchar(max)), '')), '')
	, iif(len(trim(isnull(cast(x.EmailBody as nvarchar(max)), ''))) > 0,  @NewLineChar + 'Email Body: ' + trim(isnull(cast(x.EmailBody as nvarchar(max)), '')), '')
))) as content
, 'comment' as category
--, 'candidate' as type
, 'contact' as type
--, 'company' as type
, -10 as user_account_id
from Correspondence x
left join CompanyDetails com on com.CompanyID = x.CompanyID
left join Contacts con on con.ContactID = x.ContactID
left join Candidates can on can.CandidateID = x.CandidateID
left join Vacancies v on v.VacancyID = x.VacancyID
where
x.CandidateID <> 0 or x.ContactID <> 0 or x.CompanyID <> 0 or x.VacancyID <> 0
order by x.CreationDate