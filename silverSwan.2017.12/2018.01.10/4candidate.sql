
with
attachment as ( SELECT ParentID, STUFF((SELECT ',' + filename from Attachments WHERE ParentID = c.ParentID and filename is not NULL and filename <> '' FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS filename FROM Attachments as c GROUP BY c.ParentID )
-- select * from attachment
-- select ParentID,filename from CandidatesAttachments

select
  c.CandidateId As 'candidate-externalId'
	, case when (ltrim(replace(C.firstName,'?','')) = '' or  C.firstName is null) then 'FirstName' else ltrim(replace(C.firstName,'?','')) end as 'contact-firstName'
	, case when (ltrim(replace(C.lastName,'?','')) = '' or  C.lastName is null) then concat('LastName-',C.CandidateId) else ltrim(replace(C.lastName,'?','')) end as 'contact-Lastname'
, c.Email As 'candidate-email'
, c.Phone As 'candidate-phone'
, c.Mobile As 'candidate-mobile'
, c.ExperienceinYears As 'candidate-workHistory'
, c.CurrentEmployer As 'candidate-employer1'
, c.CurrentJobTitle As 'candidate-jobTitle1'
, c.HighestQualificationHeld As 'candidate-education'
, c.SkypeAddress As 'candidate-skype'
, u.email As 'candidate-owners' -- , c.CandidateOwnerID As 'candidate-owners' --, 'philippa@silverswanrecruitment.com' As 'candidate-owners'
, c.secondTelephone As 'Candidate-HomePhone'
,CONVERT(date, CONVERT(datetime, replace(convert(varchar(50),c.DOB_Yachts),'',''),120), 103)  as 'candidate-dob'
--, c.Nationality_Yachts 
, case
		when c.Nationality_Yachts like 'African%' then 'ZA'
		when c.Nationality_Yachts like 'Austral%' then 'AU'
		when c.Nationality_Yachts like 'AUSTRAL%' then 'AU'
		when c.Nationality_Yachts like 'Brazili%' then 'BR'
		when c.Nationality_Yachts like 'BRAZIL%' then 'BR'
		when c.Nationality_Yachts like 'Britisg%' then 'GB'
		when c.Nationality_Yachts like 'British%' then 'GB'
		when c.Nationality_Yachts like 'Bulgari%' then 'BG'
		when c.Nationality_Yachts like 'BULGARI%' then 'BG'
		when c.Nationality_Yachts like 'Canadia%' then 'CA'
		when c.Nationality_Yachts like 'CHILE%' then 'CL'
		when c.Nationality_Yachts like 'Croatia%' then 'HR'
		when c.Nationality_Yachts like 'CROATIA%' then 'HR'
		when c.Nationality_Yachts like 'Czech%' then 'CZ'
		when c.Nationality_Yachts like 'Dutch%' then 'NL'
		when c.Nationality_Yachts like 'English%' then 'GB'
		when c.Nationality_Yachts like 'Filipin%' then 'PH'
		when c.Nationality_Yachts like 'Finnish%' then 'FI'
		when c.Nationality_Yachts like 'FRANCE%' then 'FR'
		when c.Nationality_Yachts like 'French%' then 'FR'
		when c.Nationality_Yachts like 'German%' then 'DE'
		when c.Nationality_Yachts like 'GERMANY%' then 'DE'
		when c.Nationality_Yachts like 'Greek%' then 'GR'
		when c.Nationality_Yachts like 'Hungari%' then 'HU'
		when c.Nationality_Yachts like 'Indones%' then 'ID'
		when c.Nationality_Yachts like 'Irish%' then 'IE'
		when c.Nationality_Yachts like 'Italian%' then 'IT'
		when c.Nationality_Yachts like 'ITALY%' then 'IT'
		when c.Nationality_Yachts like 'Latvian%' then 'LV'
		when c.Nationality_Yachts like 'LITHUAN%' then 'LT'
		when c.Nationality_Yachts like 'NEW%' then 'NZ'
		when c.Nationality_Yachts like 'Polish%' then 'PL'
		when c.Nationality_Yachts like 'Portugu%' then 'PT'
		when c.Nationality_Yachts like 'Romania%' then 'RO'
		when c.Nationality_Yachts like 'ROMANIA%' then 'RO'
		when c.Nationality_Yachts like 'Scottis%' then 'GB'
		when c.Nationality_Yachts like 'Sloveni%' then 'SI'
		when c.Nationality_Yachts like 'SLOVENI%' then 'SI'
		when c.Nationality_Yachts like 'SOUTH%' then 'ZA'
		when c.Nationality_Yachts like 'Spanish%' then 'ES'
		when c.Nationality_Yachts like 'Swedish%' then 'SE'
		when c.Nationality_Yachts like 'Swiss%' then 'CH'
		when c.Nationality_Yachts like 'Ukraine%' then 'UA'
		when c.Nationality_Yachts like 'UKRAINE%' then 'UA'
		when c.Nationality_Yachts like 'Zealand%' then 'NZ'
		when c.Nationality_Yachts like '%UNITED%ARAB%' then 'AE'
		when c.Nationality_Yachts like '%UAE%' then 'AE'
		when c.Nationality_Yachts like '%U.A.E%' then 'AE'
		when c.Nationality_Yachts like '%UNITED%KINGDOM%' then 'GB'
		when c.Nationality_Yachts like '%UNITED%STATES%' then 'US'
		when c.Nationality_Yachts like '%US%' then 'US'
                end As 'Citizenship'
        , ltrim(Stuff(    Coalesce('Full Name: ' + NULLIF(c.FullName, '') + char(10), '')
                        + Coalesce('Associated Tags: ' + NULLIF(c.AssociatedTags, '') + char(10), '')
                        + Coalesce('Created By: ' + NULLIF(c.CreatedBy, '') + char(10), '')
                        + Coalesce('Modified By: ' + NULLIF(c.ModifiedBy, '') + char(10), '')
                        + Coalesce('Created Time: ' + NULLIF(c.CreatedTime, '') + char(10), '')
                        + Coalesce('Updated On: ' + NULLIF(c.UpdatedOn, '') + char(10), '')
                        + Coalesce('Last Activity Time: ' + NULLIF(c.LastActivityTime, '') + char(10), '')
                        + Coalesce('Source: ' + NULLIF(c.Source, '') + char(10), '')
                        + Coalesce('Email Opt Out: ' + NULLIF(c.EmailOptOut, '') + char(10), '')
                        + Coalesce('Is Hot Candidate: ' + NULLIF(c.IsHotCandidate, '') + char(10), '')
                        + Coalesce('Is Locked: ' + NULLIF(c.IsLocked, '') + char(10), '')
                        + Coalesce('Is Locked: ' + NULLIF(c.IsLocked, '') + char(10), '')
                        + Coalesce('Is Attachment Present: ' + NULLIF(c.IsAttachmentPresent, '') + char(10), '')
                        + Coalesce('Candidate Status: ' + NULLIF(c.CandidateStatus, '') + char(10), '')
                        + Coalesce('Looking For: ' + NULLIF(c.LookingFor, '') + char(10), '')
                        + Coalesce('Role: ' + NULLIF(c.Role, '') + char(10), '')
                        + Coalesce('Source of Candidate: ' + NULLIF(c.SourceofCandidate, '') + char(10), '')
                        + Coalesce('Registered: ' + NULLIF(c.Registered, '') + char(10), '')
                        + Coalesce('Career Page Invite Status: ' + NULLIF(c.CareerPageInviteStatus, '') + char(10), '')
                        + Coalesce('Visa (Yachts): ' + NULLIF(c.Visa_Yachts, '') + char(10), '')
                        + Coalesce('Rating: ' + NULLIF(c.Rating, '') + char(10), '')
                        + Coalesce('French: ' + NULLIF(c.French, '') + char(10), '')
                , 1, 0, '') ) as 'candidate-note'
, a.filename as 'candidate-document'
from Candidates c
left join attachment a on a.ParentId = c.CandidateId
left join users u on u.userid = c.CandidateOwnerID



----
----------

with comment as (
        select
                   j.ParentID
                 , CONVERT(datetime, replace(convert(varchar(50),j.CreatedTime),'',''),120) as 'comment_timestamp|insert_timestamp'
                 , ltrim(Stuff(   Coalesce('Note Owner: ' + NULLIF(u1.email, '') + char(10), '')
                                + Coalesce('Note Title: ' + NULLIF(j.NoteTitle, '') + char(10), '')
                                + Coalesce('Note Content: ' + NULLIF(j.NoteContent, '') + char(10), '')
                                + Coalesce('Created By: ' + NULLIF(u2.email, '') + char(10), '')
                                + Coalesce('Created Time: ' + NULLIF(j.CreatedTime, '') + char(10), '')
                        , 1, 0, '') ) as 'comment'
        -- select top 100 * 
        from Notes J
        left join (select * from users) u1 on u1.userid = j.NoteOwnerId
        left join (select * from users) u2 on u2.userid = j.CreatedBy
        --left join Contacts c on c.ContactID = j.ParentID where c.ContactID is not null
UNION ALL
        select
                   j.EntityId
                 , CONVERT(datetime, replace(convert(varchar(50),j.CreatedTime),'',''),120) as 'comment_timestamp|insert_timestamp'
                 , ltrim(Stuff(   Coalesce('Created Time: ' + NULLIF(j.CreatedTime, '') + char(10), '')
                                + Coalesce('Modified Time: ' + NULLIF(j.ModifiedTime, '') + char(10), '')
                                + Coalesce('Created By: ' + NULLIF(j.CreatedBy, '') + char(10), '')
                                + Coalesce('Subject: ' + NULLIF(j.Subject, '') + char(10), '')
                        , 1, 0, '') ) as 'comment'
        -- select * 
        from Emails J
        ---left join Contacts c on c.ContactID = j.EntityId where c.ContactID is not null
UNION ALL
        select
                 --i.JobOpeningId
                   i.CandidateId
                 , CONVERT(datetime, replace(convert(varchar(50),i.CreatedTime),'',''),120) as 'comment_timestamp|insert_timestamp'
                 , ltrim(Stuff(   'INTERVIEW NOTES:' + char(10) +
                                + Coalesce('Company : ' + NULLIF(j1.ClientName, '') + char(10), '') --i.ClientId
                                + Coalesce('Consultant: ' + NULLIF(u1.email, '') + char(10), '') --i.InterviewOwnerId
                                + Coalesce('Type: ' + NULLIF(i.Type, '') + char(10), '')
                                + Coalesce('Job Name: ' + NULLIF(j2.PostingTitle, '') + char(10), '') --i.JobOpeningId
                                + Coalesce('Interview Subject: ' + NULLIF(i.InterviewName, '') + char(10), '')
                                + Coalesce('Interviewer: ' + NULLIF(u2.email, '') + char(10), '') --i.Interviewer
                                + Coalesce('Location: ' + NULLIF(i.Location, '') + char(10), '')
                                + Coalesce('From: ' + NULLIF(i.From_, '') + char(10), '')
                                + Coalesce('To: ' + NULLIF(i.To_, '') + char(10), '')
                                + Coalesce('Comments: ' + NULLIF(i.ScheduleComments, '') + char(10), '')
                                + Coalesce('Created Date Time: ' + NULLIF(i.CreatedTime, '') + char(10), '')
                                + Coalesce('Modified Date Time: ' + NULLIF(i.ModifiedTime, '') + char(10), '')
                                + Coalesce('Created By: ' + NULLIF(u3.email, '') + char(10), '') --i.CreatedBy
                                + Coalesce('Modified By: ' + NULLIF(u4.email, '') + char(10), '') --i.ModifiedBy
                        , 1, 0, '') ) as 'comment'
        -- select * 
        from Interviews i
        left join (select * from Users) u1 on u1.UserID = i.InterviewOwnerId
        left join (select * from Users) u2 on u2.UserID = i.InterviewName
        left join (select * from Users) u3 on u3.UserID = i.CreatedBy
        left join (select * from Users) u4 on u4.UserID = i.ModifiedBy
        left join (select ClientId,ClientName from Clients) j1 on j1.ClientId = i.ClientId --where j1.ClientId is not null
        left join (select JobOpeningId,PostingTitle from JobOpenings) j2 on j2.JobOpeningID = i.JobOpeningId --where j2.JobOpeningId is not null
        left join (select CandidateId,FullName from Candidates) j3 on j3.CandidateId = i.CandidateId where j3.CandidateId is not null
)
--select count(*) from comment where comment.comment is not null --8157
select
        c.CandidateId as 'externalId'
        , cast('-10' as int) as 'user_account_id'
        --, CONVERT(datetime, replace(convert(varchar(50),comment.CreatedTime),'',''),120) 'comment_timestamp|insert_timestamp'
        , [comment_timestamp|insert_timestamp]
        , comment.comment  as 'comment_body'
from Candidates c
left join comment on comment.ParentID = c.CandidateId 
where c.CandidateId is not null and comment.comment is not null

*/


with t0 (CandidateId,firstName,Role) as (
        SELECT    CandidateId,firstName
                , Split.a.value('.', 'VARCHAR(max)') AS String
        FROM ( SELECT     CandidateId, firstName
                        , CAST ('<M>' + REPLACE(Role,';','</M><M>') + '</M>' AS XML) AS Data 
               FROM Candidates ) AS A CROSS APPLY Data.nodes ('/M') AS Split(a)
        )
select distinct Role from t0


with t as (
        select 
                c.CandidateId as 'additional_id'
                --, case when (ltrim(replace(C.firstName,'?','')) = '' or  C.firstName is null) then 'FirstName' else ltrim(replace(C.firstName,'?','')) end as 'contact-firstName'
                --, case when (ltrim(replace(C.lastName,'?','')) = '' or  C.lastName is null) then concat('LastName-',C.CandidateId) else ltrim(replace(C.lastName,'?','')) end as 'contact-Lastname'
                        , 'add_cand_info' as additional_type
                        , convert(int,1006) as form_id
                        , convert(int,1018) as field_id
                , c.Role
                , replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
                replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
                replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(c.Role
        ,'Domestic Couple','')
        ,'House Manager','')
        ,'Private House - Single Gardener','18')
        ,'Yacht - Chief Stewardess/Purser','20')
        ,'Private House - Single HK/Cook','17')
        ,'Yacht - Beautician/Massuese','22')
        ,'Private Household - Chef','15')
        ,'Private House - Manager','13')
        ,'Yacht - Officer/Captain','25')
        ,'Private House - General','19')
        ,'Private House - Couple','16')
        ,'Private House - Butler','14')
        ,'Yacht - Stewardess','21')
        ,'Yacht - Cook/Stew','30')
        ,'Yacht - Engineer','27')
        ,'Yacht - Deckhand','23')
        ,'Yacht - Steward','29')
        ,'Chalet Manager','4')
        ,'Yacht - Couple','28')
        ,'Resort Manager','3')
        ,'Yacht - Other','26')
        ,'Office Based','11')
        ,'Yacht - Chef','24')
        ,'Housekeeper','9')
        ,'Maintenance','8')
        ,'Chauffeur','7')
        ,'Childcare','10')
        ,'Couple','6')
        ,'Other','12')
        ,'Cook','2')
        ,'Host','5')
        ,'Chef','1')
        ,';',',') as field_value
        -- select distinct Role
        from Candidates c
        where LookingFor <> '' )
--select distinct field_value from t
select count(*) from t where field_value <> ''


select 
        c.CandidateId as 'additional_id'
	, case when (ltrim(replace(C.firstName,'?','')) = '' or  C.firstName is null) then 'FirstName' else ltrim(replace(C.firstName,'?','')) end as 'contact-firstName'
	, case when (ltrim(replace(C.lastName,'?','')) = '' or  C.lastName is null) then concat('LastName-',C.CandidateId) else ltrim(replace(C.lastName,'?','')) end as 'contact-Lastname'
                , 'add_cand_info' as additional_type
                , convert(int,1006) as form_id
                , convert(int,1019) as field_id
        , c.LookingFor
        ,replace(replace(replace(replace(replace(replace(replace(replace(c.LookingFor,
        'Winter','1'
        ),'Summer','2'
        ),'Permanent','3'
        ),'Temporary','4'
        ),'Private Household','5'
        ),'Yacht','6'
        ),'Middle East','7'),';',',') as field_value
-- select distinct LookingFor
from Candidates c
where LookingFor <> ''
