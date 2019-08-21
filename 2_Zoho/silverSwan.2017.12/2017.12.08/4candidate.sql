with
attachment as ( SELECT ParentID, STUFF((SELECT ',' + filename from CandidatesAttachments WHERE ParentID = c.ParentID and filename is not NULL and filename <> '' FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS filename FROM CandidatesAttachments as c GROUP BY c.ParentID )
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
, c.SkypeID As 'candidate-skype'
, 'philippa@silverswanrecruitment.com' As 'candidate-owners' --, c.CandidateOwner As 'candidate-owners'
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

/*
----------
with comment as (
        select
                   j.ParentID                 , j.CreatedTime
                 , ltrim(Stuff(     Coalesce('Note Owner: ' + NULLIF(j.NoteOwner, '') + char(10), '')
                                + Coalesce('Note Title: ' + NULLIF(j.NoteTitle, '') + char(10), '')
                                + Coalesce('Note Content: ' + NULLIF(j.NoteContent, '') + char(10), '')
                                + Coalesce('Created By: ' + NULLIF(j.CreatedBy, '') + char(10), '')
                                + Coalesce('Created Time: ' + NULLIF(j.CreatedTime, '') + char(10), '')
                        , 1, 0, '') ) as 'comment'
        -- select * 
        from ClientsNotes J
UNION ALL
        select
                   j.ParentID                 , j.CreatedTime
                 , ltrim(Stuff(     Coalesce('Note Owner: ' + NULLIF(j.NoteOwner, '') + char(10), '')
                                + Coalesce('Note Title: ' + NULLIF(j.NoteTitle, '') + char(10), '')
                                + Coalesce('Note Content: ' + NULLIF(j.NoteContent, '') + char(10), '')
                                + Coalesce('Created By: ' + NULLIF(j.CreatedBy, '') + char(10), '')
                                + Coalesce('Created Time: ' + NULLIF(j.CreatedTime, '') + char(10), '')
                        , 1, 0, '') ) as 'comment'
        -- select * 
        from JobNotes J --left join JobOpenings jo on jo.JobOpeningId = j.ParentID
UNION ALL
        select
                   j.ParentID
                 , j.CreatedTime
                 , ltrim(Stuff(     Coalesce('Note Owner: ' + NULLIF(j.NoteOwner, '') + char(10), '')
                                + Coalesce('Note Title: ' + NULLIF(j.NoteTitle, '') + char(10), '')
                                + Coalesce('Note Content: ' + NULLIF(j.NoteContent, '') + char(10), '')
                                + Coalesce('Created By: ' + NULLIF(j.CreatedBy, '') + char(10), '')
                                + Coalesce('Created Time: ' + NULLIF(j.CreatedTime, '') + char(10), '')
                        , 1, 0, '') ) as 'comment'
        -- select * 
        from CandidatesNotes J
)
--select top 1000 * from comment

select 
                  c.CandidateId
                , CONVERT(datetime, replace(convert(varchar(50),comment.CreatedTime),'',''),120) as 'comment_timestamp|insert_timestamp'
                , cast('-10' as int) as 'user_account_id'
                , cast('4' as int) as 'contact_method'
                , cast('1' as int) as 'related_status'
                , comment.comment  as comment_body
                
from Candidates c
left join comment on comment.ParentID = c.CandidateId where comment.comment is not null

*/