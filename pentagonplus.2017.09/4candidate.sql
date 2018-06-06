
select --top 300
          c.CandidateId as 'candidate-externalid'
        , c.FirstName as 'candidate-firstName'
        , c.LastName as 'candidate-lastName'
        , c.Email as 'candidate-email'
        , c.Phone as 'candidate-phone'
        , c.Mobile as 'candidate-mobile'
        , c.HighestQualificationHeld as 'candidate-education'
        , case when c.Salutation = 'Mr.' then 'MR'
                when c.Salutation = 'Mrs.' then 'MRS'
                when c.Salutation = 'Ms.' then 'MS'
                end as 'candidate-title'
        --, c.CandidateOwnerId as 'candidate-owners'
        , u.email as 'candidate-owners'
        , case when c.Gender = 'Female' then 'FEMALE'
                when c.Gender = 'Male' then 'MALE'
                end as 'candidate-gender'
        --, convert(varchar(10),c.DateofBirth,120) as 'candidate-dob'
        , CONVERT(varchar(10), CONVERT(date, c.DateofBirth, 103), 120) as 'candidate-dob'
        , c.HomePersonalNumber as 'candidate-homePhone'
        , c.OfficeNumber as 'candidate-workPhone'
        , c.Contactaddress as 'candidate-address'        
        --, c.Nationality as 'candidate-citizenship'
	, case	when c.Nationality like '850602%' then ''
		when c.Nationality like '9012954%' then ''
		when c.Nationality like 'African%' then 'CF'
		when c.Nationality like 'America%' then 'US'
		when c.Nationality like 'Austral%' then 'AU'
		when c.Nationality like 'Belgium%' then 'BE'
		when c.Nationality like 'Birtish%' then 'GB'
		when c.Nationality like 'Canadia%' then 'CA'
		when c.Nationality like 'China%' then 'CN'
		when c.Nationality like 'Chinese%' then 'CN'
		when c.Nationality like 'Egytian%' then 'EG'
		when c.Nationality like 'Filipin%' then 'PH'
		when c.Nationality like 'French%' then 'FR'
		when c.Nationality like 'Germany%' then 'DE'
		when c.Nationality like 'Indian?%' then 'IN'
		when c.Nationality like 'Indian%' then 'IN'
		when c.Nationality like 'india%' then 'IN'
		when c.Nationality like 'India%' then 'IN'
		when c.Nationality like 'Indones%' then 'ID'
		when c.Nationality like 'Iranian%' then 'IR'
		when c.Nationality like 'iran%' then 'IR'
		when c.Nationality like 'Iran%' then 'IR'
		when c.Nationality like 'Italian%' then 'IT'
		when c.Nationality like 'Lyndia%' then ''
		when c.Nationality like 'Maaysia%' then 'MY'
		when c.Nationality like 'Malasyi%' then 'MY'
		when c.Nationality like 'Malasys%' then 'MY'
		when c.Nationality like 'Malayas%' then 'MY'
		when c.Nationality like 'Malayia%' then 'MY'
		when c.Nationality like 'Malayis%' then 'MY'
		when c.Nationality like 'Malaysa%' then 'MY'
		when c.Nationality like 'malaysi%' then 'MY'
		when c.Nationality like 'Malaysi%' then 'MY'
		when c.Nationality like 'MAlaysi%' then 'MY'
		when c.Nationality like 'MALAYSI%' then 'MY'
		when c.Nationality like 'Malay%' then 'MY'
		when c.Nationality like 'Maldivi%' then 'IN'
		when c.Nationality like 'Malysia%' then 'MY'
		when c.Nationality like 'Morocca%' then 'MA'
		when c.Nationality like 'Mslaysi%' then 'MY'
		when c.Nationality like 'Nigeria%' then 'NG'
		when c.Nationality like 'Pakista%' then 'PK'
		when c.Nationality like 'philipi%' then 'PH'
		when c.Nationality like 'Philipi%' then 'PH'
		when c.Nationality like 'Philipp%' then 'PH'
		when c.Nationality like 'Rusian%' then 'RU'
		when c.Nationality like 'Singapo%' then 'SG'
		when c.Nationality like 'Sri%' then 'LK'
		when c.Nationality like 'Sudanes%' then 'SD'
		when c.Nationality like 'Thailan%' then 'TH'
		when c.Nationality like 'Thai%' then 'TH'
		when c.Nationality like 'United States of America%' then 'US'
		when c.Nationality like 'Uzbekis%' then 'UZ'
		when c.Nationality like 'Vietnam%' then 'VN'
		when c.Nationality like 'Zealand%' then 'NZ'
		when c.Nationality like '%UNITED%ARAB%' then 'AE'
		when c.Nationality like '%UAE%' then 'AE'
		when c.Nationality like '%U.A.E%' then 'AE'
		when c.Nationality like '%UNITED%KINGDOM%' then 'GB'
		when c.Nationality like '%UNITED%STATES%' then 'US'
                else '' end as 'candidate-citizenship'
                        
/*
, SupplyChainLogisticsSkills_ifapplicable
, BankingInsurance
, ITSkills_ifapplicable
, Industrymaxselect2
, ITQualification_ifapplicable
, FinanceQualifications_ifapplicable
, EngineeringSkills_ifapplicable
, FinanceSkills_ifapplicable
, ITSystems_ifapplicable
, SalesMarketingSkills_ifapplicable
, HRSkills_ifapplicable
, CandidatesKeyExperienceSkills
*/
        , Stuff(  Coalesce('Supply Chain & Logistics Skills: ' + NULLIF(c.SupplyChainLogisticsSkills_ifapplicable, '') + char(10), '')
                + Coalesce('Banking & Insurance: ' + NULLIF(c.BankingInsurance, '') + char(10), '')
                + Coalesce('IT Skills: ' + NULLIF(c.ITSkills_ifapplicable, '') + char(10), '')
                + Coalesce('Industry: ' + NULLIF(c.Industry_maxselect2, '') + char(10), '')
                + Coalesce('IT Qualification: ' + NULLIF(c.ITQualification_ifapplicable, '') + char(10), '')
                + Coalesce('Finance Qualifications: ' + NULLIF(c.FinanceQualifications_ifapplicable, '') + char(10), '')
                + Coalesce('Engineering Skills: ' + NULLIF(c.EngineeringSkills_ifapplicable, '') + char(10), '')
                + Coalesce('Finance Skills: ' + NULLIF(c.FinanceSkills_ifapplicable, '') + char(10), '')
                + Coalesce('IT Systems: ' + NULLIF(c.ITSystems_ifapplicable, '') + char(10), '')
                + Coalesce('Sales & Marketing Skills: ' + NULLIF(c.SalesMarketingSkills_ifapplicable, '') + char(10), '')
                + Coalesce('HR Skills: ' + NULLIF(c.HRSkills_ifapplicable, '') + char(10), '')
                + Coalesce('Candidates''s Key Experience & Skills: ' + NULLIF(c.CandidatesKeyExperienceSkills, '') + char(10), '')
                , 1, 0, '') as 'candidate-skills'
                
/*
        , c.ExperienceinYears as 'candidate-workHistory'
        , c.CurrentSalary as 'candidate-workHistory'
        , c.PositionLevel as 'candidate-workHistory'
        , c.PresentSalary as 'candidate-workHistory'
        , c.FixedAllowanceBenefits as 'candidate-workHistory'
        , c.Totalworkexpmonth as 'candidate-workHistory'
        , c.CurrentWorkLocation as 'candidate-workHistory'
*/
        , Stuff(
                  Coalesce('Experience in Years: ' + NULLIF(c.ExperienceinYears, '') + char(10), '')
                + Coalesce('Current Salary: ' + NULLIF(c.CurrentSalary, '') + char(10), '')
                + Coalesce('Position Level: ' + NULLIF(c.PositionLevel, '') + char(10), '')
                --+ Coalesce('Present Salary: ' + NULLIF(c.PresentSalary, '') + char(10), '')
                + Coalesce('Fixed Allowance + Benefits: ' + NULLIF(c.FixedAllowanceBenefits, '') + char(10), '')
                + Coalesce('Total Work exp (month): ' + NULLIF(c.Totalworkexp_month, '') + char(10), '')
                + Coalesce('Current Work Location: ' + NULLIF(c.CurrentWorkLocation, '') + char(10), '')
                , 1, 0, '') as 'candidate-workHistory'
        , c.PresentSalary as 'candidate-currentsalary'
        
        , convert(varchar(10),c.EmploymentDate,120) as 'candidate-startDate1'
        , convert(varchar(10),c.PrevEmploymentDate1,120) as 'candidate-startDate2'
        , convert(varchar(10),c.PrevEmploymentDate2,120) as 'candidate-startDate3'
        --, c.CurrentJobTitle as 'candidate-jobTitle1'
        --, c.CurrentJobTitle_ as 'candidate-jobTitle1'
        , ltrim(Stuff(  Coalesce(' ' + NULLIF(c.CurrentJobTitle, ''), '') 
                + Coalesce(', ' + NULLIF(c.CurrentJobTitle2, ''), '') , 1, 1, '')) as 'candidate-jobTitle1'
        , c.PreviousJobTitle1 as 'candidate-jobTitle2'
        , c.PreviousJobTitle2 as 'candidate-jobTitle3'
        , c.CurrentEmployer as 'candidate-employer1'
        , c.PreviousEmployer1 as 'candidate-employer2'
        , c.Previousemployer2 as 'candidate-employer3'

        , Stuff(
                  Coalesce('Full Name: ' + NULLIF(c.FullName, '') + char(10), '')
                + Coalesce('Present Salary: ' + NULLIF(c.PresentSalary, '') + char(10), '')
                + Coalesce('Associated Tags: ' + NULLIF(c.AssociatedTags, '') + char(10), '')
                --+ Coalesce('Created By: ' + NULLIF(c.CreatedBy, '') + char(10), '')
                --+ Coalesce('Modified By: ' + NULLIF(c.ModifiedBy, '') + char(10), '')
                + Coalesce('Created By: ' + NULLIF(cast(concat(u1.firstname,' ',u1.lastname,' ',u1.email) as varchar(max)), '') + char(10), '')
                + Coalesce('Modified By: ' + NULLIF(cast(concat(u2.firstname,' ',u2.lastname,' ',u2.email) as varchar(max)), '') + char(10), '')
                + Coalesce('Created Time: ' + NULLIF(c.CreatedTime, '') + char(10), '')
                + Coalesce('Updated On: ' + NULLIF(c.UpdatedOn, '') + char(10), '')
                + Coalesce('Last Activity Time: ' + NULLIF(c.LastActivityTime, '') + char(10), '')
                + Coalesce('Last Mailed Time: ' + NULLIF(c.LastMailedTime, '') + char(10), '')
                + Coalesce('Source: ' + NULLIF(c.Source, '') + char(10), '')
                --+ Coalesce('Is Hot Candidate: ' + NULLIF(IsHotCandidate, '') + char(10), '')
                --+ Coalesce('Is Locked: ' + NULLIF(IsLocked, '') + char(10), '')
                --+ Coalesce('Is Unqualified: ' + NULLIF(IsUnqualified, '') + char(10), '')
                + Coalesce('Candidate Status: ' + NULLIF(c.CandidateStatus, '') + char(10), '')
                + Coalesce('Preferred Work Location: ' + NULLIF(c.PreferredWorkLocation, '') + char(10), '')
                + Coalesce('Discipline: ' + NULLIF(c.Discipline, '') + char(10), '')
                + Coalesce('Marital Status: ' + NULLIF(c.MaritalStatus, '') + char(10), '')
                + Coalesce('My First Impression: ' + NULLIF(c.MyFirstImpression, '') + char(10), '')
                + Coalesce('Candidates''s reason for Looking Out: ' + NULLIF(c.CandidatesreasonforLookingOut, '') + char(10), '')
                + Coalesce('Interviewer: ' + NULLIF(c.Interviewer, '') + char(10), '')
                + Coalesce('What Position Candidate is suitable: ' + NULLIF(c.WhatPositionCandidateissuitable, '') + char(10), '')
                + Coalesce('Referred by: ' + NULLIF(c.Referredby, '') + char(10), '')
                + Coalesce('IC/Passport Number: ' + NULLIF(c.ICPassportNumber, '') + char(10), '')
                + Coalesce('Candidate Summary for Client: ' + NULLIF(c.CandidateSummaryforClient, '') + char(10), '')
                + Coalesce('Preferred name: ' + NULLIF(c.Preferredname, '') + char(10), '')
                + Coalesce('Expected Salary: ' + NULLIF(c.ExpectedSalary, '') + char(10), '')
                + Coalesce('Notice Period: ' + NULLIF(c.NoticePeriod, '') + char(10), '')
                + Coalesce('Career Page Invite Status: ' + NULLIF(c.CareerPageInviteStatus, '') + char(10), '')
                , 1, 0, '') as 'candidate-note',
        doc.docs as 'candidate-resume'
--select count(*) --9319
from candidates c
left join users u on u.userid = c.CandidateOwnerId
left join (select userid, email, firstname, lastname from users) u1 on u1.userid = c.CreatedBy
left join (select userid, email, firstname, lastname from users) u2 on u2.userid = c.ModifiedBy
--left join (SELECT b.parentid, STUFF((SELECT DISTINCT ',' + replace(replace(a.FileName,',',''),'''','') from attachments a left join clients c on a.parentid = c.ClientId WHERE a.parentid = b.parentid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS docs FROM attachments AS b GROUP BY b.parentid) doc on doc.parentid = c.candidateID --14142
left join (SELECT b.parentid, STUFF((SELECT DISTINCT ',' + replace(replace(a.FileName,',',''),'''','') from attachments a left join candidates c on a.parentid = c.candidateID WHERE a.parentid = b.parentid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS docs FROM attachments AS b GROUP BY b.parentid) doc on doc.parentid = c.candidateID
--where c.candidateID in ('Zrecruit_139304000003852380','Zrecruit_139304000004248001','Zrecruit_139304000005721481','Zrecruit_139304000005727096','Zrecruit_139304000005744194','Zrecruit_139304000005758015','Zrecruit_139304000005766775','Zrecruit_139304000005773383','Zrecruit_139304000005778099','Zrecruit_139304000005785250','Zrecruit_139304000005793024')

/*
select u.*
from candidate c
left join users u on u.userid = c.CandidateOwnerId


select top 100
          t.ParentId as 'candidate_id'
        , cast('-10' as int) as 'userid'
        , cast('-10' as int) as 'user_account_id'
        , cast('4' as int) as 'contact_method'
        , cast('1' as int) as 'related_status'
        --, Coalesce(NULLIF(cast(t.CreatedTime as varchar(max)), '') + char(10), '') as 'feedback_timestamp_insert_timestamp'
        --, Coalesce(NULLIF(CONVERT(varchar(10), CONVERT(date, t.CreatedTime, 103), 120), '') + char(10), '') as 'feedback_timestamp_insert_timestamp'
        , CONVERT(DATETIME, t.CreatedTime, 103) as 'feedback_timestamp_insert_timestamp'
	, Stuff( 
	                  Coalesce('Title: ' + NULLIF(cast(t.NoteTitle as varchar(max)), '') + char(10), '')
                        + Coalesce('Content: ' + char(10) + NULLIF(cast(t.NoteContent as varchar(max)), '') + char(10) + char(10), '')
                        --+ Coalesce('Created By: ' + NULLIF(cast(t.CreatedBy as varchar(max)), '') + char(10), '')
                        + Coalesce('Created By: ' + NULLIF(cast(concat(u1.firstname,' ',u1.lastname,' ',u1.email) as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Modified By: ' + NULLIF(t.ModifiedBy, '') + char(10), '')
                        + Coalesce('Modified By: ' + NULLIF(cast(concat(u2.firstname,' ',u2.lastname,' ',u2.email) as varchar(max)), '') + char(10), '')
                        + Coalesce('Created Time: ' + NULLIF(t.CreatedTime, '') + char(10), '')
                        + Coalesce('Modified Time: ' + NULLIF(t.ModifiedTime, '') + char(10), '')
                , 1, 0, '') as 'comment_body'
--select count(*) --19647
from note t
left join candidate c on c.CandidateId = t.parentid
left join (select userid, email, firstname, lastname from users) u1 on u1.userid = t.CreatedBy
left join (select userid, email, firstname, lastname from users) u2 on u2.userid = t.ModifiedBy
where c.CandidateId is not null --and t.CreatedTime is null
--and t.ParentId = 'Zrecruit_139304000000185315'



------------
-- CALL COMMENT - INJECT TO VINCERE
select top 100
        t.RelatedTo as 'candidate_id'
        , cast('-10' as int) as 'userid'
        , cast('-10' as int) as 'user_account_id'
        , cast('4' as int) as 'contact_method'
        , cast('1' as int) as 'related_status'
        --, Coalesce(NULLIF(cast(t.CreatedTime as varchar(max)), '') + char(10), '') as 'feedback_timestamp_insert_timestamp'
        --, Coalesce(NULLIF(CONVERT(varchar(10), CONVERT(date, t.CreatedTime, 103), 120), '') + char(10), '') as 'feedback_timestamp_insert_timestamp'
        , CONVERT(DATETIME, t.CreatedTime, 103) as 'feedback_timestamp_insert_timestamp'
	, Stuff( 
	                  Coalesce('Subject: ' + NULLIF(cast(t.Subject as varchar(max)), '') + char(10), '')
                        + Coalesce('Call Type: ' + NULLIF(cast(t.CallType as varchar(max)), '') + char(10), '')
                        + Coalesce('Call Purpose: ' + NULLIF(cast(t.CallPurpose as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Contact Name: ' + NULLIF(cast(t.ContactName as varchar(max)), '') + char(10), '')
                        + Coalesce('Call Start Time: ' + NULLIF(cast(t.CallStartTime as varchar(max)), '') + char(10), '')
                        + Coalesce('Call Duration: ' + NULLIF(cast(t.CallDuration as varchar(max)), '') + char(10), '')
                        + Coalesce('Description: ' + NULLIF(cast(t.Description as varchar(max)), '') + char(10), '')
                        + Coalesce('Call Result: ' + NULLIF(cast(t.CallResult as varchar(max)), '') + char(10), '')
                        + Coalesce('Created By: ' + NULLIF(cast(concat(u1.firstname,' ',u1.lastname,' ',u1.email) as varchar(max)), '') + char(10), '')
                        + Coalesce('Modified By: ' + NULLIF(cast(concat(u2.firstname,' ',u2.lastname,' ',u2.email) as varchar(max)), '') + char(10), '')
                        + Coalesce('Created Time: ' + NULLIF(t.CreatedTime, '') + char(10), '')
                        + Coalesce('Modified Time: ' + NULLIF(t.ModifiedTime, '') + char(10), '')
                        + Coalesce('Status: ' + NULLIF(cast(t.Status as varchar(max)), '') + char(10), '')
                        + Coalesce('Reminder: ' + NULLIF(cast(t.Reminder as varchar(max)), '') + char(10), '')
                , 1, 0, '') as 'comment_body'
--select count(*) --13
from call t
left join candidate c on c.candidateID = t.RelatedTo
left join (select userid, email, firstname, lastname from users) u1 on u1.userid = t.CreatedBy
left join (select userid, email, firstname, lastname from users) u2 on u2.userid = t.ModifiedBy
where c.CandidateId is not null
--and t.ParentId = 'Zrecruit_139304000005265299'


*/
