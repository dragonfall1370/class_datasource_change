/*
-- select top 100 * from JOURNALS
-- select top 100 
        UniqueID
        ,CandidateUniqueID
        ,VacancyUniqueID
        ,MatchUniqueID
        ,ClientUniqueID
        ,SiteUniqueID
        ,ContactUniqueID
        ,ContactType
        ,ProjectUniqueID
        ,TenderUniqueID
        ,CandContactUniq
        ,ThreadUniqueID
        ,OriginalUniqID
 from JOURNALS
select top 1000
          CreatingUser
        , CreationDate
        , AmendmentDate
        , DiaryDate
        , DiaryTime
        , DiaryUser
        , Subject
        , Priority
        , ActionDate
        , ActionTime
        , ContactType
        , DirectionIO
        , Ref
        , CallType
        , CallObjective
        , CallResult
        , EmailedYN
        , CallRecording
        , CallDuration
        , SMSRepliableYN
from JOURNALS
where ContactUniqueID <> 0
CandContactUniq
ContactType
*/


with t as (
        select
         j.UniqueID
        --,j.CandidateUniqueID as 'externalId'
        --,j.VacancyUniqueID as 'externalId'
        --,j.MatchUniqueID
        ,j.ClientUniqueID as 'externalId'
        --,j.SiteUniqueID
        --,j.ContactUniqueID as 'externalId'
        --,j.ContactType
        --,j.ProjectUniqueID
        --,j.TenderUniqueID
        --,j.CandContactUniq as 'externalId'
        --,j.ThreadUniqueID
        --,j.OriginalUniqID
        , cast('-10' as int) as userid
        , CONVERT(DATETIME, j.CreationDate, 103) as 'comment_timestamp|insert_timestamp'
        ,Stuff( 
           Coalesce('AmendingUser: ' + NULLIF(code1.description, '') + char(10), '') --j.AmendingUser am.fullname
         + Coalesce('CreationDate: ' + NULLIF(j.CreationDate, '') + char(10), '')
         + Coalesce('AmendmentDate: ' + NULLIF(j.AmendmentDate, '') + char(10), '')
         + Coalesce('DiaryDate: ' + NULLIF(j.DiaryDate, '') + char(10), '')
         + Coalesce('DiaryTime: ' + NULLIF(j.DiaryTime, '') + char(10), '')
         + Coalesce('DiaryUser: ' + NULLIF(code2.description, '') + char(10), '') --j.DiaryUser am.fullname
         + Coalesce('Subject: ' + NULLIF(j.Subject, '') + char(10), '')
         + Coalesce('Priority: ' + NULLIF(j.Priority, '') + char(10), '')
         + Coalesce('ActionDate: ' + NULLIF(j.ActionDate, '') + char(10), '')
         + Coalesce('ActionTime: ' + NULLIF(j.ActionTime, '') + char(10), '')
         + Coalesce('ContactType: ' + NULLIF(j.ContactType, '') + char(10), '')
         + Coalesce('DirectionIO: ' + NULLIF(j.DirectionIO, '') + char(10), '')
         + Coalesce('Ref: ' + NULLIF(j.Ref, '') + char(10), '')
         + Coalesce('CallType: ' + NULLIF(j.CallType, '') + char(10), '')
         + Coalesce('CallObjective: ' + NULLIF(j.CallObjective, '') + char(10), '')
         + Coalesce('CallResult: ' + NULLIF(j.CallResult, '') + char(10), '')
         + Coalesce('EmailedYN: ' + NULLIF(j.EmailedYN, '') + char(10), '')
         + Coalesce('CallRecording: ' + NULLIF(j.CallRecording, '') + char(10), '')
         + Coalesce('CallDuration: ' + NULLIF(j.CallDuration, '') + char(10), '')
         + Coalesce('SMSRepliableYN: ' + NULLIF(j.SMSRepliableYN, '') + char(10), '')
         , 1, 0, '') as 'comment_content'
         , jc.JournalBodyV2
-- select count (*) -- select * 
from JOURNALS j --66339
left join (select distinct code, description from codetables where Tabname = 'Influence Users') code1 on code1.code = j.AmendingUser
left join (select distinct code, description from codetables where Tabname = 'Influence Users') code2 on code2.code = j.DiaryUser
left join clients c on c.UniqueID = j.ClientUniqueID --where c.UniqueID is not null --38345
--left join contacts c on c.ContactUniqueID = j.ContactUniqueID where c.ContactUniqueID is not null --64512
--left join vacancies v on v.UniqueID = j.VacancyUniqueID where v.UniqueID is not null --10262
--left join candidates c on c.UniqueID = j.CandidateUniqueID where c.UniqueID is not null --43512
left join jnlent jc on jc.unique_id = j.UniqueID
where c.UniqueID is not null
)
--select * from t
--select count(*) from t --38345
select top 3000 * from t where JournalBodyV2 is not null



select distinct j.AmendingUser,am.fullname from JOURNALS j left join AccountManager am on am.[user] = j.AmendingUser where am.fullname is not null 
select distinct j.DiaryUser,am.fullname  from JOURNALS j left join AccountManager am on am.[user] = j.DiaryUser where am.fullname is not null 


select * from tmp_country where country like '%Syria%'
select * from codetables c where TabName = 'Country Codes' and c.code in ('BM','RS','SY')

select distinct c.code, c.description, tc.ABBREVIATION -- from tmp_country
from codetables c
left join tmp_country tc on tc.country = c.description
where TabName = 'Country Codes' --and c.codelen = 2 

select distinct AmendingUser from JOURNALS j
select * from codetables c where Tabname = 'Influence Users' c.code in ('NM')

