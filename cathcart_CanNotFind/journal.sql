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

with
-- EMAIL
  mail1 (ID,email) as (select ContactUniqueID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(email,',',HomeEmail),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),CHAR(9),' ') as mail from contacts where email like '%_@_%.__%' or HomeEmail like '%_@_%.__%' )
, mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
/*, mail5 (ID, email1, email2) as (
		select pe.ID, email as email1, we.email2 as email2 from mail4 pe
		left join (select ID, email as email2 from mail4 where rn = 2) we on we.ID = pe.ID
		--left join (SELECT ID, STUFF((SELECT DISTINCT ',' + email from mail4 WHERE rn > 2 and ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS email3 FROM mail4 AS a where rn > 2 GROUP BY a.ID ) oe on oe.ID = pe.ID
		where pe.rn = 1 ) */
, e1 as (select ID, email from mail4 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 as (select ID, email from mail4 where rn = 2)

, t as (
        select
         j.UniqueID
        --, j.CandidateUniqueID as 'externalId'
        --,j.VacancyUniqueID as 'externalId'
        --,j.MatchUniqueID
        --,j.ClientUniqueID as 'externalId'
        --,j.SiteUniqueID
        , j.ContactUniqueID as 'externalId', coalesce( nullif(e1.email,''), coalesce( nullif(ed.email,'') + '(duplicated_' + cast(ed.rn as varchar(10)) + ')','') ) as 'candidate-email' --, c.Email as 'contact-email#'
        --,concat(j.ContactUniqueID,'con') as 'externalId'
        --,j.ContactType
        --,j.ProjectUniqueID
        --,j.TenderUniqueID
        --,j.CandContactUniq as 'externalId'
        --,j.ThreadUniqueID
        --,j.OriginalUniqID
        , cast('-10' as int) as userid
        , CONVERT(DATETIME, j.CreationDate, 103) as 'comment_timestamp|insert_timestamp'
                  , cast('4' as int) as contact_method
                  , cast('1' as int) as related_status
        ,Stuff( 
           Coalesce('Amending User: ' + NULLIF(code1.description, '') + char(10), '') --j.AmendingUser am.fullname
         + Coalesce('Creation Date: ' + NULLIF(j.CreationDate, '') + char(10), '')
         + Coalesce('Amendment Date: ' + NULLIF(j.AmendmentDate, '') + char(10), '')
         + Coalesce('Diary Date: ' + NULLIF(j.DiaryDate, '') + char(10), '')
         + Coalesce('Diary Time: ' + NULLIF(j.DiaryTime, '') + char(10), '')
         + Coalesce('Diary User: ' + NULLIF(code2.description, '') + char(10), '') --j.DiaryUser am.fullname
         + Coalesce('Diary Entry Type: ' + NULLIF(code3.description, '') + char(10), '') --j.Diary Entry Type
         + Coalesce('Subject: ' + NULLIF(j.Subject, '') + char(10), '')
         + Coalesce('Priority: ' + NULLIF(j.Priority, '') + char(10), '')
         + Coalesce('Action Date: ' + NULLIF(j.ActionDate, '') + char(10), '')
         + Coalesce('Action Time: ' + NULLIF(j.ActionTime, '') + char(10), '')
         + Coalesce('Contact Type: ' + NULLIF(j.ContactType, '') + char(10), '')
         + Coalesce('Direction IO: ' + NULLIF(j.DirectionIO, '') + char(10), '')
         + Coalesce('Ref: ' + NULLIF(j.Ref, '') + char(10), '')
         + Coalesce('Call Type: ' + NULLIF(code4.description, '') + char(10), '') --j.CallType
         + Coalesce('Call Objective: ' + NULLIF(j.CallObjective, '') + char(10), '')
         + Coalesce('Call Result: ' + NULLIF(j.CallResult, '') + char(10), '')
         + Coalesce('Emailed YN: ' + NULLIF(j.EmailedYN, '') + char(10), '')
         + Coalesce('Call Recording: ' + NULLIF(j.CallRecording, '') + char(10), '')
         + Coalesce('Call Duration: ' + NULLIF(j.CallDuration, '') + char(10), '')
         + Coalesce('SMS Repliable YN: ' + NULLIF(j.SMSRepliableYN, '') + char(10), '')
         + Coalesce('Journal Body: ' + NULLIF(jc.Journal_Body_V2, '') + char(10), '')
         --+ Coalesce('Client Profile: ' + NULLIF(ec.Client_Profile, '') + char(10), '') --company
         --+ Coalesce('CV Text: ' + NULLIF(ed.CV_Text, '') + char(10), '') --candidate
         , 1, 0, '') as 'comment_content'
         , jc.Journal_Body_V2
-- select count (*) -- select top 100 * -- select distinct j.calltype -- select top 1000 j.UniqueID, j.CreationDate, concat(c.Forename,' ',c.Surname)
from JOURNALS j --76232
left join jnlent jc on jc.unique_id = j.UniqueID
--left join ENICTAB ec on ec.unique_id = j.UniqueID --company   select * from ENICTAB where Client_Profile <> ''
--left join ENIDTAB ed on ed.unique_id = j.UniqueID --candidate select * from ENIDTAB where CV_Text <> ''
left join (select distinct code, description from codetables where Tabname = 'Influence Users') code1 on code1.code = j.AmendingUser
left join (select distinct code, description from codetables where Tabname = 'Influence Users') code2 on code2.code = j.DiaryUser
left join (select Code, Description from CodeTables where TabName = 'Journal Entry Typ') code3 on code3.code = j.DiaryEntryType
left join (select Code, Description,TabName from CodeTables where TabName = 'Call Types') code4 on code4.code = j.calltype
--left join clients c on c.UniqueID = j.ClientUniqueID where c.UniqueID is not null and c.UniqueID = '1655' --38345
left join contacts c on c.ContactUniqueID = j.ContactUniqueID --where c.ContactUniqueID is not null --74095
--left join vacancies v on v.UniqueID = j.VacancyUniqueID where v.UniqueID is not null --10262
--left join candidates c on c.UniqueID = j.CandidateUniqueID 
left join e1 ON c.ContactUniqueID = e1.ID -- candidate-email
left join ed ON c.ContactUniqueID = ed.ID -- candidate-email-DUPLICATION
where c.ContactUniqueID is not null --and j.ClientUniqueID in ('638','474') --52461
--and ec.Client_Profile <> ''

)
--select count(*) from t where [comment_content] is not null --38345
--select top 100 * from t where Journal_Body_V2 is not null
--select count(*) from t where externalID in ('10019con','10102con','10534con','10544con','10593con','10054con','10060con','10098con','10100con','10061con','10104con','10195con','1031con','10394con','10553con','10103con','10326con','10393con','10417con','10561con','10569con','10622con','10675con','10841con','10846con','10880con','10632con','10967con','11150con','11213con','10663con','10728con','10731con','10736con','1080con','10863con','11037con','10954con','10968con','11059con','11154con','11063con','11114con','11124con','11223con','1128con','11175con','11350con','11412con','11436con','11624con','11232con','11254con','11257con','11301con','11414con','11307con','11534con','11549con','1164con','115con','11540con','11622con','1165con','11631con','11752con','11655con','11880con','12112con','11714con','11922con','11899con','11905con','11925con','12101con','11926con','1194con','11946con','12024con','12120con','12121con','12184con','12187con','12204con','12244con','12230con','12379con','12300con','12340con','12318con','12386con','12405con','12422con','12505con','12460con','12489con','1260con','1327con','1389con','1399con','1668con','1719con','1447con','1453con','1449con','1463con','1523con','1465con','1552con','1636con','1673con','1750con','1943con','1678con','1871con','1840con','1888con','1941con','1990con','1979con','2055con','2094con','1984con','2005con','2014con','2020con','2040con','2069con','2201con','2250con','2187con','2210con','2214con','2188con','2329con','2318con','2470con','2543con','2341con','2391con','2372con','2422con','243con','2463con','2471con','2480con','2509con','2514con','2557con','2567con','2644con','2658con','2670con','2671con','2707con','2765con','2970con','2709con','2769con','2806con','281con','3402con','2841con','3415con','347con','3472con','3482con','3511con','3495con','3555con','3553con','3557con','3646con','3577con','3594con','3607con','3651con','370con','373con','3752con','4176con','3771con','3993con','3779con','3995con','4039con','4261con','410con','4195con','4231con','4219con','4277con','4259con','4289con','4306con','4287con','4609con','4650con','4288con','4324con','4350con','4348con','4364con','4771con','458con','4755con','4777con','476con','4798con','481con','4839con','513con','4967con','5096con','510con','5104con','5150con','511con','512con','515con','5178con','5253con','5156con','5274con','5325con','5241con','5316con','5440con','5396con','5417con','5449con','5579con','5450con','5621con','5627con','551con','5629con','5586con','5594con','560con','5647con','5638con','5810con','5906con','5742con','5765con','5851con','5966con','5904con','5950con','5958con','5995con','5977con','5993con','6023con','6041con','6304con','6065con','6074con','6097con','6115con','6131con','6144con','6184con','6173con','6236con','6294con','6303con','6495con','6308con','6608con','637con','6502con','6628con','6643con','6668con','6675con','6847con','6724con','6729con','6794con','6799con','7417con','7593con','6837con','7016con','7055con','7067con','7100con','7237con','7116con','7160con','721con','7225con','7321con','7332con','7372con','7401con','7416con','7421con','7523con','7535con','7596con','7554con','7626con','7597con','7628con','7897con','7944con','7634con','7766con','7654con','7663con','7958con','7849con','7990con','8024con','8262con','8333con','8057con','8179con','8100con','8132con','8299con','8207con','8240con','8248con','8331con','8347con','8436con','8456con','8522con','8570con','8552con','8631con','8646con','865con','869con','8737con','8801con','8959con','8741con','8833con','8991con','8994con','9329con','8998con','9001con','906con','9008con','9219con','909con','918con','9226con','9293con','9340con','9344con','9405con','9356con','9444con','9412con','9421con','9413con','944con','9469con','9480con','9483con','9518con','9481con','9498con','9503con','9599con','9538con','9607con','9547con','9567con','9679con','9619con','9637con','9658con','9641con','9672con','9693con','9784con','9719con','9825con','9828con','9972con','9858con','9878con','9889con','9942con','9943con','9977con','9992con','9996con')
--select * from t where externalID in ('4588','4746')
select * from t where [candidate-email] = 'mme_dumrus@hotmail.com'

select --top 100 
        j.UniqueID
        , jc.Journal_Body_V2
        , ec.Client_Profile
-- select count(*) -- select *
from JOURNALS j --76232
left join jnlent jc on jc.unique_id = j.UniqueID
left join ENICTAB ec on ec.unique_id = j.UniqueID --company   select * from ENICTAB where Client_Profile <> ''
where ec.Client_Profile <> '' or j.UniqueID = '36416'

select top 100 
        j.UniqueID
        , jc.Journal_Body_V2
        , ec.Client_Profile
        , ed.CV_Text
-- select count(*)
from JOURNALS j --76232
left join jnlent jc on jc.unique_id = j.UniqueID
left join ENIDTAB ed on ed.unique_id = j.UniqueID --candidate select * from ENIDTAB where CV_Text <> ''
where j.UniqueID = '36416' and Journal_Body_V2 is not null


select distinct j.AmendingUser,am.fullname from JOURNALS j left join AccountManager am on am.[user] = j.AmendingUser where am.fullname is not null 
select distinct j.DiaryUser,am.fullname  from JOURNALS j left join AccountManager am on am.[user] = j.DiaryUser where am.fullname is not null 

select top 100 * from JOURNALS where UniqueID = '36416' like '54FIN is looking to hire%'
select top 100 * from jnlent where Journal_Body_V2 like '54FIN is looking to hire%'
select * from contacts where contactuniqueID = '36416'

select * from tmp_country where country like '%Syria%'
select * from codetables c where TabName = 'Country Codes' and c.code in ('BM','RS','SY')

select distinct c.code, c.description, tc.ABBREVIATION -- from tmp_country
from codetables c
left join tmp_country tc on tc.country = c.description
where TabName = 'Country Codes' --and c.codelen = 2 

select distinct AmendingUser from JOURNALS j
select * from codetables c where Tabname = 'Influence Users' c.code in ('NM')

