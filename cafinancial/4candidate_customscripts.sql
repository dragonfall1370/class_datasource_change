
with
-- LANGUAGE
 l (id,name) as ( SELECT CVID
                 , STUFF((SELECT DISTINCT ', ' + Language from Languages WHERE CVID = a.CVID
                                FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS name 
                 FROM (select CVID from Languages ) AS a GROUP BY a.CVID )


-- WORK HISTORY
, w (id,name) as ( SELECT CVID
                 , STUFF((SELECT char(10)
                                   + Coalesce('Owner: ' + NULLIF(cast( concat(o.fullname,' - ',o.email) as varchar(max) ), '') + char(10), '') --<<< UserID
                                   --+ Coalesce('CompanyRowID: ' + NULLIF(cast(WHCompanyRowID as varchar(max)), '') + char(10), '') --<<<
                                   + Coalesce('Company: ' + NULLIF(cast(WHCompany as varchar(max)), '') + char(10), '')
                                   + Coalesce('Start Date: ' + NULLIF(cast(WHStartDate as varchar(max)), '') + char(10), '')
                                   + Coalesce('Position: ' + NULLIF(cast( WHPosition as varchar(max)), '') + char(10), '')
                                   + Coalesce('Duties: ' + NULLIF(cast( WHDuties_ as varchar(max)), '') + char(10), '')
                                   + Coalesce('Leaving Reason: ' + NULLIF( WHLeavingReason , '') + char(10), '')
                                   + Coalesce('Comment: ' + NULLIF(cast(WHComment as varchar(max)), '') + char(10), '')
                                   + char(10)
                                from WorkHistory w
                                left join owners o on o.id = w.UserID
                                WHERE CVID = a.CVID
                                FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS name
                 FROM (select CVID from WorkHistory ) AS a GROUP BY a.CVID )


-- QUALIFICATION
, q (id,name) as ( SELECT CVID
                 , STUFF((SELECT char(10)
                                   + Coalesce('Owner: ' + NULLIF( cast( concat(o.fullname,' - ',o.email) as varchar(max) ), '') + char(10), '') --<<<
                                   + Coalesce('Institute: ' + NULLIF(cast(Institute as varchar(max)), '') + char(10), '')
                                   + Coalesce('Qualification Description: ' + NULLIF(cast(QualificationDescription as varchar(max)), '') + char(10), '')
                                   + Coalesce('Enddate: ' + NULLIF(cast(Enddate as varchar(max)), '') + char(10), '')
                                   + Coalesce('Comments: ' + NULLIF(cast(Comments as varchar(max)), '') + char(10), '')
                                   + Coalesce('Institute Type: ' + NULLIF(cast(InstituteType as varchar(max)), '') + char(10), '')
                                   + Coalesce('Qualification Status: ' + NULLIF(cast(QualificationStatus as varchar(max)), ''), '')
                                   + char(10)
                                from Qualifications q
                                left join owners o on o.id = q.UserID
                                WHERE CVID = a.CVID
                                FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS name
                 FROM (select CVID from Qualifications ) AS a GROUP BY a.CVID )
--select top 100 * from q



---------------------------------------------------------------
select --top 20
         c.CVID as 'candidate-ExternalId'
       , c.FirstName as 'candidate-FirstName'
       , c.MiddleName as 'candidate-MiddleName'
       , c.Surname as 'candidate-Lastname'
       , c.SAID as 'SA ID' --<<
       , c.Race as 'Race' --<<
       , c.NoticePeriod as 'Notice period (days)' --<<
       , c.Category as 'Category' --<<
       , c.Source as 'Source' --<<
       , c.Status as 'Status'--<<
       , c.Newsletter as 'Newsletter' --<<
       , l.name AS 'candidate-languages' -->>
       , w.name as '#candidate-workhistory'
       , q.name as '#candidate-education'
--  select count(*) --21736 -- select distinct ConsultantName --Gender --Title --Nationality -- select top 10 *
from candidates c
left join l on l.id = c.CVID
left join w on w.id = c.CVID
left join q on q.id = c.CVID
where c.CVID = -2146257280












--- SA ID
select
       c.CVID as 'additional_id'
        , 'add_can_info' as additional_type
        , 1005 as form_id
        , 1017 as field_id
       , convert(varchar,c.SAID) as 'field_value'
-- select count(*) --5996 -- select distinct Newsletter
from candidates c;


--- Race
select
       c.CVID as 'additional_id'
        , 'add_can_info' as additional_type
        , 1005 as form_id
        , 1018 as field_id
       , convert(varchar,
              case c.race
              when 'Asian' then 1
              when 'Black' then 2
              when 'Coloured' then 3
              when 'Indian' then 4
              when 'Non White' then 5
              when 'Unknown' then 6
              when 'White' then 7
              end
              )
         as 'field_value'
-- select count(*) --5996 -- select distinct Newsletter
from candidates c;


-- CATEGORY
select
       c.CVID as 'additional_id'
        , 'add_can_info' as additional_type
        , 1005 as form_id
        , 1020 as field_id
       , convert(varchar,
              case c.category
                     when 'Acc-Bs' then 1
                     when 'Acc-Tb' then 2
                     when 'Admin, Office & Support' then 3
                     when 'Agriculture' then 4
                     when 'Audit' then 5
                     when 'BI' then 6
                     when 'Business & Management' then 7
                     when 'C & MA' then 8
                     when 'CA (other)' then 10
                     when 'CA' then 9
                     when 'CFA' then 11
                     when 'CIMA' then 12
                     when 'Company Secretary' then 13
                     when 'CRS' then 14
                     when 'Data Capture' then 15
                     when 'Distribution, Warehousing & Freight' then 16
                     when 'DRS' then 17
                     when 'Engineering' then 18
                     when 'Fin Serv' then 19
                     when 'Finance' then 20
                     when 'FM' then 21
                     when 'FMCG, Retail & Wholesale' then 22
                     when 'Gen' then 23
                     when 'Government & Local Government' then 24
                     when 'Graduate' then 25
                     when 'Hospitality & Restaurant' then 26
                     when 'HR' then 27
                     when 'IT' then 28
                     when 'Legal' then 29
                     when 'Logistics' then 30
                     when 'Manufacturing, Production & Trades' then 31
                     when 'Marketing' then 32
                     when 'Media' then 33
                     when 'Medical' then 34
                     when 'Mining' then 35
                     when 'Motor' then 36
                     when 'Non Fin' then 37
                     when 'Other' then 38
                     when 'Petrochemical' then 39
                     when 'Procurement & Supply Chain' then 40
                     when 'Property' then 41
                     when 'Risk Analyst/Mangement' then 42
                     when 'Social & Community' then 43
                     when 'Supply Chain & Logistics' then 44
                     when 'Tax' then 45
                     when 'Telecommunication' then 46
                     when 'Transport & Aviation' then 47
                     when 'Wage/Sal' then 48
              end
              )
         as 'field_value'
-- select count(*) --5996 -- select distinct Newsletter
from candidates c;


--STATUS
select
       c.CVID as 'additional_id'
        , 'add_can_info' as additional_type
        , 1005 as form_id
        , 1021 as field_id
       , convert(varchar,
              case c.status
                     when 'Available' then 1
                     when 'Black list' then 2
                     when 'Do Not Use' then 3
                     when 'ICT ADMINISTRATOR' then 4
                     when 'Immediate' then 5
                     when 'Placed CA' then 7
                     when 'Placed' then 6
                     when 'Potential' then 8
                     when 'Under Offer' then 9
                     when 'Working' then 10
              end
              )
         as 'field_value'
-- select count(*) --5996 -- select distinct Newsletter
from candidates c;

-- Newsletter
select
       c.CVID as 'additional_id'
        , 'add_can_info' as additional_type
        , 1005 as form_id
        , 1022 as field_id
       , convert(varchar,
              case c.Newsletter
                     when 'Yes' then 1
                     when 'No' then 2
              end
              )
         as 'field_value'
-- select count(*) --5996 -- select distinct Newsletter
from candidates c;



-- DesiredLocation
with t as (
       select --top 100
              c.CVID as 'additional_id'
               --, c.FirstName as 'candidate-FirstName', c.Surname as 'candidate-Lastname', d.DesiredLocation
               , convert(varchar,
                     case d.DesiredLocation
                            when 'All' then 1
                            when 'Bisho' then 2
                            when 'Bloemfontein' then 3
                            when 'Cape - CBD' then 4
                            when 'Cape - N Subs' then 5
                            when 'Cape - S Subs' then 6
                            when 'Centurion' then 7
                            when 'Durban' then 8
                            when 'East Africa' then 9
                            when 'East London' then 10
                            when 'English Africa' then 11
                            when 'Franschoek' then 12
                            when 'French Africa' then 13
                            when 'Gauteng' then 14
                            when 'George' then 15
                            when 'Hermanus' then 16
                            when 'Jhb - East' then 17
                            when 'Jhb - North' then 18
                            when 'Jhb - South' then 19
                            when 'Jhb - West' then 20
                            when 'Johannesburg' then 21
                            when 'Kimberley' then 22
                            when 'Knysna' then 23
                            when 'KZN' then 24
                            when 'Midrand' then 25
                            when 'Montague Gardens' then 26
                            when 'Mpumalanga' then 27
                            when 'Paarl' then 28
                            when 'Port Elizabeth' then 29
                            when 'Portuguese Africa' then 30
                            when 'Pretoria' then 31
                            when 'Randburg' then 32
                            when 'Sandton' then 33
                            when 'Somerset West' then 34
                            when 'Stellenbosch' then 35
                            when 'Strand' then 36
                            when 'Uitenhage' then 37
                            when 'Umtata' then 38
                            when 'Welkom' then 39
                            when 'Wellington' then 40
                            when 'West Africa' then 41
                            when 'West Coast' then 42
                     end
                     )
                as 'field_value'
       , DesiredLocation
       -- select count(*)
       from DesiredLocation d
       left join candidates c on c.CVID = d.CVID
       where d.DesiredLocation <> ''
       --where d.CVID = -2146356094
)

select
       additional_id
       --, additional_type, form_id, field_id
       , 'add_can_info' as additional_type
       , 1005 as form_id
       , 1023 as field_id
       --, [candidate-FirstName], [candidate-Lastname], [DesiredLocation]
       , STUFF((SELECT distinct ',' + field_value from t WHERE additional_id = a.additional_id FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') as field_value
from t as a
GROUP BY a.additional_id


/*
-- WORK HISTORY
SELECT 
  CVID as 'external_id'
, WHCompany as 'current_employer'
, WHPosition as 'job_Title'
, STUFF(         Coalesce('Start Date: ' + NULLIF(cast(WHStartDate as varchar(max)), ''), '')
                   + Coalesce(char(10) + 'End Date: ' + NULLIF(cast(WHEndDate as varchar(max)), ''), '')
                   + Coalesce(char(10) + 'Duties: ' + NULLIF(cast( WHDuties_ as varchar(max)), ''), '')
                   + Coalesce(char(10) + 'Leaving Reason: ' + NULLIF( WHLeavingReason , ''), '')
                   + Coalesce(char(10) + 'Comment: ' + NULLIF(cast(WHComment as varchar(max)), ''), '')
                , 1, 0, '') AS 'company'
-- select count(*)
from WorkHistory
where CVID in (-2146355829) ---2146257280,2147402519)*/

select
       CVID as 'external_id'
       , c.CurrentEmployer as 'candidate-Employer1'
       , c.CurrentPosition as 'candidate-JobTitle1'
       , c.CurrentEmployer as 'candidate-Company1'
from candidates c
where CVID in (2147478639, 2147479271 )  ---2146257280,2147402519)

UNION

SELECT 
         CVID as 'external_id'
       , WHCompany as 'current_employer'
       , WHPosition as 'job_Title'
       , convert(date, WHStartDate_) as 'start_date'
       , convert(date, WHEndDate_) as 'end-date'
       , STUFF( 'Duties: ' + NULLIF(cast( WHDuties_ as varchar(max)), ''), '')
                  + Coalesce(char(10) + 'Leaving Reason: ' + NULLIF( WHLeavingReason , ''), '')
                  + Coalesce(char(10) + 'Comment: ' + NULLIF(cast(WHComment as varchar(max)), ''), '')
                   , 1, 0, '') AS 'company'
       , row_number() over (partition by CVID order by WHStartDate_ desc) as rn_tmp
-- select count(*) --117483-- select top 10 *
from wh --where WHStartDate_ is null
where CVID in (2147478639, 2147479271 ) ---2146257280,2147402519)
order by  CVID desc, WHStartDate_ desc


 
 
 
 
 
with t as (
select cvid, WHStartDateOK, WHEndDateOK
       , ROW_NUMBER() OVER(PARTITION BY cvid, WHStartDateOK, WHEndDateOK  ORDER BY cvid asc ) AS rn 
from wh where WHStartDateOK <>  '' or  WHEndDateOK <> ''
)
select * from t where rn > 2
 
 
/* -- EDUCATION
 SELECT 
  CVID as 'external_id'
, Institute as 'Institution'
, QualificationDescription as 'Qualification'
, Type as 'Degree'
, STUFF(     
                   + Coalesce('End Date: ' + NULLIF(cast(EndDate as varchar(max)), ''), '')
                   + Coalesce(char(10) + 'Description: ' + NULLIF(cast(Comments as varchar(max)), '') + char(10), '')
                   + Coalesce(char(10) + 'Qualification Status: ' + NULLIF(cast(QualificationStatus as varchar(max)), ''), '')
                , 1, 0, '') AS 'Description'
-- select count(*)
from Qualifications
where CVID in (-2146257280) ---2146257280,2147402519)*/

 SELECT 
         CVID as 'external_id'
       , case
              When TypeOK = 'Doctorate (PhD)' then '1'
              When TypeOK = 'Master (1~2 year advanced degree)' then '2'
              When TypeOK = 'Post Grad Diploma' then '3'
              When TypeOK = 'Degree (3~4 year degree)' then '4'
              When TypeOK = 'Diploma (=2 year degree)' then '5'
              When TypeOK = 'ITE/Tech/Teaching Cert' then '6'
              When TypeOK = 'Professional Qualification' then '7'
              When TypeOK = 'N/O/A' then '8'
              When TypeOK = 'Incomplete Sec Education' then '9'
              When TypeOK = 'Primary' then '10'
              When TypeOK = 'No Formal Education' then '11'
              When TypeOK = 'High School Graduate' then '12'
         end as 'educationId' --'Education level' , TypeOK, Type
       , Institute as 'institutionName'
       , EndDateOK as 'graduationDate' --, EndDate
       , QualificationDescription as 'qualification'
       , STUFF( Coalesce('Description: ' + NULLIF(cast(Comments as varchar(max)), '') + char(10), '')
                  + Coalesce(char(10) + 'Qualification Status: ' + NULLIF(cast(QualificationStatus as varchar(max)), ''), '')
                     , 1, 0, '') AS 'description'
-- select count(*) -- select distinct TypeOK -- , type
from quas
where CVID in ('-2146059586') ---2146257280,2147402519)


-- EDUCATION SUMMARY
 with
 -- Associations
a (id,name) as (SELECT CVID
                 , STUFF(( SELECT Coalesce('Association: ' + NULLIF(cast(Association as varchar(max)), '') + char(10), '')
                                from Associations WHERE CVID = a.CVID
                                FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 0, '') AS name 
                 FROM (select CVID from Associations ) AS a GROUP BY a.CVID )

SELECT 
         c.CVID as 'candidate-ExternalId'
       , STUFF(Coalesce('' + NULLIF(ltrim( cast(a.name as varchar(max)) ), '') + char(10), '')
                 + Coalesce('Profile: ' + NULLIF(cast(c.CandProfile as varchar(max)), '') + char(10), '')
                 --+ Coalesce('Qualifications: ' + char(10) + NULLIF(cast(q.name as varchar(max)), '') + char(10), '')
              , 1, 0, '') AS 'candidate-education'
from candidates c
left join a on a.id = c.CVID
where a.name is not null and CVID in ('-2146059586')        

SELECT count(*) from candidates c where CandProfile <> '' and CandProfile is not null


-- LANGUAGE
-- select top 100 * from Languages where CVID = 2146355829
-- select distinct Language from Languages
with
l1 as (
select
       CVID as 'external_id'
       , case 
              when Language like'Afrikaans%' then 'af'
              when Language like 'Arabic%' then 'ar'
              when Language like 'Chinese%' then 'zh_HK'
              when Language like 'Dutch%' then 'nl'
              when Language like '%Engl%' then 'en'
              when Language like 'French%' then 'fr'
              when Language like 'German%' then 'de'
              when Language like 'Greek%' then 'el'
              when Language like 'Hebrew%' then 'iw'
              when Language like 'Hindi%' then 'hi'
              --when Language like 'IsiZulu%' then ''
              when Language like 'Italian%' then 'it'
              when Language like 'Japanese%' then 'ja'
              --when Language like 'Ndebele%' then ''
              --when Language like 'Pedi%' then ''
              when Language like 'Polish%' then 'pl'
              when Language like 'Portugese%' then 'pt'
              when Language like 'Russian%' then 'ru'
              --when Language like 'Sepedi%' then ''
              when Language like 'Sesotho%' then 'st'
              when Language like 'Setswana%' then 'tn'
              when Language like 'Siswati%' then 'ss'
              --when Language like 'Sotho%' then ''
              when Language like 'Spanish%' then 'es'
              --when Language like 'Swati%' then ''
              --when Language like 'Swazi%' then ''
              --when Language like 'Tshivenda%' then ''
              when Language like 'Tsonga%' then 'ts'
              --when Language like 'Tswana%' then ''
              --when Language like 'Venda%' then ''
              when Language like 'Xhosa%' then 'xh'
              when Language like 'Zulu%' then 'zu'
              end as 'language' , language as 'origin_language'
from Lang ) --uages )
select * from l1
select count(*) from l1 where language is null
select distinct origin_language from l1 where language is null
select * from l1 where language <> ''

, l2 (id,language) as ( 
       SELECT 
              external_id
              , STUFF((SELECT DISTINCT ', ' + language from l1 WHERE external_id = a.external_id and language <> '' FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS name
       FROM (select external_id from l1 ) AS a GROUP BY a.external_id )
select * from l2
--select count(*) from l2


>>>> UPDATE to tmp_note of Vincere candidate table
with 
 l (id,name) as ( SELECT CVID
                 , STUFF((SELECT DISTINCT ', ' + Language from Languages WHERE CVID = a.CVID
                                FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS name 
                 FROM (select CVID from Languages ) AS a GROUP BY a.CVID )
select --top 20
         c.CVID as 'externalId'
       , Coalesce( char(10) + 'Languages: ' + NULLIF(cast(l.name as varchar(max)), ''), '') as  'tmp_note' -->>
from candidates c
left join l on l.id = c.CVID