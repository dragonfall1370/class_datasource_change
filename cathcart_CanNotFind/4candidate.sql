

with
  mail (ID,email,rn) as ( SELECT UniqueID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY email ORDER BY email desc) FROM candidates where email <> '' and email like '%@%')
, e1 as (select ID, email from mail where rn = 1)
, ed (ID,email,rn) as (select ID, email, rn from mail where rn > 1)
--select * from ed where ID in ('2456','995','2154')

select
          c.UniqueID As 'candidate-externalId'
        , coalesce(nullif(c.Forename,''),'No FirstName') As 'contact-firstName'
        , coalesce(nullif(c.Surname,''),'No LastName') As 'contact-lastName'
        , c.Postcode As 'candidate-zipCode'
        , case c.Title when 'M' then 'MR' when 'F' then 'MISS' else '' end as 'candidate-title'
        , case c.Sex when 'M' then 'MALE' when 'F' then 'FEMALE' else '' end As 'candidate-gender'
        , case when c.DateOfBirth not in ('21/11/16063') then replace(CONVERT(varchar(10), CONVERT(date, cast(c.DateOfBirth as varchar(max)), 103), 120),'1900-01-01','') end As 'candidate-dob'
        --, c.Nationality As 'candidate-citizenship'
        , case
                when c.Nationality = 'Russian Federation' then 'RU' 
                when c.Nationality in ('BM','SY') then '' 
                when c.Nationality in ('') then 'TH' --Set all to TH
                else n.ABBREVIATION
                end 'candidate-citizenship' 
        , coalesce( nullif(e1.email,''), coalesce( nullif(ed.email,'') + '_duplicated' + cast(ed.rn as varchar(10)),'') ) As 'candidate-email' --, c.Email 
        , c.Mobile As 'candidate-mobile'
        , c.Telephone As 'candidate-phone'
        , c.WorkTelephone As 'candidate-workPhone'
        , c.BasicSalary as 'candidate-currentSalary' --As 'Current Annual Salary'
        , am.email As 'candidate-owners' --MainConsultant
        , c.DigitalIDFilename As 'candidate-photo'
        , c.CurrentEmployer As 'candidate-employer1'
        --, c.JobDescription As 'candidate-company1' -- INJECTION
        
        , ltrim(Stuff( 
                  Coalesce(' ' + NULLIF(cast(ltrim(rtrim(c.AddressHseNo)) as varchar(max)), ''), '')
                --+ Coalesce(', ' + NULLIF(cast(replace(ltrim(rtrim(c.Address)),'   ','') as varchar(max)), ''), '')
                + Coalesce(', ' + NULLIF(cast(replace(replace(replace(c.Address,' ','<>'),'><',''),'<>',' ') as varchar(max)), ''), '')
                + Coalesce(', ' + NULLIF(cast(ltrim(rtrim(c.AddressLine1)) as varchar(max)), ''), '')
                + Coalesce(', ' + NULLIF(cast(ltrim(rtrim(c.AddressLine2)) as varchar(max)), ''), '')
                + Coalesce(', ' + NULLIF(cast(ltrim(rtrim(c.AddressLine3)) as varchar(max)), ''), '')
                + Coalesce(', ' + NULLIF(cast(ltrim(rtrim(c.AddressLine4)) as varchar(max)), ''), '')
                + Coalesce(', ' + NULLIF(cast(ltrim(rtrim(c.AddressLine5)) as varchar(max)), ''), '')
                + Coalesce(', ' + NULLIF(cast(ltrim(rtrim(c.AddressLine6)) as varchar(max)), ''), '')
                , 1, 1, '')) as 'candidate-address'
        --,AddressHseNo        ,Address        ,AddressLine1        ,AddressLine2        ,AddressLine3        ,AddressLine4        ,AddressLine5        ,AddressLine6
        , Stuff(
                  Coalesce('Creating User: ' + NULLIF(cast(am.fullname as varchar(max)), '') + char(10), '') --c.CreatingUser
                + Coalesce('Creation Date: ' + NULLIF(cast(c.CreationDate as varchar(max)), '') + char(10), '')
                + Coalesce('Other Benefit: ' + NULLIF(cast(c.OtherBenefit as varchar(max)), '') + char(10), '')
                + Coalesce('Current OTE: ' + NULLIF(cast(c.CurrentOTE as varchar(max)), '') + char(10), '') --c.SiteUnique
                + Coalesce('Notice: ' + NULLIF(cast(c.Notice as varchar(max)), '') + char(10), '')
                + Coalesce('Email Address2: ' + NULLIF(cast(c.EmailAddress2 as varchar(max)), '') + char(10), '')
                , 1, 0, '') as 'candidate-note'
        , Stuff( 
	                  --Coalesce(NULLIF(cast(c.DocumentDirectory as varchar(max)), '') + char(10), '')
                          Coalesce(' ' + NULLIF(cast(c.DocumentsNames001 as varchar(max)), ''), '')
                        + Coalesce(',' + NULLIF(cast(c.DocumentsNames002 as varchar(max)), ''), '')
                        + Coalesce(',' + NULLIF(cast(c.DocumentsNames003 as varchar(max)), ''), '')
                        + Coalesce(',' + NULLIF(cast(c.DocumentsNames004 as varchar(max)), ''), '')
                        + Coalesce(',' + NULLIF(cast(c.DocumentsNames005 as varchar(max)), ''), '')
                        + Coalesce(',' + NULLIF(cast(c.DocumentsNames006 as varchar(max)), ''), '')
                , 1, 1, '') as 'company-resume'
-- select top 100 c.DocumentDirectory -- select distinct c.Nationality -- select top 200 * -- select count(*)
from candidates c
left join e1 ON c.UniqueID = e1.ID -- candidate-email
left join ed ON c.UniqueID = ed.ID -- candidate-email-DUPLICATION
left join (
        select distinct c.code, c.description, tc.ABBREVIATION
        from codetables c
        left join tmp_country tc on tc.country = c.description
        where TabName = 'Country Codes') n on n.code = c.Nationality
left join AccountManager am on am.[user] = c.MainConsultant
-- left join (select code,description from codetables where TabName = 'Candidate Status') ct on ct.code = c.status -- CANNDIDATE STATUS
where c.Forename = 'Surachet'
where c.UniqueID in ('8223','8820')
order by c.UniqueID asc

--select distinct c.Title from candidates c

--select case when c.DateOfBirth not in ('21/11/16063') then replace(CONVERT(varchar(10), CONVERT(date, cast(c.DateOfBirth as varchar(max)), 103), 120),'1900-01-01','') end As 'candidate-dob' from candidates c where c.DateOfBirth <> '21/11/16063'


/*
select distinct ATTOBJECTTYPE
from AttributesLink a


with t as (
        select c.UniqueID,concat(c.Forename,' ',c.Surname), a.ATTDESCRIPTION
        from candidates c
        left join AttributesLink a on a.ATTRIBUTEUNIQ = c.UniqueID
        where a.ATTOBJECTTYPE = 'CAND' )
select distinct ATTDESCRIPTION from t
*/


with t as (
        select    c.UniqueID As 'externalId'
                , concat(c.Forename,' ',c.Surname) AS 'fullname'
                , c.CurrentEmployer As 'employer1'
                , c.JobDescription As 'jobtitle1'
        from candidates c
        )
select *
from t 
where jobtitle1 is not null and jobtitle1 <> '' --9198
and [candidate-externalId] = '6354'

