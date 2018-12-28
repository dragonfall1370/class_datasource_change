-- CONTACTS
select --top 100
        Cl.clientID as 'contact-externalId'
	, case when (ltrim(replace(UC.firstName,'?','')) = '' or  UC.firstName is null) then 'Firstname' else ltrim(replace(UC.firstName,'?','')) end as 'candidate-firstName'
	, case when (ltrim(replace(UC.lastName,'?','')) = '' or  UC.lastName is null) then concat('Lastname-',Cl.clientID) else ltrim(replace(UC.lastName,'?','')) end as 'candidate-Lastname'
	, ltrim(rtrim(CL.status)) as 'status'
        --Coalesce('Lead Status: ' + NULLIF(cast(customText1 as varchar(max)), '') + char(10), '')
        , customText1 as 'LeadStatus'
        --Coalesce('Sold By: ' + NULLIF(customText11, '') + char(10), '') -- CUSTOM FIELD
        , customText11 as 'SoldBy'
        , customText3 as 'Industry'
from bullhorn1.BH_Client Cl
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
where isPrimaryOwner = 1 and --Cl.clientID in (25,44,47) and
customText3 is not null and customText3 <> ''
--and CL.status is not null and CL.status <> '' 
order by Cl.clientID DESC;


-->>> INJECTION
SELECT top 100
          'add_con_info' as additional_type
        , Cl.clientID as additional_id  
        , 1004 as form_id
        , 1015 as field_id_LeadStatus
        , 1016 as field_id_SoldBy
        --, ltrim(rtrim(CL.status)) as field_value_name
        , ltrim(rtrim(UC.customText1)) as field_value_name_LeadStatus
        , ltrim(rtrim(UC.customText11)) as field_value_name_SoldBy
-- select count(*) --10984 --select top 100 *
from bullhorn1.BH_Client Cl
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
where CL.isPrimaryOwner = 1 and Cl.clientID in (11151)
--CL.status is not null and CL.status <> '' 
and ( (UC.customText1 is not null and UC.customText1 <> '') or (UC.customText11 is not null and UC.customText11 <> '') )
order by Cl.clientID DESC;




--CANDIDATES: STATUS & REFERREDBY
select --top 1000
         candidateID
	,Coalesce(NULLIF(replace(FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
        ,Coalesce(NULLIF(replace(LastName,'?',''), ''), concat('Lastname-',candidateID)) as 'contact-lastName'         
        ,dateAdded
        ,status
        ,referredBy
        ,source
        ,cast(customFloat3 as int) as 'Graduation Year'
--select dateAdded
from bullhorn1.Candidate where firstname in ('Robert','Findlay','Arthur') and lastname = 'Ingram' --candidateID in (755),1090,14449,16393,17669,1821,2140,22268,22606)


SELECT top 3000
         'add_cand_info' as additional_type
        , CA.candidateID as additional_id  
	,Coalesce(NULLIF(replace(CA.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
        ,Coalesce(NULLIF(replace(CA.LastName,'?',''), ''), concat('Lastname-',candidateID)) as 'contact-lastName'          
        , cast(1001 as int) as form_id
        , cast(1020 as int) as field_id -- STATUS
        , ltrim(rtrim(CA.status)) as field_value_status
                , case when CA.status = 'Active' then '1'
                       when CA.status = 'Archive' then '2'
                       when CA.status = 'DNC' then '3'
                       when CA.status = 'New Lead' then '7'
                  else '' end as field_value_id_manual
        --, cast(1021 as int) as field_id -- REFERRED BY
        --, ltrim(rtrim(CA.referredBy)) as field_value_referredBy
        --,cast(customFloat3 as int) as field_value_Graduation_Year
-- select count(*) --58284
from bullhorn1.Candidate CA 
where CA.status in ('Active','Archive','DNC','New Lead') --CA.status is not null and CA.status <> '' and CA.candidateID = 684


-- Graduation_Year
with t as (SELECT
         'add_cand_info' as additional_type
        , CA.candidateID as additional_id  
        , cast(1001 as int) as form_id
        , cast(1018 as int) as field_id
        ,cast(customFloat3 as int) as field_value_Graduation_Year
from bullhorn1.Candidate CA 
--where CA.status is not null and CA.status <> ''
)

select top 1000 *
--select count(*) --11564 -- select customFloat3 from bullhorn1.Candidate where candidateID in (37062,192,689)
from t where field_value_Graduation_Year is not null and field_value_Graduation_Year <> '' and additional_id = 684


-- CANDIDATES: COURCE
-- #Education
select top 200
           C.candidateID
         --, C.customText1 as 'candidate-schoolName'
         --, C.customText15 as 'candidate-degreeName' >>> grade
         --, customText13 as 'Course'
         --, customText15 as 'Grade'
         --, CASE WHEN PatIndex('[0-9][0-9][0-9][0-9]', C.customText16) > 0 THEN cast( (C.customText16 + '-07-01 00:00:00') as datetime) ELSE null END as 'candidate-graduationDate'
         --, cast ('[{"educationId":"","schoolName":"C.customText1","schoolAddress":"","institutionName":"","institutionAddress":"","course":"C.customText13","startDate":"","startDateTmp":"","graduationDate":"2020-01-01","graduationDateTmp":":","training":"","degreeName":":","qualification":"","department":"","thesis":"","description":"","grade":"0","gpa":"0","honorific":"","hornors":"","major":"","minor":""}]' as text) as edu
         , concat('[{"educationId":"","schoolName":"',C.customText1,'","schoolAddress":"","institutionName":"","institutionAddress":"","course":"',C.customText13,'","startDate":"","startDateTmp":"","graduationDate":"',CASE WHEN PatIndex('[0-9][0-9][0-9][0-9]', C.customText16) > 0 THEN concat(C.customText16,'-07-01') ELSE ':' END,'","graduationDateTmp":"","training":"","degreeName":"","qualification":"',customText15,'","department":"","thesis":"","description":"","grade":"0","gpa":"0","honorific":"","hornors":"","major":"","minor":""}]') as edu
-- select count(*) --66663
from bullhorn1.Candidate C
where C.isPrimaryOwner = 1 and C.candidateID = 3099 and
(C.customText13 is not null and C.customText13 <> '') or 
(C.customText15 is not null and C.customText15 <> '') or 
(C.customText16 is not null and C.customText16 <> '')
--group by C.candidateID having count(C.candidateID) > 1


-- CANDIDATES: CustomTextBlock5 - Sectors of Interest
with
ind0 (candidateID,customTextBlock5) as (select candidateID,
        replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
        cast(customTextBlock5 as varchar(max)),'1',''),'2',''),'3',''),'4',''),'5',''),'6',''),'7',''),'8',''),'9',''),'0',''),'{',''),'}',''),'\',''),'=','') as customTextBlock5
        from bullhorn1.Candidate where customTextBlock5 is not null and cast(customTextBlock5 as varchar(max)) <> '' )
,ind (ID,ind) as (SELECT candidateID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT candidateID, CAST ('<M>' + REPLACE(cast(customTextBlock5 as varchar(max)),',','</M><M>') + '</M>' AS XML) AS Data FROM ind0) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
--select top 100 * from ind0
--select distinct ltrim(rtrim(ind)) from ind where ind <> ''

-- #INSERT TO INDUSTRY
select top 1000 *
       -- count(*)
         -- CA.candidateID
        --, ltrim(rtrim(ind.ind)) as 'industry'
from bullhorn1.Candidate CA
left join ind on ind.ID = CA.candidateID
where CA.isPrimaryOwner = 1 and ind.ind <> '' and CA.candidateID in (34871,34888,34917)

-- #INSERT TO KEYWORD
select --top 100
          CA.candidateID
        , Coalesce(NULLIF(replace(FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
        , Coalesce(NULLIF(replace(LastName,'?',''), ''), concat('Lastname-',candidateID)) as 'contact-lastName'
        , Stuff(
                Coalesce(NULLIF(email, ''), '')
                + Coalesce(',' + NULLIF(email2, ''), '')
                + Coalesce(',' + NULLIF(email3, ''), '')
                , 1, 0, '') as email
        , ltrim(rtrim(cast(CA.customTextBlock5 as varchar(max)))) as '#Sectors of Interest'
        , ltrim( replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
        cast(customTextBlock5 as varchar(max)),'1',''),'2',''),'3',''),'4',''),'5',''),'6',''),'7',''),'8',''),'9',''),'0',''),'{',''),'}',''),'\',''),'=',''),', ',char(10)) ) as 'Sectors of Interest'
from bullhorn1.Candidate CA
where CA.isPrimaryOwner = 1 and CA.candidateID in  (34871,34888,34917)
and CA.customTextBlock5 is not null and cast(CA.customTextBlock5 as varchar(max)) <> ''
