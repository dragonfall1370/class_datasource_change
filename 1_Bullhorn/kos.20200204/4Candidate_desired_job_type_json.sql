
-- 1 Permanent, 2 Contract, 3 Temp-To-Perm, 4 Temporary --[{"desiredJobTypeId":"1"},{"desiredJobTypeId":"2"},[{"desiredJobTypeId":"3"}],{"desiredJobTypeId":"4"}]
--SELECT candidateID, employmentPreference FROM bullhorn1.Candidate WHERE candidateID in (18550)

with
employmentPreference1 as (
       SELECT candidateID
       , userID
	, coalesce(nullif(replace(FirstName,'?',''), ''), 'No Firstname') as 'firstName'
       , coalesce(nullif(replace(LastName,'?',''), ''), concat('Lastname-',userID)) as 'lastName'
       , Split.a.value('.', 'VARCHAR(2000)') AS employmentPreference 
       FROM ( SELECT candidateID, userID, FirstName, LastName, CAST('<M>' + REPLACE(cast( ltrim(rtrim( convert(nvarchar(max),employmentPreference) )) as nvarchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM bullhorn1.Candidate where isdeleted <> 1 and status <> 'Archive') t 
       CROSS APPLY x.nodes('/M') AS Split(a)
       --WHERE candidateID in (18550)
)

, employmentPreference2 as (
       select distinct candidateID, userID, firstName, lastname
              --, employmentPreference
              , case
                     when employmentPreference like '%Permanent%' then '{"desiredJobTypeId":"1"}'
                     when employmentPreference like '%Contract%' then '{"desiredJobTypeId":"2"}'
                     when employmentPreference like '%Temporary%' then '{"desiredJobTypeId":"4"}'
                     when employmentPreference = '' then '{"desiredJobTypeId":"1"}'
                     when employmentPreference is null then '{"desiredJobTypeId":"1"}'
              end as 'desired_job_type_json'      
       from employmentPreference1
)
--select distinct desired_job_type_json from employmentPreference2 

, employmentPreference3 as (
       SELECT candidateID, coalesce('[' + nullif( STRING_AGG( desired_job_type_json,',' ) WITHIN GROUP (ORDER BY desired_job_type_json) + ']', ''), '') name
       from employmentPreference2 
       WHERE desired_job_type_json <> '' GROUP BY candidateID
)
select * from employmentPreference3