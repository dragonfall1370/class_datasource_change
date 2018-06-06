/*
select  concat(c.candidate_firstName,' ',c.candidate_Lastname) as fullname
        ,convert(varchar(max),c.industry)
        , convert(varchar(max),c.functionalexpertise)
        , convert(varchar(max),c.subfunctionalexpertise)
from CandidatesImportAutomappingTemplate c where convert(varchar(max),c.subfunctionalexpertise) like '%,%' and candidate_externalid = 10094

select distinct convert(varchar(max),c.industry)
from CandidatesImportAutomappingTemplate c

-----
select distinct convert(varchar(max),c.functionalexpertise) 
from CandidatesImportAutomappingTemplate c

-----
select distinct convert(varchar(max),c.subfunctionalexpertise) 
from CandidatesImportAutomappingTemplate c

----- SFE
select count(*) from CandidatesImportAutomappingTemplate c where convert(varchar(max),c.functionalexpertise) = 'Design,Engineering'
select candidate_externalId, convert(varchar(max),c.subfunctionalexpertise) from CandidatesImportAutomappingTemplate c where convert(varchar(max),c.subfunctionalexpertise) <> ''
SELECT candidate_externalId, CAST ('<M>' + REPLACE(REPLACE(convert(varchar(500),c.subfunctionalexpertise),'H&S','H_S'),',','</M><M>') + '</M>' AS XML) AS Data FROM CandidatesImportAutomappingTemplate c where convert(varchar(500),c.subfunctionalexpertise) <> ''
with t (candidate_externalId,sfe) as (SELECT candidate_externalId, Split.a.value('.[1]', 'VARCHAR(max)') AS String FROM (SELECT candidate_externalId, CAST ('<M>' + REPLACE(REPLACE(convert(varchar(500),c.subfunctionalexpertise),'H&S','H_S'),',','</M><M>') + '</M>' AS XML) AS Data FROM CandidatesImportAutomappingTemplate c where convert(varchar(500),c.subfunctionalexpertise) <> '') AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
select candidate_externalId,sfe from t

with t (fe,sfe) as (
        SELECT    fe
                , Split.a.value('.', 'VARCHAR(max)') AS String
        FROM ( SELECT     convert(varchar(max),functionalexpertise) as fe
                        , CAST ('<M>' + REPLACE(REPLACE(convert(varchar(500),subfunctionalexpertise),'H&S','H_S'),',','</M><M>') + '</M>' AS XML) AS Data 
               FROM CandidatesImportAutomappingTemplate where convert(varchar(max),functionalexpertise) <> '' ) AS A CROSS APPLY Data.nodes ('/M') AS Split(a)
        )
, t0 (fe,sfe) as (select distinct fe,sfe from t)
, t1 as (
        select  --fe as fe_origin
                case fe
                        when 'Engineering' then 3097
                        when 'Design Engineer' then 2986
                        when 'Design' then 2987
                        when 'Design,Engineering' then 2987
                        when 'Executive' then 2988
                        when 'Operational' then 2989
                        when 'Opertional' then 2989
                        when 'Sales' then 2990
                        when 'Support' then 2991
                        end as fe
                , sfe
        from t
        UNION ALL
        select
                case fe
                        when 'Engineering' then 3097
                        when 'Design Engineer' then 2986
                        when 'Design' then 2987
                        when 'Design,Engineering' then 3097
                        when 'Executive' then 2988
                        when 'Operational' then 2989
                        when 'Opertional' then 2989
                        when 'Sales' then 2990
                        when 'Support' then 2991
                        end as fe
                , sfe
        from t
        )
, t2 as (select distinct fe,sfe from t1) -- where sfe <> ''
*/

with t (candidate_externalId,fe,sfe) as (
        SELECT    candidate_externalId,fe
                , Split.a.value('.', 'VARCHAR(max)') AS String
        FROM ( SELECT     candidate_externalId, convert(varchar(max),functionalexpertise) as fe
                        , CAST ('<M>' + REPLACE(REPLACE(convert(varchar(500),subfunctionalexpertise),'H&S','H_S'),',','</M><M>') + '</M>' AS XML) AS Data 
               FROM CandidatesImportAutomappingTemplate where convert(varchar(max),functionalexpertise) <> '' ) AS A CROSS APPLY Data.nodes ('/M') AS Split(a)
        )
-- select count(*) from t --12354
select candidate_externalId
        --,fe
                , case fe
                                when 'Engineering' then 3097
                                when 'Design Engineer' then 2986
                                when 'Design' then 2987
                                when 'Design,Engineering' then 2987
                                when 'Executive' then 2988
                                when 'Operational' then 2989
                                when 'Opertional' then 2989
                                when 'Sales' then 2990
                                when 'Support' then 2991
                                end as fe        
        ,sfe 
from t 
--where sfe is not null
where fe = 'Design,Engineering'

