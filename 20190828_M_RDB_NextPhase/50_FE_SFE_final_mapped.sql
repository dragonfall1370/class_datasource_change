--FE mapping
select distinct VCFE
from fe_sfe_skills
where VCFE is not NULL

--SFE mapping
select distinct VCFE
, VCSFE
, current_timestamp as insert_timestamp
from fe_sfe_skills
where VCFE is not NULL
order by VCFE, VCSFE

-->> Skills mapped <<--
with candidateinIDobject as (
        select o.ObjectID,o.LocationId,o.FileAs,o.FlagText,o.SourceId,o.CreatedOn
        from dbo.Objects o
        where o.ObjectTypeId=1 --candidate
)

, fesfe as (select --distinct am.Description as FEUnique --23 rows
	a.AttributeMasterId as FEID
	, am.Description as FE
	, am.ParentAttributeMasterId
	, am.AllowClient
	, am.AllowContact
	, am.AllowApplicant
	, a.AttributeId as SFEID
	, case when a.Notes is NULL or a.Notes = '' then a.Description
		else a.Notes end as SFE
	, a.Description --reference
	from Attributes a
	left join AttributeMaster am on am.AttributeMasterId = a.AttributeMasterId
	where am.Description not like '%DO NOT USE%'
	--and am.AllowApplicant = 'Y'
)
, mappedskills as (select o.*
	, fesfe.FE
	, fesfe.sfe
	, fe.attribute_master
	, fe.attributes
	, fe.skills
	from ObjectAttributes o
	left join fesfe on fesfe.SFEID = o.AttributeId
	inner join fe_sfe_skills fe on fe.attributes = fesfe.SFE
	where 1=1
	and o.ObjectID in (select ObjectId from candidateinIDobject)
	and fe.skills is not NULL --2332 not distinct
	)
/* REFERENCE CHECK
select * from mappedskills 
where 1=1
and ObjectID = 105687
and fe like '%.Pharma LIMS%'
*/

--MAIN SCRIPT
select concat('NP', ObjectID) as cand_ext_id
, string_agg(skills, ', ') as cand_skills
from (select distinct ObjectID, skills from mappedskills where skills is not NULL) a
group by ObjectID --1975 rows


-->> FE/SFE <<
with candidateinIDobject as (
        select o.ObjectID,o.LocationId,o.FileAs,o.FlagText,o.SourceId,o.CreatedOn
        from dbo.Objects o
        where o.ObjectTypeId=1 --candidate
)

, fesfe as (select --distinct am.Description as FEUnique --23 rows
	a.AttributeMasterId as FEID
	, am.Description as FE
	, am.ParentAttributeMasterId
	, am.AllowClient
	, am.AllowContact
	, am.AllowApplicant
	, a.AttributeId as SFEID
	, case when a.Notes is NULL or a.Notes = '' then a.Description
		else a.Notes end as SFE
	, a.Description --reference
	from Attributes a
	left join AttributeMaster am on am.AttributeMasterId = a.AttributeMasterId
	where am.Description not like '%DO NOT USE%'
	--and am.AllowApplicant = 'Y'
)
, mappedskills as (select o.ObjectID
	, fesfe.FE
	, fesfe.SFE
	, fe.attribute_master
	, fe.attributes
	, fe.VCFE
	, fe.VCSFE
	from ObjectAttributes o
	left join fesfe on fesfe.SFEID = o.AttributeId
	inner join fe_sfe_skills fe on fe.attributes = fesfe.SFE and fe.attribute_master = fesfe.FE
	where 1=1
	and o.ObjectID in (select ObjectId from candidateinIDobject)
	and fe.VCFE is not NULL
	)

select concat('NP', objectID) as cand_ext_id
, VCFE
, VCSFE
, current_timestamp as insert_timestamp
from (select distinct objectID, VCFE, VCSFE from mappedskills) a --38326 rows


-->> CANDIDATE EDUCATION <<
with candidateinIDobject as (
        select o.ObjectID,o.LocationId,o.FileAs,o.FlagText,o.SourceId,o.CreatedOn
        from dbo.Objects o
        where o.ObjectTypeId=1 --candidate
)

, fesfe as (select --distinct am.Description as FEUnique --23 rows
	a.AttributeMasterId as FEID
	, am.Description as FE
	, am.ParentAttributeMasterId
	, am.AllowClient
	, am.AllowContact
	, am.AllowApplicant
	, a.AttributeId as SFEID
	, case when a.Notes is NULL or a.Notes = '' then a.Description
		else a.Notes end as SFE
	, a.Description --reference
	from Attributes a
	left join AttributeMaster am on am.AttributeMasterId = a.AttributeMasterId
	where am.Description not like '%DO NOT USE%'
	--and am.AllowApplicant = 'Y'
)
, mappedskills as (select o.ObjectID
	, fesfe.FE
	, fesfe.SFE
	, fe.education
	from ObjectAttributes o
	left join fesfe on fesfe.SFEID = o.AttributeId
	inner join fe_sfe_skills fe on fe.attributes = fesfe.SFE and fe.attribute_master = fesfe.FE
	where 1=1
	and o.ObjectID in (select ObjectId from candidateinIDobject)
	and fe.education is not NULL
	)

select concat('NP', ObjectID) as cand_ext_id
, concat('Languages: ', nullif(string_agg(education, ', '),'')) as cand_edu
from (select distinct objectID, education from mappedskills) a --2512 rows
group by ObjectID --1466 rows


-->> AUDIT <<
select id, email, external_id, skills, education_summary
--into mike_candidate_edu_skills
from candidate
where education_summary is not NULL or skills is not NULL

