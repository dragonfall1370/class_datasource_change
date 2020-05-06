/* Audit PRODUCTION
select *
from candidate_functional_expertise
*/


with candidateinIDobject as (
        select ObjectID
        from dbo.Objects
        where ObjectTypeId=1 --candidate

		UNION ALL
		select ObjectID
		from cand_2sector
)

, SFE as (select AttributeId, trim(Description) as attributes
		from Attributes
		where AttributeMasterId = 426 --.Pharma Position
)

, finalvc as (select distinct oa.ObjectID
		, oa.AttributeId
		, sfe.attributes
		, VCFE
		, VCSFE
		from ObjectAttributes oa
		inner join candidateinIDobject c on c.ObjectID = oa.ObjectID
		inner join SFE on sfe.AttributeId = oa.AttributeId
		left join FE_SFE fs on fs.Attributes = sfe.attributes)

/* --Audit
select cand_ext_id, count(*)
from finalvc
group by cand_ext_id
having count(*) > 1 */

select distinct concat('NP', ObjectID) as cand_ext_id 
, VCFE
, VCSFE
, current_timestamp as insert_timestamp
from finalvc
where VCFE is not NULL
--and cand_ext_id = 'NP101551'
--and ObjectID in (select ObjectId from cand_2sector)