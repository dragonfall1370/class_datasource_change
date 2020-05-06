--FE&SFE settings
select --distinct am.Description as FEUnique --23 rows
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
order by FE, SFE

--Attribute group by entities
with attribute_filter as (select --distinct am.Description as FEUnique --23 rows
					a.AttributeMasterId
					, am.Description as FE
					, am.ParentAttributeMasterId
					, am.AllowClient
					, am.AllowContact
					, am.AllowApplicant
					, a.AttributeId
					, case when a.Notes is NULL or a.Notes = '' then a.Description
								else a.Notes end as SFE
					, a.Description --reference
				from Attributes a
				left join AttributeMaster am on am.AttributeMasterId = a.AttributeMasterId
				where am.Description not like '%DO NOT USE%'
				--and am.AllowApplicant = 'Y'
				--order by FE, SFE
)

, attribute_grade as (select oa.ObjectID
				, FE
				, concat_ws(' - ', a.SFE, nullif(g.Description,'')) as attribute
				from ObjectAttributes oa
				left join attribute_filter a on a.AttributeId = oa.AttributeId
				left join Grades g on g.GradeId = oa.Grade
				--where oa.ObjectId = 8666
)

, attribute_group as (select ObjectID, FE
		, string_agg(nullif(attribute,''), ', ') within group (order by attribute) as attribute_group
		from attribute_grade
		group by ObjectID, FE
)

select ObjectID
, string_agg(concat_ws(': ', nullif(FE, ''), nullif(attribute_group, '')), char(10)) as attribute_all
from attribute_group
where FE is not NULL
group by ObjectID