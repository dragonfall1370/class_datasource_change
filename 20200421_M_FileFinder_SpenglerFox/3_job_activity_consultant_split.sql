with consultant_split as (
--Consultant Split
		select idfee
		, idassignment
		, c.iduser
		, c.createdon::timestamp as createdon
		, concat_ws(chr(10)
				, coalesce('Created on: ' || nullif(REPLACE(c.createdon, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Consultant: ' || nullif(REPLACE(consultant, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Consultant fee note: ' || nullif(REPLACE(consultantfeenote, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Consultant fee rate: ' || nullif(REPLACE(consultantfeerate, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Currency: ' || nullif(REPLACE(cy.value, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Consultant fee percent: ' || nullif(REPLACE(consultantfeepercent, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Consultant fee description: ' || nullif(REPLACE(consultantfeedescription, '\x0d\x0a', ' '), ''), NULL)
			) as description
		, row_number() over(partition by c.idassignment order by createdon::timestamp, idfee, consultantfeenote = 'Originator' desc) rn
		, 1 rnk
		from consultantfee c
		join currency cy on cy.idcurrency = c.idcurrency
		where 1=1
		and idfee is not NULL --20453
		--and idfee is NULL --49083
		--order by idassignment
)

--Consultant Split Breakdown
, consultant_breakdown as (select idfee
		, idassignment
		, c.iduser
		, c.createdon::timestamp as createdon
		, concat_ws(chr(10)
				, coalesce('Created on: ' || nullif(REPLACE(c.createdon, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Consultant: ' || nullif(REPLACE(consultant, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Consultant fee note: ' || nullif(REPLACE(consultantfeenote, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Consultant fee rate: ' || nullif(REPLACE(consultantfeerate, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Currency: ' || nullif(REPLACE(cy.value, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Consultant fee percent: ' || nullif(REPLACE(consultantfeepercent, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Consultant fee description: ' || nullif(REPLACE(consultantfeedescription, '\x0d\x0a', ' '), ''), NULL)
			) as description
		, row_number() over(partition by c.idassignment order by createdon::timestamp, idfee, consultantfeenote = 'Originator' desc) rn
		, 2 rnk
		from consultantfee c
		join currency cy on cy.idcurrency = c.idcurrency
		where 1=1
		and idfee is not NULL --20453
		--and idfee is NULL --49083
		--order by idassignment
		) 

, consultant_split_all as	(select idassignment
	, rnk
	, '[Consultant split]' || chr(10) || string_agg(description, chr(10) || chr(13) order by rn) as description
	from consultant_split
	group by idassignment, rnk
	
	UNION ALL
	select idassignment
	, rnk
	, '[Consultant split breakdown]' || chr(10) || string_agg(description, chr(10) || chr(13) order by rn) as description
	from consultant_breakdown
	group by idassignment, rnk
	) --select * from consultant_split_all
	
, final_activity as (select idassignment job_ext_id
	, string_agg(description, chr(10) || chr(13) order by rnk) description
	from consultant_split_all
	group by idassignment)
	
select *
, cast('-10' as int) as user_account_id
, 'comment' as category
, 'job' as type
, current_timestamp as created_date
from final_activity