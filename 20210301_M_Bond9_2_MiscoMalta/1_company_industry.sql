with industry as (select sub.uniqueid
, sub."1 attribute alphanumeric" as sub_type
, case sub."11 rootkey codegroup  58"
		when 'A' then 'Skills'
		when 'B' then 'Industry'
		when 'C' then 'Categories'
		end as code_type
, sub."2 parent xref"
, main."1 attribute alphanumeric" as main_type
from f12 sub
left join f12 main on main.uniqueid = sub."2 parent xref"
where 1=1
--and main."2 parent xref" is NULL --combination with codegroup
and main."11 rootkey codegroup  58" = 'B' --Industry
--and main."11 rootkey codegroup  58" = 'A' --Skills
--and main."11 rootkey codegroup  58" = 'C' --Categories
--and main."11 rootkey codegroup  58" in ('A', 'B', 'C')
--and main."11 rootkey codegroup  58" not in ('A', 'B', 'C') --Other types
--and sub."2 parent xref" is not NULL --checking hierachy mapping
order by main."1 attribute alphanumeric", sub."1 attribute alphanumeric"
)

, company_industry as (select uniqueid
	, "99 industry xref"
	, a.industry
	, a.splitrn
	from f02, unnest(string_to_array("99 industry xref", '~')) with ordinality as a(industry, splitrn)
	where 1=1
	and "99 industry xref" is not NULL
	)

select pi.uniqueid as com_ext_id
, pi.industry
, i.sub_type as final_industry
from company_industry pi
left join industry i on i.uniqueid = pi.industry
where i.sub_type is not NULL