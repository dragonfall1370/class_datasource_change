with main_category as (select sub.uniqueid
	, sub."1 attribute alphanumeric" as sub_value
	, case sub."11 rootkey codegroup  58"
			when 'A' then 'Skills'
			when 'B' then 'Industry'
			when 'C' then 'Categories'
			end as code_type
	, sub."2 parent xref" as category
	, main."1 attribute alphanumeric" as main_category
	from f12 sub
	left join f12 main on main.uniqueid = sub."2 parent xref"
	where 1=1
	and main."11 rootkey codegroup  58" in ('A', 'B', 'C')
	order by main."1 attribute alphanumeric", sub."1 attribute alphanumeric"
	)

select sub.uniqueid
, sub."1 attribute alphanumeric" as sub_value
, sub."2 parent xref"
, main.sub_value as main_value
from f12 sub
left join main_category main on main.uniqueid = sub."2 parent xref"
where 1=1
and sub."2 parent xref" is not NULL --checking hierachy mapping
and main.main_category = 'Skills'
order by main.sub_value, sub."1 attribute alphanumeric"