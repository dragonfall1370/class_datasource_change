--Check category | skills | industry
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
--and main."2 parent xref" is NULL --combination with codegroup | reference only
--and main."11 rootkey codegroup  58" = 'B' --Industry
--and main."11 rootkey codegroup  58" = 'A' --Skills
--and main."11 rootkey codegroup  58" = 'C' --Categories
--and main."11 rootkey codegroup  58" in ('A', 'B', 'C')
--and main."11 rootkey codegroup  58" not in ('A', 'B', 'C') --Other types
and sub."2 parent xref" is not NULL --checking hierachy mapping
order by main."1 attribute alphanumeric", sub."1 attribute alphanumeric"
)


--CHECK PARENT / CHILD 
with skill_parent as (select *
from f12
where "2 parent xref" = '80810C0182828480'
)

select *
from f12
where "2 parent xref" in (select uniqueid from skill_parent)


--CATEGORY
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