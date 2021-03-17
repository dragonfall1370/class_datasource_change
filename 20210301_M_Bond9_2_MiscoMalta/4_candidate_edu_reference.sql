--OPTION 1
with t1 as --count max order before execution
	(
	select 1 as ord
	union select 2
	union select 3
	union select 4
	union select 5
	union select 6
	union select 7
	union select 8
	union select 9
	union select 10
	union select 11
	union select 12
	union select 13
	union select 14
	union select 15
	union select 16
	union select 17
	union select 18
	union select 19
	union select 20
	)

select split_part("42 educ text alphanumeric", '~', ord)
, split_part("227 edudateto date", '~', ord)
from t1
inner join f01 on split_part("42 educ text alphanumeric", '~', ord) <> ''
where f01.uniqueid = '8081010184D98080'
order by ord

--OPTION 2 (more general)
with a as (select f01.uniqueid
	, "11 educ level codegroup  11"
	, c11.description as edu_lvl
	, "42 educ text alphanumeric"
	, edu_text
	, arn::int
	, "44 qual codegroup 129"
	, "45 grade alphanumeric"
	, "59 prof quals codegroup  37"
	, "64 educ dates date"
	, "222 graduate codegroup  23"
	, c23.description as grad_code
	, "223 studystat codegroup 130"
	, c130.description as study_stat
	, "224 gradyear date" as gradyear
	, "227 edudateto date"
	from f01
	left join (select * from codes where codegroup = '11') c11 on c11.code = f01."11 educ level codegroup  11"
	left join (select * from codes where codegroup = '130') c130 on c130.code = f01."223 studystat codegroup 130"
	left join (select * from codes where codegroup = '23') c23 on c23.code = f01."222 graduate codegroup  23"
		, unnest(string_to_array("42 educ text alphanumeric", '~')) with ordinality as a (edu_text, arn)
	--where f01.uniqueid = '8081010184D98080'
	)
	
select a.uniqueid
, a.edu_lvl
, a.edu_text
, a.arn
, a2.edu_qual
, c129.description as edu_qualification
, a3.edu_grade
, a4.edu_prod
, c37.description as edu_prod_qualif
, to_date(nullif(a5.edu_date, ''), 'DD/MM/YY') as edu_date
, a.grad_code
, a.study_stat
, a.gradyear
, to_date(nullif(a6.edu_to, ''), 'DD/MM/YY') as edu_to
--into education --temp table
from a
	left join (select uniqueid, arn, split_part("44 qual codegroup 129", '~', arn) as edu_qual from a) a2 on a.uniqueid = a2.uniqueid and a.arn = a2.arn
	left join (select uniqueid, arn, split_part("45 grade alphanumeric", '~', arn) as edu_grade from a) a3 on a.uniqueid = a3.uniqueid and a.arn = a3.arn
	left join (select uniqueid, arn, split_part("59 prof quals codegroup  37", '~', arn) as edu_prod from a) a4 on a.uniqueid = a4.uniqueid and a.arn = a4.arn
	left join (select uniqueid, arn, split_part("64 educ dates date", '~', arn) as edu_date from a) a5 on a.uniqueid = a5.uniqueid and a.arn = a5.arn
	left join (select uniqueid, arn, split_part("227 edudateto date", '~', arn) as edu_to from a) a6 on a.uniqueid = a6.uniqueid and a.arn = a6.arn
	left join (select * from codes where codegroup = '129') c129 on c129.code = a2.edu_qual
	left join (select * from codes where codegroup = '37') c37 on c37.code = a4.edu_prod
where 1=1
order by arn