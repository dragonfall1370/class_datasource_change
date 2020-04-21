WITH cte_contact AS (
	SELECT cp.idperson con_ext_id
	, px.createdon, concat_ws(',', nullif(mobileprivate,'')
	, nullif(px.mobilebusiness,'')) mobile
	, TRIM('' FROM (TRIM('''' FROM (translate(px.emailprivate, ':?!\/#$%^&*()<>{}[]', ''))))) personal_email
	, p.dateofbirth::timestamp as dob --#inject
	, p.createdon::timestamp as reg_date --#inject
	, case t.value
		when 'Miss' then 'Miss.'
		when 'Ms' then 'Ms.'
		when 'Dr' then 'Dr.'
		when 'Mr' then 'Mr.'
		when 'Mrs' then 'Mrs.'
		else NULL end gender_title --#inject
	, ROW_NUMBER() OVER(PARTITION BY cp.idperson ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole) rn
	FROM company_person cp
	JOIN (select * from personx where isdeleted = '0') px ON cp.idperson = px.idperson
	JOIN (select * from person where isdeleted = '0') p ON cp.idperson = p.idperson
	LEFT JOIN title t on t.idtitle = p.idtitle
	--where coalesce(nullif(mobileprivate,''), nullif(px.mobilebusiness,'')) is not NULL
	--where nullif(p.idtitle,'') is not NULL
	--where nullif(px.emailprivate,'') is not NULL
	where nullif(p.dateofbirth, '') is not NULL
)

SELECT *
from cte_contact
where 1=1
and rn = 1
and dob is not NULL