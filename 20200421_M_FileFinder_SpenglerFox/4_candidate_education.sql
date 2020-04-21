--Education
with edu as (select e.idperson
		, e.educationestablishment
		, e.educationsubject
		, e.idqualification
		, e.notes
		, e.educationfrom
		, e.educationto
		, e.checkedon
		, e.checkedby
		, e.createdon
		, concat_ws(chr(10)
					, coalesce('[Education from] ' || nullif(REPLACE(e.educationfrom, '\x0d\x0a', ' '), ''), NULL)
					, coalesce('[Education to] ' || nullif(REPLACE(e.educationto, '\x0d\x0a', ' '), ''), NULL)
					, coalesce('[College / University] ' || nullif(REPLACE(e.educationestablishment, '\x0d\x0a', ' '), ''), NULL)
					, coalesce('[Course] ' || nullif(REPLACE(e.educationsubject, '\x0d\x0a', ' '), ''), NULL) --Education subject
					, coalesce('[Qualification] ' || nullif(REPLACE(q.value, '\x0d\x0a', ' '), ''), NULL)
					, coalesce('[Checked by] ' || nullif(REPLACE(e.checkedby, '\x0d\x0a', ' '), ''), NULL)
					, coalesce('[Checked on] ' || nullif(REPLACE(e.checkedon, '\x0d\x0a', ' '), ''), NULL)
					, coalesce('[Notes]' || chr(10) || nullif(REPLACE(e.notes, '\x0d\x0a', ' '), ''), NULL)
					) as edu
		from education e
		left join qualification q on e.idqualification = q.idqualification)

, contact_edu as (select idperson
		, string_agg(edu, chr(10) || chr(13) order by educationto desc, educationfrom desc, createdon asc) as contact_edu
		from edu
		group by idperson)

--Language
, contact_language as (select pc.idperson
		, string_agg(replace(l.value, '\x0d\x0a', ''), ', ') as contact_language
		from personcode pc
		left join language l on l.idlanguage = pc.codeid
		where idtablemd = 'c69e91b3-9f35-4c73-ba46-2e17ad8ce6aa' --language
		group by pc.idperson)
		
--MAIN SCRIPT
, cte_candidate AS (SELECT p.idperson contact_id
	, ROW_NUMBER() OVER(PARTITION BY c.idperson ORDER BY px.createdon DESC) rn
	, concat_ws(chr(10)
		, coalesce('[Education Summary] ' || chr(10) || nullif(REPLACE(cedu.contact_edu, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[International] ' || nullif(REPLACE(px.internationalvalue_string, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Language] ' || nullif(REPLACE(cl.contact_language, '\x0d\x0a', ' '), ''), NULL)
	) as candidate_education
	FROM candidate c
	JOIN (select * from personx where isdeleted = '0') px ON c.idperson = px.idperson
	JOIN (select * from person where isdeleted = '0') p ON c.idperson = p.idperson
	LEFT JOIN contact_edu cedu on cedu.idperson = c.idperson
	LEFT JOIN contact_language cl on cl.idperson = c.idperson
	--where c.idperson = '00187105-3e37-4edd-ae25-e0ade4ef8bd4'
	)
	
select contact_id as cand_ext_id
, candidate_education
FROM cte_candidate
WHERE rn = 1
and nullif(candidate_education, '') is not NULL