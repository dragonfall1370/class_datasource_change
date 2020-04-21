/* CANDIDATE SOURCE

select distinct value as candidate_source
, 1 source_type
, 0 payment_style
, current_timestamp insert_timestamp
, current_timestamp periodic_payment_start_date
, current_timestamp + interval '2 years' periodic_payment_end_date
, 1 included_job_count
, 1 show_job
from personorigin
order by value

*/


--Candidate source
with cte_candidate as (select c.idperson cand_ext_id
	, ROW_NUMBER() OVER(PARTITION BY c.idperson ORDER BY px.createdon DESC) rn
	, REPLACE(po.value, '\x0d\x0a', ' ') as candidate_source
	FROM candidate c
	JOIN (select * from personx where isdeleted = '0') px ON c.idperson = px.idperson
	JOIN (select * from person where isdeleted = '0') p ON c.idperson = p.idperson
	LEFT JOIN personorigin po ON px.idpersonorigin_string = po.idpersonorigin
	)

select cand_ext_id
, case when candidate_source = 'Linkedin' then 'LinkedIn'
		else candidate_source end as candidate_source
from cte_candidate
where rn = 1
and candidate_source is not NULL --100628 rows