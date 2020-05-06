--Update FE/SFE to new mapping
select insert_timestamp, count(*)
from candidate_functional_expertise
group by insert_timestamp --"33246"
having count(*) > 1000

select *
from mike_candidate_functional_expertise_bkup_20191007

select insert_timestamp, count(*)
from mike_candidate_functional_expertise_bkup_20191007
group by insert_timestamp --"33270"
having count(*) > 1000

--41102 rows
select *
into mike_candidate_functional_expertise_bkup_20191015
from candidate_functional_expertise

--39771 rows
delete
from candidate_functional_expertise
where insert_timestamp in ('2019-10-07 18:46:38.293', '2019-08-29 17:02:52.097')