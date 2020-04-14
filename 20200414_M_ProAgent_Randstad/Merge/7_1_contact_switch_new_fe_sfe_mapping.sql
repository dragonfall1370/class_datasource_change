/* TEMP TABLE
CREATE TABLE mike_tmp_vc_2_vc_new_fe_sfe 
(
	vc_fe_id bigint, 
	vc_fe_name character varying (1000), 
	vc_sfe_id bigint, 
	vc_sfe_name character varying (1000), 
	VCFEID bigint, 
	vc_new_fe_en character varying (1000), 
	vc_new_fe_ja character varying (1000), 
	VCSFEID bigint, 
	vc_new_sfe_split character varying (1000)
)

--> BACKUP ALL TABLE --due to transfer all data
select *
into contact_functional_expertise_bkup_20200304
from contact_functional_expertise

select *
into position_description_functional_expertise_bkup_20200304
from position_description_functional_expertise

select *
into candidate_functional_expertise_bkup_20200304
from candidate_functional_expertise
*/
/* -->> AUDIT DATA <<--
select cfe.*
, m.*
from contact_functional_expertise cfe
left join mike_tmp_vc_2_vc_new_fe_sfe m on m.vc_fe_id = cfe.functional_expertise_id and m.vc_sfe_id = cfe.sub_functional_expertise_id

select contact_id, functional_expertise_id, count(*)
from contact_functional_expertise cfe
group by contact_id, functional_expertise_id
having count(*) > 1 --34 contacts

select *
from contact_functional_expertise
where contact_id = 25067

select contact_id, functional_expertise_id, sub_functional_expertise_id
, row_number() over(partition by contact_id order by functional_expertise_id, sub_functional_expertise_id) as rn
, row_number() over(partition by contact_id, functional_expertise_id order by functional_expertise_id, sub_functional_expertise_id) as fe_rn
from contact_functional_expertise cfe
*/


--CURRENT VC FE/SFE > NEW FE/SFE
-->CONTACT w/o SFE --to be double check with RANDSTAD
with contact_wo_sfe as (select id, contact_id, functional_expertise_id, sub_functional_expertise_id
	from contact_functional_expertise
	where sub_functional_expertise_id is NULL --678
	)

, merged_new as (select c.id, c.contact_id, c.functional_expertise_id, c.sub_functional_expertise_id
	, m.vcfeid
	--, m.vcsfeid
	, m. vc_new_fe_en
	--, m.vc_new_sfe_split
	from contact_wo_sfe c
	join (select distinct vc_fe_id, vcfeid, vc_new_fe_en from mike_tmp_vc_2_vc_new_fe_sfe) m on m.vc_fe_id = c.functional_expertise_id
	--4168 rows
	) --select * from merged_new

--MAIN SCRIPT
insert into contact_functional_expertise (contact_id, functional_expertise_id)
select contact_id
, vcfeid
from merged_new


--CONTACT w SFE
with contact_w_sfe as (select id, contact_id, functional_expertise_id, sub_functional_expertise_id
	from contact_functional_expertise
	where sub_functional_expertise_id is not NULL --806
	)
	
, merged_new as (select c.id, c.contact_id, c.functional_expertise_id, c.sub_functional_expertise_id
	, m.vcfeid
	, m.vc_new_fe_en
	, m.vcsfeid
	, m.vc_new_sfe_split
	from contact_w_sfe c
	join mike_tmp_vc_2_vc_new_fe_sfe m on m.vc_fe_id = c.functional_expertise_id and m.vc_sfe_id = c.sub_functional_expertise_id
	--808 rows
	) --select * from merged_new

--MAIN SCRIPT
insert into contact_functional_expertise (contact_id, functional_expertise_id, sub_functional_expertise_id)
select contact_id
, vcfeid
, vcsfeid
from merged_new


--->DELETE OLD FE/SFE
delete from contact_functional_expertise
where id <= 3701

select *
from contact_functional_expertise
where id <= 3701