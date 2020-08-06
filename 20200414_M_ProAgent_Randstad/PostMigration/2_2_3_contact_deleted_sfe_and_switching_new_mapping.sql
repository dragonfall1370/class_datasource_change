--#STEP 1: Delete contact SFE records if SFE deleted
--Backup contact FE/SFE
select *
into mike_bkup_contact_functional_expertise_20200803
from contact_functional_expertise


--Check to-be-deleted records
select cfe.contact_id
, cfe.functional_expertise_id
, cfe.sub_functional_expertise_id
from contact_functional_expertise cfe
where cfe.functional_expertise_id in (3046, 3051, 3055)
and cfe.sub_functional_expertise_id in (698, 756, 757, 758, 764, 822, 823, 824, 825)


--Deleted SFEs
delete from contact_functional_expertise
where functional_expertise_id in (3046, 3051, 3055)
and sub_functional_expertise_id in (698, 756, 757, 758, 764, 822, 823, 824, 825) --7 rows



--#STEP 2: Delete contact FE/SFE records if mapping changed
with contact_fe_sfe_bkup as (select distinct contact_id
	from mike_tmp_contact_functional_expertise_20200705
	where 1=1
	and functional_expertise_id in (3004, 3001)
	and sub_functional_expertise_id in (330, 250, 549, 639, 435, 254, 553, 523, 524, 582, 516, 592, 420, 604, 425, 606, 529, 422, 426, 607, 608, 427, 603, 255, 256, 580, 528, 423, 579, 429, 584, 430, 525, 431, 438)
	) --select * from contact_fe_sfe_bkup --67 records


/*--Capture all contacts from 1st conversion
select distinct cfe.contact_id
, cfe.functional_expertise_id
, cfe.sub_functional_expertise_id
from contact_functional_expertise cfe
where cfe.functional_expertise_id in (3044, 3046, 3047, 3051, 3052, 3055, 3058, 3059) --8 FEs in conversion 1
and cfe.sub_functional_expertise_id in (657, 685, 686, 695, 755, 756, 757, 758, 759, 760, 763, 764, 766, 790, 799, 805, 807, 822, 823, 824, 825, 827, 855, 858, 861, 866, 869, 872, 906)
and cfe.contact_id in (select contact_id from contact_fe_sfe_bkup) --67 rows
order by cfe.contact_id -- unique contact IDs
*/

delete from contact_functional_expertise
where functional_expertise_id in (3044, 3046, 3047, 3051, 3052, 3055, 3058, 3059) --8 FEs in conversion 1
and sub_functional_expertise_id in (657, 685, 686, 695, 755, 756, 757, 758, 759, 760, 763, 764, 766, 790, 799, 805, 807, 822, 823, 824, 825, 827, 855, 858, 861, 866, 869, 872, 906)
and contact_id in (select contact_id from contact_fe_sfe_bkup) --63 rows


--#STEP 3: Insert new mapping from backup on 05/07/2020 - before original FE/SFE to 1st conversion mapping
---Check 2nd conversion before insert
select cfe.contact_id
, functional_expertise_id
, m.vc_fe_name
, sub_functional_expertise_id
, m.vc_sfe_name
, m.vcfeid
, m.vc_new_fe
, m.vcsfeid
, m.vc_new_sfe_split
from mike_tmp_contact_functional_expertise_20200705 cfe
left join mike_tmp_vc_2_vc_new_fe_sfe_v2 m on concat_ws('', m.vc_fe_id, m.vc_sfe_id) = concat_ws('', cfe.functional_expertise_id, cfe.sub_functional_expertise_id)
join (select id from contact where deleted_timestamp is NULL) c on c.id = cfe.contact_id --get only active contacts
where 1=1
and cfe.functional_expertise_id in (3004, 3001)
and cfe.sub_functional_expertise_id in (330, 250, 549, 639, 435, 254, 553, 523, 524, 582, 516, 592, 420, 604, 425, 606, 529, 422, 426, 607, 608, 427, 603, 255, 256, 580, 528, 423, 579, 429, 584, 430, 525, 431, 438)

/*
select max(id), count(id)
from contact_functional_expertise
*/

--Insert into contact_functional_expertise
insert into contact_functional_expertise (contact_id, functional_expertise_id, sub_functional_expertise_id, insert_timestamp)
select distinct cfe.contact_id
, m.vcfeid functional_expertise_id
, m.vcsfeid sub_functional_expertise_id
, current_timestamp as insert_timestamp
from mike_tmp_contact_functional_expertise_20200705 cfe
left join mike_tmp_vc_2_vc_new_fe_sfe_v2 m on concat_ws('', m.vc_fe_id, m.vc_sfe_id) = concat_ws('', cfe.functional_expertise_id, cfe.sub_functional_expertise_id)
join (select id from contact where deleted_timestamp is NULL) c on c.id = cfe.contact_id
where 1=1
and cfe.functional_expertise_id in (3004, 3001)
and cfe.sub_functional_expertise_id in (330, 250, 549, 639, 435, 254, 553, 523, 524, 582, 516, 592, 420, 604, 425, 606, 529, 422, 426, 607, 608, 427, 603, 255, 256, 580, 528, 423, 579, 429, 584, 430, 525, 431, 438)


---AUDIT CONTACT
--Case 1: 22408 | タカシ	柳生
select *
from mike_tmp_contact_functional_expertise_20200705
where contact_id = 22408


select *
from contact_functional_expertise
where contact_id = 22408


--Case 2: 20275 | (退任) フィリップ	オヴァロ
select *
from mike_tmp_contact_functional_expertise_20200705
where contact_id = 20275


select *
from contact_functional_expertise
where contact_id = 20275


--Case 3: 
select *
from mike_tmp_contact_functional_expertise_20200705
where functional_expertise_id = 3004
and sub_functional_expertise_id = 330

--20469 | 吉田
select *
from mike_tmp_contact_functional_expertise_20200705
where contact_id = 20469


select *
from contact_functional_expertise
where contact_id = 20469
