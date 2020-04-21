--CHECK AND CORRECT JOBS > CONTACTS

--CASE #1 | Check special case
select id
from company
where external_id = 'CPY000714' --company_id=40702

select *
from contact
where 1=1
--and company_id = 40702 --contact_id=65314
and company_id = 14657 --merge with com_ext_id = 'CPY000714'

--Check special case for job > default contact = 'DEFCPY000714' | update to correct and merged contact in VC
select id, external_id, company_id, contact_id, company_id_bkup, contact_id_bkup
from position_description
where company_id = 14657

select id, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where id = 65314

select id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where external_id = 'REC-276223' --id=62813

select *
from mike_tmp_contact_dup_check2
where contact_id = 62813 --merge with contact_id=26586

select id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where id in (26586, 62813, 65314)

select id, external_id, company_id, company_id_bkup, contact_id, contact_id_bkup
from position_description
where company_id = 14657
and contact_id = 65314 --140 jobs

update position_description
set contact_id = 26586 --switch to VC merged contact 26586 via contact_id=62813
where company_id = 14657
and contact_id = 65314 --wrong contact from PA instead of correct contact_id=62813


-->>CASE #2 | 
select id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where external_id = 'REC-207239' --id=38204

select *
from mike_tmp_contact_dup_check2
where contact_id = 38204 --merge with contact_id=26586

select *
from contact
where id = 65313

select id, external_id, company_id, contact_id, company_id_bkup, contact_id_bkup
from position_description
where external_id in ('JOB031396','JOB031394','JOB031407','JOB031412','JOB031403','JOB031441','JOB031411','JOB031422','JOB031408','JOB031430','JOB031443','JOB031419','JOB031417','JOB031402','JOB032177','JOB031389','JOB032016','JOB036020','JOB031376','JOB031400','JOB031391','JOB031405','JOB031398','JOB005144','JOB039797','JOB040581','JOB031446','JOB031444','JOB031436')

update position_description
set contact_id = 38204
where external_id in ('JOB031396','JOB031394','JOB031407','JOB031412','JOB031403','JOB031441','JOB031411','JOB031422','JOB031408','JOB031430','JOB031443','JOB031419','JOB031417','JOB031402','JOB032177','JOB031389','JOB032016','JOB036020','JOB031376','JOB031400','JOB031391','JOB031405','JOB031398','JOB005144','JOB039797','JOB040581','JOB031446','JOB031444','JOB031436')


-->>CASE #3
--Check special case for job > default contact = 'DEFCPY000714' | update to correct and merged contact in VC
select id, external_id, company_id, contact_id, company_id_bkup, contact_id_bkup
from position_description
where company_id = 14657
and company_id_bkup = 40702 --140 rows


select id, external_id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where external_id = 'REC-155683' --id=37729

select *
from mike_tmp_contact_dup_check2
where contact_id = 37729 --merge with contact_id=18849

select id, external_id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where id in (18849, 37729)

select id, external_id, company_id, contact_id, company_id_bkup, contact_id_bkup
from position_description
where external_id in ('JOB029677','JOB029548','JOB033665','JOB035483','JOB035485','JOB035487','JOB029678','JOB033667','JOB033666','JOB033669','JOB033672','JOB029676','JOB033668','JOB037725','JOB037722','JOB035488','JOB038058','JOB038059','JOB038056','JOB039017','JOB039013','JOB037724','JOB029675','JOB037723','JOB039016','JOB039014','JOB038057','JOB035484','JOB043276','JOB043278','JOB043280','JOB043284','JOB043286','JOB043287','JOB043288','JOB043290','JOB043291','JOB043292','JOB043293')

--Update 39 rows
update position_description
set contact_id = 18849
where external_id in ('JOB029677','JOB029548','JOB033665','JOB035483','JOB035485','JOB035487','JOB029678','JOB033667','JOB033666','JOB033669','JOB033672','JOB029676','JOB033668','JOB037725','JOB037722','JOB035488','JOB038058','JOB038059','JOB038056','JOB039017','JOB039013','JOB037724','JOB029675','JOB037723','JOB039016','JOB039014','JOB038057','JOB035484','JOB043276','JOB043278','JOB043280','JOB043284','JOB043286','JOB043287','JOB043288','JOB043290','JOB043291','JOB043292','JOB043293')


-->>CASE #4
select id, external_id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where external_id = 'REC-250767' --id=40338

select *
from mike_tmp_contact_dup_check2
where contact_id = 40338 --merge with contact_id=18853

select id, external_id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where id in (18853, 40338)

select id, external_id, company_id, contact_id, company_id_bkup, contact_id_bkup
from position_description
where external_id in ('JOB035486','JOB039015','JOB041618','JOB049117','JOB038060')

--Update 5 rows
update position_description
set contact_id = 18853
where external_id in ('JOB035486','JOB039015','JOB041618','JOB049117','JOB038060')


-->>CASE #5
select id, external_id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where external_id = 'REC-250883' --id=40431

select *
from mike_tmp_contact_dup_check2
where contact_id = 40431 --merge with contact_id=18852

select id, external_id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where id in (18852, 40431)

select id, external_id, company_id, contact_id, company_id_bkup, contact_id_bkup
from position_description
where external_id in ('JOB051079')

--Update 1 rows
update position_description
set contact_id = 18852
where external_id in ('JOB051079')


-->>CASE #6
select id, external_id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where external_id = 'REC-251507' --id=40978

select id, external_id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact 
where company_id =14657

select *
from mike_tmp_contact_dup_check2
where contact_id = 40978 --merge with contact_id=?

select id, external_id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where id in (40978)

select id, external_id, company_id, contact_id, company_id_bkup, contact_id_bkup
from position_description
where external_id in ('JOB052049','JOB066978','JOB049387','JOB049385','JOB049040','JOB048950','JOB049386','JOB048973','JOB043474','JOB045681','JOB049087','JOB049388')

--Update 12 rows
update position_description
set contact_id = 40978
where external_id in ('JOB052049','JOB066978','JOB049387','JOB049385','JOB049040','JOB048950','JOB049386','JOB048973','JOB043474','JOB045681','JOB049087','JOB049388')


-->>CASE #7
select id, external_id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where external_id = 'REC-251819' --id=41227

select id, external_id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact 
where company_id =14657

select *
from mike_tmp_contact_dup_check2
where contact_id = 41227 --merge with contact_id=18851

select id, external_id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where id in (18851,41227)

select id, external_id, company_id, contact_id, company_id_bkup, contact_id_bkup
from position_description
where external_id in ('JOB050594','JOB048817','JOB050518')

--Update 3 rows
update position_description
set contact_id = 18851
where external_id in ('JOB050594','JOB048817','JOB050518')


-->>CASE #8
select id, external_id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where external_id = 'REC-253581' --id=42711

select id, external_id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact 
where company_id =14657

select *
from mike_tmp_contact_dup_check2
where contact_id = 42711 --merge with contact_id=18851

select id, external_id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where id in (42711)

select id, external_id, company_id, contact_id, company_id_bkup, contact_id_bkup
from position_description
where external_id in ('JOB052225')

--Update 1 rows
update position_description
set contact_id = 42711
where external_id in ('JOB052225')


-->>CASE #9
select id, external_id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where external_id = 'REC-276223' --id=62813

select id, external_id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact 
where company_id =14657

select *
from mike_tmp_contact_dup_check2
where contact_id = 62813 --merge with contact_id=26586

select id, external_id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where id in (26586, 62813)

select id, external_id, company_id, contact_id, company_id_bkup, contact_id_bkup
from position_description
where external_id in ('JOB135808')

--Update 1 rows
update position_description
set contact_id = 26586
where external_id in ('JOB135808')


-->>CASE #10
select id, external_id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where external_id = 'DEFCPY000714' --id=65314

select id, external_id, company_id, contact_id, company_id_bkup, contact_id_bkup
from position_description
where external_id in ('JOB013822','JOB013827','JOB013826','JOB013825','JOB013811','JOB013816','JOB013828','JOB013813','JOB013809','JOB013817','JOB013818','JOB013819','JOB013815','JOB006338','JOB009695','JOB002529','JOB013820','JOB009696','JOB002527','JOB002531','JOB003480','JOB002524','JOB002426','JOB002355','JOB002353','JOB003481','JOB002530','JOB002427','JOB005640','JOB005642','JOB005643','JOB005736','JOB005731','JOB002525','JOB002535','JOB002526','JOB003482','JOB002532','JOB005448','JOB007359','JOB007358','JOB006704','JOB006703','JOB006702','JOB006340','JOB005685','JOB005452','JOB005451','JOB005445','JOB005444','JOB002354','JOB002533','JOB005449','JOB006339','JOB007357','JOB009703','JOB009704','JOB009697','JOB013823','JOB002528','JOB010426','JOB010427','JOB007356','JOB005689','JOB009699','JOB005450','JOB005442','JOB002534','JOB010425','JOB009702','JOB009705','JOB010430','JOB010429','JOB010428','JOB009706','JOB009701','JOB009698','JOB009700')

--Update 78 rows
update position_description
set contact_id = 65314
where external_id in ('JOB013822','JOB013827','JOB013826','JOB013825','JOB013811','JOB013816','JOB013828','JOB013813','JOB013809','JOB013817','JOB013818','JOB013819','JOB013815','JOB006338','JOB009695','JOB002529','JOB013820','JOB009696','JOB002527','JOB002531','JOB003480','JOB002524','JOB002426','JOB002355','JOB002353','JOB003481','JOB002530','JOB002427','JOB005640','JOB005642','JOB005643','JOB005736','JOB005731','JOB002525','JOB002535','JOB002526','JOB003482','JOB002532','JOB005448','JOB007359','JOB007358','JOB006704','JOB006703','JOB006702','JOB006340','JOB005685','JOB005452','JOB005451','JOB005445','JOB005444','JOB002354','JOB002533','JOB005449','JOB006339','JOB007357','JOB009703','JOB009704','JOB009697','JOB013823','JOB002528','JOB010426','JOB010427','JOB007356','JOB005689','JOB009699','JOB005450','JOB005442','JOB002534','JOB010425','JOB009702','JOB009705','JOB010430','JOB010429','JOB010428','JOB009706','JOB009701','JOB009698','JOB009700')