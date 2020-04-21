select distinct ts2_taxonomy_skill_c
from ts2_skill_c

select distinct ts2_skill_name_c
from ts2_skill_c --780 rows

select distinct ts2_taxonomy_c
from ts2_skill_c

select t.id as FE_id
, t.name as FE
, t.ts2_taxonomy_externalid_c
, s.ts2_taxonomyid_c as FE_SFE_id
, s.id as SFE_id
, s.name as SFE_unused --not correct
, s.ts2_term_c as SFE
, s.ts2_skillsexternalid_c
, s.ts2_taxonomyid_txt_c
from ts2_taxonomies_c t --FE
left join ts2_skills_c s on s.ts2_taxonomyid_c = t.id --SFE table

--Check FE/SFE referece
select count(id) from ts2_skill_c --349730 | total records

--Check if FE reference exists
select count(id) from ts2_skill_c where ts2_taxonomy_c is not NULL --2108

select count(id) from ts2_skill_c where ts2_taxonomy_skill_c is not NULL --85484

select distinct ts2_taxonomy_skill_c from ts2_skill_c --370 rows

select distinct ts2_skill_name_c
from ts2_skill_c
where lower(ts2_skill_name_c) in (select distinct lower(name) from ts2_skills_c) --565 found

select distinct ts2_skill_name_c
from ts2_skill_c
where lower(ts2_skill_name_c) not in (select distinct lower(name) from ts2_skills_c) --214 not found

select * from ts2_skill_c
where ts2_skill_name_c ilike '%web%analytic%' --72

select * from ts2_skill_c
where ts2_skill_name_c like '%(JS)' --9 rows

select * from ts2_skills_c
where ts2_term_c ilike '%web%analytic%' --2 | ts2_taxonomyid_c = a0h0Y000007CeqQQAS, a0h0Y000007ELebQAG

select ts2_name_c
from ts2_taxonomies_c
where id in ('a0h0Y000007CeqQQAS', 'a0h0Y000007ELebQAG')
