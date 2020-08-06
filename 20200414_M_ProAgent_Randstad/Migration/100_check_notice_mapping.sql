--Incorrect mapping PA FE/SFE
select * from mike_tmp_desired_fe_sfe_audit --3048 | Executive Support / エフゼクティブサポート - 3053 | Telemarketing Call Center Management / コールセンター運営管理

select * from mike_tmp_desired_fe_sfe_audit
where vc_sfe_id is null

select *
from sub_functional_expertise
where functional_expertise_id in (3048, 3053)

select * from mike_tmp_desired_fe_sfe_audit
where vc_sfe is null

select id, external_id
from candidate
where id in (135027, 137008, 131867)

update sub_functional_expertise
set name = trim(name)

select *
from sub_functional_expertise_detail_language

update sub_functional_expertise_detail_language
set name = trim(name)


/* DETAILS PA FE/SFE
製造・技術系	生産企画・生産管理 (incorrect mapping) -> 事務・管理系	生産企画・生産管理
*/