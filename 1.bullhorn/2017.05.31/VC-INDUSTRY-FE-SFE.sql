select * from candidate_functional_expertise where candidate_id = 62810;

select id,external_id, first_name, last_name from candidate where id in (62810) or external_id = '104'



------------------------------------------------------
-- INDUSTRY
select * from candidate_industry where candidate_id = 124586 --INDUSTRY
select * from vertical where id IN (28882) --INDUSTRY LIST
select * from vertical where name like '%Advertising%' -- 'Promotional Incentives'
------------------------------------------------------
-- FUNCTIONAL_EXPERTISE
CREATE SEQUENCE rid_seq START WITH 3200 INCREMENT BY 1 ;
alter table functional_expertise alter column id set default nextval('rid_seq');
commit;

select * from functional_expertise where id in (3048,3113)
insert into functional_expertise(name) values ('Wholesale')

------------------------------------------------------
-- SUB-FUNCTIONAL_EXPERTISE
select * from sub_functional_expertise where name in ('Audit - Hong Kong','Risk - Hong Kong') and functional_expertise_id in (2995,3030,3077,3108) --id in (334,390)
select distinct name from sub_functional_expertise

