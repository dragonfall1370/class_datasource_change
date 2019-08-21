
select id,external_id, first_name, last_name from candidate where id in (62810) or external_id = '104'

select * from candidate_industry where candidate_id = 71780 --INDUSTRY
select * from vertical where id IN (28885,28915) --INDUSTRY LIST
------------------------------------------------------
select * from candidate_functional_expertise where candidate_id = 62810;
------------------------------------------------------
select * from functional_expertise where id in (3048,3113)

CREATE SEQUENCE rid_seq START WITH 3200 INCREMENT BY 1 ;
alter table functional_expertise alter column id set default nextval('rid_seq');
commit;

insert into functional_expertise(name) values ('Wholesale')
------------------------------------------------------

select * from sub_functional_expertise where  name in ('Audit - Hong Kong','Risk - Hong Kong') and functional_expertise_id in (2995,3030,3077,3108)
--id in (334,390)
select distinct name from sub_functional_expertise

