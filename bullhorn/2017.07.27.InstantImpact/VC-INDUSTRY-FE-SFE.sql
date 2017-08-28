
---------------------------------------------------- CONTACT ----------------------------------------------------
-- CONTACT FE SFE
select * from contact_functional_expertise where contact_id = 125374;

select c.id,c.external_id, c.first_name, c.last_name
       --, cfe.* --, cfe.functional_expertise_id, cfe.sub_functional_expertise_id 
       , fe.name as fe, sfe.name as sfe
from contact_functional_expertise cfe
left join contact c on c.id = cfe.contact_id
left join functional_expertise fe on fe.id = cfe.functional_expertise_id
left join sub_functional_expertise sfe on sfe.id = cfe.sub_functional_expertise_id
--where c.external_id is not null
where c.external_id in ('314208-8189-6324')
order by c.first_name asc;

delete from contact_functional_expertise cfe where cfe.contact_id in (
                select distinct contact_id from contact_functional_expertise cfe left join contact c on c.id = cfe.contact_id
                where c.external_id is not null ) --1502


---------------------------------------------------- CANDIDATE ----------------------------------------------------
select id,external_id, first_name, last_name from candidate where external_id in ('992904-5027-13143') -- id in (124611) or first_name = 'Madeleine' or 

-- INDUSTRY
select * from candidate_industry where candidate_id = 71780 --INDUSTRY
select * from vertical where id IN (28885,28915) --INDUSTRY LIST


-- CANDIDATE FE SFE
select distinct candidate_id from candidate_functional_expertise where functional_expertise_id = 2994;
select * from candidate_functional_expertise where candidate_id in (180668,188793,129047)

select c.id,c.external_id, c.first_name, c.last_name
       --, cfe.* --, cfe.functional_expertise_id, cfe.sub_functional_expertise_id 
       , fe.name as fe, sfe.name as sfe
from candidate_functional_expertise cfe
left join candidate c on c.id = cfe.candidate_id
left join functional_expertise fe on fe.id = cfe.functional_expertise_id
left join sub_functional_expertise sfe on sfe.id = cfe.sub_functional_expertise_id
--where c.id in (180668,188793,129047)
--where c.external_id is not null
where c.external_id in ('992904-5027-13143')
order by c.first_name asc;

-- INSERT IMPORTED FE & SFE
insert into candidate_functional_expertise (functional_expertise_id,candidate_id) values (2994,193110);

-- DELETE IMPORTED FE & SFE
delete from candidate_functional_expertise cfe where cfe.candidate_id in (
                select distinct candidate_id from candidate_functional_expertise cfe left join candidate c on c.id = cfe.candidate_id
                where c.external_id is not null ) --c.external_id is null --33262 33253

-- COUNTING
with t as (select c.id,c.external_id, c.first_name, c.last_name
               --, cfe.* --, cfe.functional_expertise_id, cfe.sub_functional_expertise_id 
               , fe.name as fe, sfe.name as sfe
           from candidate_functional_expertise cfe
           left join candidate c on c.id = cfe.candidate_id
           left join functional_expertise fe on fe.id = cfe.functional_expertise_id
           left join sub_functional_expertise sfe on sfe.id = cfe.sub_functional_expertise_id
           --where c.external_id is not null
           --where c.external_id in ('110387-4971-1110','100593-8845-12136') -- ('110998-3207-1554','110452-3164-11130','110393-3899-1662','110387-4971-1110','110362-5188-1540','110256-8229-9337','110245-1034-8294','110206-7622-13100','110129-5026-15322','110046-5356-15347','')
           where fe.name = 'Corporate PR' --and c.first_name = 'William'
           --limit 10
          )
--select * from t where sfe = ''
--select distinct external_id from t where sfe = ''
select count(distinct external_id) from t --4533





--------------------------------------------------------------------------------------------------------------
-- FE list
CREATE SEQUENCE rid_seq START WITH 3200 INCREMENT BY 1 ;
alter table functional_expertise alter column id set default nextval('rid_seq');
commit;

insert into functional_expertise(name) values ('Wholesale');
select * from functional_expertise where name in ('Corporate PR') --id in (3048,3113);


-- SFE list
select * from sub_functional_expertise where functional_expertise_id = (3025) --name in ('Events') or 
select distinct name from sub_functional_expertise


-- List all of FE, SFE
select  fe.id, fe.name
        ,sfe.id, sfe.name
from functional_expertise fe
left join sub_functional_expertise sfe on fe.id = sfe.functional_expertise_id
where fe.name in ('Corporate PR')