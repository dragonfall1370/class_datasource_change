select count(*) from position_candidate_feedback where user_account_id = '-10' 
select * from position_candidate_feedback limit 2000 where user_account_id = '-10' limit 100 --doc_id in (1498016,1498017,886944)
select * from contact_comment
select contact_id,user_id,comment_content from contact_comment
select * from position_candidate_fact2 where owner_id::int = 402796
select id,external_id from candidate where id = 78784
select count(*) from candidate

update company_comment 
set comment_content = cf.comment_body
-- select cf.comment_body,cf.doc_id, c.id, c.external_id, cc.company_id,cc.user_id,cc.comment_content
from position_candidate_feedback cf
left join position_candidate_fact2 cf2 on cf.doc_id::int = cf2.doc_id::int
left join contact c on c.external_id::int = cf2.owner_id::int
left join company_comment cc on c.id::int = cc.company_id
where c.id = 78784

========================================================================================================================
create sequence rid_seq;
alter table contact_comment alter column id set default nextval('rid_seq');
commit;
------------------- 
CREATE SEQUENCE rid_seq0 START WITH 5000 INCREMENT BY 1;
alter table contact_comment alter column id set default nextval('rid_seq0');
commit;
-------------------
alter table contact_comment alter column insert_timestamp set default now()::timestamp ; -- RESULT: insert_timestamp TIMESTAMP(6) WITHOUT TIME ZONE DEFAULT (now())::TIMESTAMP without TIME zone,
------------------
insert into position_candidate_feedback(candidate_id,user_account_id,comment_body,feedback_timestamp) --values(78784, -10, 'ABC')
select ca.id,-10,d.document, d.updated_date -- ,ca.first_name, ca.last_name, ca.external_id::int
from documents d
left join candidate ca on d.owner_id::int = ca.external_id::int
where d.DOC_CATEGORY in (6532839,6532840) and d.document != '' and d.document is not null and ca.external_id is not null
order by d.updated_date desc

select * from position_candidate_feedback limit 1000 where candidate_id = 108217
delete from contact_comment where contact_id = 78784
select id,external_id from candidate where external_id is not null
select * from documents order by updated_date
select candidate_id,user_account_id,comment_body from position_candidate_feedback order by candidate_id asc limit 100

select   f.candidate_id,f.user_account_id,f.comment_body
        ,c.id, c.external_id
from position_candidate_feedback f
left join candidate c on c.id = f.candidate_id
where c.external_id is not null --and c.external_id::int < 471167
order by c.external_id asc limit 50
