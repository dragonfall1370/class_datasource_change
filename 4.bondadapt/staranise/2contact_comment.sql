select * from position_candidate_feedback where doc_id in (1498016
,1498017,886944)
select c.first_name,c.last_name, cc.comment_content from contact_comment cc left join contact c on c.id = cc.contact_id
select contact_id,user_id,comment_content from contact_comment
select * from position_candidate_fact2 where owner_id::int = 402796
select id,external_id from contact where id = 78784

update company_comment 
set comment_content = cf.comment_body
-- select cf.comment_body,cf.doc_id, c.id, c.external_id, cc.company_id,cc.user_id,cc.comment_content
from position_candidate_feedback cf
left join position_candidate_fact2 cf2 on cf.doc_id::int = cf2.doc_id::int
left join contact c on c.external_id::int = cf2.owner_id::int
left join company_comment cc on c.id::int = cc.company_id
where c.id = 78784

--##########################--
create sequence rid_seq;
alter table contact_comment alter column id set default nextval('rid_seq');
commit;
--##########################--

insert into contact_comment(contact_id,user_id,comment_content) --values(78784, -10, 'ABC')
select c.id,-10,cf.comment_body ,cf.doc_id
from position_candidate_feedback cf
left join position_candidate_fact2 cf2 on cf.doc_id::int = cf2.doc_id::int
left join contact c on c.external_id::int = cf2.owner_id::int
left join contact_comment cc on c.id::int = cc.contact_id
--where c.id::int = 78784
order by cf.doc_id::int

delete from contact_comment where contact_id = 78784

insert into contact_comment(contact_id,user_id,comment_content) --values(78784, -10, 'ABC')
select c.id,-10,d.document --,d.doc_id
from DOCUMENTS d
left join contact c on d.owner_id::int = c.external_id::int
where d.DOC_CATEGORY in (6532841) and d.document != '' and d.document is not null and c.external_id is not null --16861
order by d.updated_date desc

select * from DOCUMENTS d where d.DOC_CATEGORY in (6532841) and d.document != '' and d.document is not null
