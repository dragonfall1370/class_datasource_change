select * from position_candidate_feedback;
select * from position_candidate_fact2 where doc_id::int = 1119235 or owner_id::int = 521280

select * from position_description where external_id::int = 521280 or id = 36002
select id,full_description,public_description from position_description where id = 36002


update company_comment 
set comment_content = cf.comment_body
--select cf.comment_body,cf.doc_id, c.id, c.external_id, cc.company_id,cc.user_id,cc.comment_content
from position_candidate_feedback cf
left join position_candidate_fact2 cf2 on cf.doc_id::int = cf2.doc_id::int
left join company c on c.external_id::int = cf2.owner_id::int
left join company_comment cc on c.id::int = cc.company_id
where c.id = 11437

update position_description
set	full_description = replace(cf.comment_body,'Job Description: ','') --Internal job description
	,public_description = replace(cf.comment_body,'Job Description: ','')
	--values(36002, 'full', 'public')
--select c.id,cf.comment_body --,cf.doc_id
from position_candidate_feedback cf
left join position_candidate_fact2 cf2 on cf.doc_id::int = cf2.doc_id::int
left join position_description c on c.external_id::int = cf2.owner_id::int
left join company_comment cc on c.id::int = cc.company_id
where c.id = 36002
--order by cf.doc_id::int


delete from company_comment where company_id = 11437

SELECT * FROM DOCUMENTS  left join PROP_CLIENT_GEN on  OWNER_ID = PROP_CLIENT_GEN.REFERENCE where DOC_CATEGORY in (7023000,31185)


update position_description
set	full_description = replace(cf.comment_body,'Job Description: ','') --Internal job description
	,public_description = replace(cf.comment_body,'Job Description: ','')
	--values(36002, 'full', 'public')
--select c.id,cf.comment_body --,cf.doc_id
from position_candidate_feedback cf
left join position_candidate_fact2 cf2 on cf.doc_id::int = cf2.doc_id::int
left join position_description c on c.external_id::int = cf2.owner_id::int
left join company_comment cc on c.id::int = cc.company_id
where c.id = 36002
--order by cf.doc_id::int

update position_description pd1
set	full_description = d.document
-- select * --count(*) --1671
-- select count(distinct pd.id) --1663-- select count(pd.id) --1671
from DOCUMENTS d
left join position_description pd on d.owner_id::int = pd.external_id::int
where d.DOC_CATEGORY in (6532897) and d.document != '' and d.document is not null and pd.external_id is not null
and d.owner_id::int = pd1.external_id::int

update position_description pd1
set	public_description = d.document
-- select pd.id, d.doc_id,d.owner_id,d.document,d.DOC_CATEGORY
--select document as (select owner_id, STUFF((SELECT DISTINCT ', ' + document from DOCUMENTS where DOC_CATEGORY in (6532850) and document != '' and document is not null and owner_id = a.owner_id FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.owner_id)
-- select OWNER_ID, string_agg(segment, ' ') from (select distinct OWNER_ID, unnest(string_to_array(document, ' ')) as segment       from DOCUMENTS     ) t group by OWNER_ID;               
-- select count(distinct pd.id) --select count(pd.id) --634
from DOCUMENTS d
left join position_description pd on d.owner_id::int = pd.external_id::int
where d.DOC_CATEGORY in (6532850) and d.document != '' and d.document is not null and pd.external_id is not null
and d.owner_id::int = pd1.external_id::int

select doc_id,owner_id,document from DOCUMENTS 
select id,full_description,public_description,external_id from position_description
select  distinct public_description from position_description