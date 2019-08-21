
create table _tmp_company_document (
        OWNER_ID bigint,
        DOC_ID text
        )

with
--doc(OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY in (7022996,31190) and FILE_EXTENSION != 'txt' and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
 doc(OWNER_ID, DOC_ID) as ( SELECT OWNER_ID, cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION as DOC_ID from DOCUMENTS WHERE DOC_CATEGORY in (7022996,31190) and FILE_EXTENSION != 'txt' )
---------------------------

select id,external_id,name from company where name like '%Ashurst%' -- id = 10213 410112
select * from _tmp_company_document where owner_id = 410112


insert into candidate_document (uploaded_filename,saved_filename,document_type,legal_doc_id) --values ('test.doc','test.doc','legal_document')
select tcd.doc_id,tcd.doc_id, 'legal_document',cld.id from _tmp_company_document tcd
left join company co on co.external_id::int = tcd.owner_id::int
left join COMPANY_LEGAL_DOCUMENT cld on cld.company_id = co.id
where co.external_id is not null and cld.ID = 9409

SELECT candidate_id,uploaded_filename,saved_filename,document_type,legal_doc_id FROM CANDIDATE_DOCUMENT WHERE document_type = 'legal_document' --uploaded_filename = '_company.doc'
-- select * from bulk_upload_detail where entity_type = 'COMPANY'

SELECT * FROM COMPANY_LEGAL_DOCUMENT WHERE COMPANY_ID = 9446
SELECT * FROM COMPANY_LEGAL_DOCUMENT WHERE ID = 9368
select * from company where id = 9446