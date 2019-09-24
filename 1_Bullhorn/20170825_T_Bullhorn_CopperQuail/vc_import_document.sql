-----COMPANY
select id,name,external_id from company where id = 9350 external_id::int = 12896728 9346 --name like '%WSP-Cape Town%'

insert into COMPANY_LEGAL_DOCUMENT (company_id) values (9346)
select * from COMPANY_LEGAL_DOCUMENT where company_id = 9346;
SELECT * FROM COMPANY_LEGAL_DOCUMENT cld left join candidate_document cd on cd.legal_doc_id = cld.id WHERE COMPANY_ID = 9346
select id as cld_ACTUAL_LEGAL_ID, company_id as cld_ACTUAL_COMPANY_ID, title from COMPANY_LEGAL_DOCUMENT where title like '%.Cli%';
-- delete FROM COMPANY_LEGAL_DOCUMENT WHERE COMPANY_ID = 9346 and title like '%.Cli%' or id = 9450

-- delete select * FROM candidate_document WHERE legal_doc_id > 0--uploaded_filename is null --COMPANY_ID = 9346
update candidate_document 
set candidate_email_id = ''  --temporary = 1 --position_description_id = 0 --uploaded_filename = '831.Cli12896728_16 Jul 2015 10 00 05_Johannes Loots.doc' --legal_doc_id = 9450 
--set saved_filename = '831.Cli12896728_16 Jul 2015 10 00 05_Johannes Loots.doc'--legal_doc_id = 9450 
where id = 67846

insert into candidate_document (uploaded_filename,saved_filename,document_type,legal_doc_id) values ('828.Cli12896728_16 Jul 2015 09 34_ESC Terms and Conditions 21-02-2014-1.pdf','828.Cli12896728_16 Jul 2015 09 34_ESC Terms and Conditions 21-02-2014-1.pdf','legal_document',9457)
SELECT candidate_id,uploaded_filename,saved_filename,document_type,legal_doc_id FROM CANDIDATE_DOCUMENT WHERE document_type = 'legal_document'; --uploaded_filename = '_company.doc'
SELECT * FROM CANDIDATE_DOCUMENT WHERE document_type = 'legal_document' and id in (67854,67856,67874) and uploaded_filename = saved_filename --and id in (67846,67856) --
-- delete from candidate_document where document_type = 'unknown' --legal_doc_id in (9430,9410,9410,9430,9430,9410,9410,9430,9430,9410,9410,9410,9430,9366)



-----CANDIDATE
insert into candidate_document (candidate_id,uploaded_filename,saved_filename,document_type) values (34412,'Can81239317.doc','Can81239317.doc','resume')
select id,first_name, last_name, external_id from candidate where external_id::int = 81239317
SELECT count(*)  FROM CANDIDATE_DOCUMENT WHERE uploaded_filename = saved_filename -- document_type = 'resume' ;
--select * from bulk_upload_detail where entity_type = 'COMPANY';


----- VC COMMAND
select * from candidate_document where candidate_id in (34496)
select * from candidate where id = 34496
select * from candidate_document_detail_view where candidate_id = 34496
update candidate_document set mime_type = 'application/msword' where id in (35626,55138)
update candidate_document
set filesize = 202443 --user_account_id = -10 --version_no = 1 --mime_type = 'application/pdf'
where id in (67846)
--delete from candidate_document where uploaded_filename is null --6138
select count(*) from candidate_document where uploaded_filename = saved_filename
update bulk_upload set status = 'COMPLETE' where id = 81

select * from bulk_upload_document_mapping where entity_type = 'COMPANY'
insert into bulk_upload_document_mapping (entity_id,entity_type,file_name,document_type) values (71194,'COMPANY','1063482.pdf','legal_document')
select * from bulk_upload_detail where  entity_type = 'COMPANY' and entity_id = 9346 order by id 

select * from bulk_upload_detail where  id = 39764 
select * from candidate_document where id = 67874