---UPDATE DOCUMENT FILE NAME
select * from company_legal_document --e.g. id = 9356, company_id = 52720

select id, external_id from company where id = 52720 --PRTR1017201

select * from candidate_document where legal_doc_id = 9356 --e.g. id = 874966, uploaded_filename = 'cf1214fe-2d2d-44d2-9198-f51f31adfe51.pdf'

update candidate_document set uploaded_filename = 'job spec asst accounting mgr.pdf' where id = 874966