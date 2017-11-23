--PHASE 1----INSERT .MSG FILES TO CANDIDATE
--Phase 1: Add components from ***documents and ***emails (.msg) from DB
with CandidateMails as (
select concat('RC',candidate_id) as RCcandidateid, concat(doc_id,'_',replace(replace(doc_name,',',''),'.',''),rtrim(ltrim(doc_ext))) as RCuploaded_filename
from tblCandidateDocs where doc_ext like '%msg%' --from Documents table with .msg type

UNION ALL

select concat('RC',ec.CandidateID), concat(ec.EmailID,rtrim(EmailFileExt)) as EmailFile 
	from tblEmailCandidates ec
	left join tblEmails e on e.EmailID = ec.EmailID) -- from Emails table

--
select RCcandidateid, RCuploaded_filename
	, RCuploaded_filename as RCsave_filename
	, 'application/octet-stream' as RCmime_type
	, 1 as RCversion_no
	, getdate() as RCinsert_timestamp
	, 'additional_documents' as RCdocument_type -- 'resume' or 'additional_documents' for CANDIDATE
	, getdate() as RCcreated
	, -10 as RCuser_account_id
	, 0 as RCsuccessful_parsing_percent
	, -1 as RCgoogle_viewer
	, 1 as RCtemporary
	, getdate() as RCtrigger_index_update_timestamp
	, 3 as RCdocument_types_id
	from CandidateMails
	where RCcandidateid between 'RC48994' and 'RC49093' -- Limit for checking purposes
	
--Phase 2: Lookup candidate id

--Phase 3: Insert into candidate_document table
select * from candidate_document

select * from candidate_document where candidate_id = 151115

select * from candidate_document_detail_view

select * from candidate_document where candidate_id = 101715

select * from candidate_document where document_type = 'additional_documents'

select * from candidate where external_id = 'RC48994' -- 101715 | "candidate-48994@noemail.com"

--Phase 4: Upload files to s3://file-server-prod-sg-hrboss-com/skillsolved.vinceredev.com/document/ 
--> Other docs will be saved here - otherwise, in resume folder

---------------------------------
--PHASE 2----INSERT .MSG FILES TO CONTACT
--Phase 1: Add components from ***emails (.msg) from DB
select concat('RC',ec.ContactID) as RCcontactid, concat(ec.EmailID,rtrim(EmailFileExt)) as RCuploaded_filename
	, concat('Contact_',ec.EmailID,rtrim(EmailFileExt)) as RCsave_filename
	, 'application/octet-stream' as RCmime_type
	, 1 as RCversion_no
	, getdate() as RCinsert_timestamp
	, 'document' as RCdocument_type -- 'document' for CONTACT
	, getdate() as RCcreated
	, -10 as RCuser_account_id
	, 0 as RCsuccessful_parsing_percent
	, -1 as RCgoogle_viewer
	, 1 as RCtemporary
	, getdate() as RCtrigger_index_update_timestamp
	from tblEmailContacts ec
	left join tblEmails e on e.EmailID = ec.EmailID
	
/* TEST SCRIPT

select concat('RC',candidate_id) as RCcandidateid, concat(doc_id,'_',replace(replace(doc_name,',',''),'.',''),rtrim(ltrim(doc_ext))) as RCuploaded_filename
from tblCandidateDocs where doc_ext like '%msg%'
and candidate_id between 48994 and 49093

UNION ALL

select concat('RC',ec.CandidateID), concat(ec.EmailID,rtrim(EmailFileExt)) as EmailFile 
	from tblEmailCandidates ec
	left join tblEmails e on e.EmailID = ec.EmailID
	where CandidateID between 48994 and 49093
	
*/	
	
--Phase 2: Lookup candidate id
select count(*) from candidate_document where contact_id is not NULL and contact_id <> 0 and uploaded_filename like '%.msg%'

select id, external_id from contact where id = 33101

select * from candidate_document where contact_id = 33101

delete from candidate_document where contact_id is not NULL and contact_id <> 0 and uploaded_filename like '%.msg%'

select * from candidate_document where contact_id is not NULL and contact_id <> 0 and uploaded_filename like '%.msg%'


--Phase 3: Insert into candidate_document table
select * from candidate_document

select * from candidate_document where contact_id = 151115

select * from candidate_document_detail_view

--13 columns:
contact_id
uploaded_filename
saved_filename
mime_type
version_no
insert_timestamp
document_type
created
user_account_id
successful_parsing_percent
google_viewer
temporary
trigger_index_update_timestamp

--Phase 4: Upload files to s3://file-server-prod-sg-hrboss-com/skillsolved.vinceredev.com/document/