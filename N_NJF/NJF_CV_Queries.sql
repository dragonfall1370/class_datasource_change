alter table functional_expertise alter column id set default nextval('functional_experties_id_seq');
select count(*) from candidate where candidate_source_id is not null
select * from activity where type = 'job'
select external_id from position_description

select * from candidate_document where candidate_id = 113307
where primary_document = 1 and right(uploaded_filename,4) = '.msg'

UPDATE candidate_document SET primary_document = 1 WHERE id = 124255;--candidate id = 113307
UPDATE candidate_document SET primary_document = 0 WHERE candidate_id = 113307 and right(uploaded_filename,4) = '.msg'

select candidate_id,min(id) as minID
from candidate_document 
where candidate_id = 113307 and left (uploaded_filename,2) = 'CV'
group by candidate_id

with--get listcandidate have primary document is an msg file
temp as (select distinct candidate_id
from candidate_document 
where primary_document = 1 and right(uploaded_filename,4) = '.msg')

--get the first CV file of these candidate
, temp1 as (select candidate_id,min(id) as minID
from candidate_document 
where candidate_id in (select candidate_id from temp) and left (uploaded_filename,2) = 'CV'
group by candidate_id)

--make the first cv file as primary document
UPDATE candidate_document SET primary_document = 1 WHERE id in (select minid from temp1);

--remove the msg from primary document
UPDATE candidate_document SET primary_document = 0 
WHERE 
--candidate_id in (select candidate_id from temp1) 
primary_document = 1 and right(uploaded_filename,4) = '.msg'