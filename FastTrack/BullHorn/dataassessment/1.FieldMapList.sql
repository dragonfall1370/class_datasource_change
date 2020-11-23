
with t as (
-- COMPANY
select * from bullhorn1.BH_FieldMapList where (
entity in ('Client') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'BH_ClientCorporation' and table_schema = 'bullhorn1') or
entity in ('Client','Company File Attachment') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'BH_ClientCorporationFile' and table_schema = 'bullhorn1')
) and columnName not like 'custom%'
UNION
select * from bullhorn1.BH_FieldMapList
where isHidden = 'false' and entity = 'Client' and columnName like '%custom%' and display not like 'custom%' and 
entity in ('Client') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'BH_ClientCorporation' and table_schema = 'bullhorn1') 


UNION
-- CONTACT
select * from bullhorn1.BH_FieldMapList where (
entity in ('Client Contact') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'View_ClientContact' and table_schema = 'bullhorn1') or
entity in ('Client Contact') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'BH_UserContact_View' and table_schema = 'bullhorn1') or
entity in ('Contact File Attachment') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'View_ClientContactFile' and table_schema = 'bullhorn1') or
entity in ('Distribution List') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'BH_DistributionList' and table_schema = 'bullhorn1') or
entity in ('Note') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'BH_UserComment' and table_schema = 'bullhorn1') or
entity in ('Appointment') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'View_Appointment' and table_schema = 'bullhorn1') or
entity in ('Task') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'View_Task' and table_schema = 'bullhorn1')
) and columnName not like 'custom%'
UNION
select * from bullhorn1.BH_FieldMapList
where isHidden = 'false' and entity = 'Client Contact' and columnName like '%custom%' and display not like 'custom%'
and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'View_ClientContact' and table_schema = 'bullhorn1')


UNION
--JOB
select *
from bullhorn1.BH_FieldMapList where (
entity in ('Job Posting') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'BH_JobPosting' and table_schema = 'bullhorn1') or
entity in ('Job Order File Attachment') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'View_JobPostingFile' and table_schema = 'bullhorn1') or
entity in ('Note') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'BH_UserComment' and table_schema = 'bullhorn1') or
entity in ('Appointment') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'View_Appointment' and table_schema = 'bullhorn1') or
entity in ('Task') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'View_Task' and table_schema = 'bullhorn1')
) and columnName not like 'custom%'
UNION
select * from bullhorn1.BH_FieldMapList
where isHidden = 'false' and entity = 'Job Posting' and columnName like '%custom%' and display not like 'custom%'
and entity in ('Job Posting') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'BH_JobPosting' and table_schema = 'bullhorn1')


UNION
-- CANDIDATE
select * from bullhorn1.BH_FieldMapList where ( --entity in ('Appointment','Candidate','Candidate File Attachment','CandidateCertification','Certification','Certification File Attachment','Distribution List','Education','Note','Reference','Task','Tearsheet','Work History')
entity in ('Appointment') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'View_Appointment' and table_schema = 'bullhorn1') or
entity in ('Candidate') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'Candidate' and table_schema = 'bullhorn1') or
entity in ('Candidate File Attachment') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'View_CandidateFile' and table_schema = 'bullhorn1') or
entity in ('CandidateCertification') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'BH_UserCertification' and table_schema = 'bullhorn1') or
entity in ('Certification') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'BH_Certification' and table_schema = 'bullhorn1') or
entity in ('Certification File Attachment') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'BH_CertificationFileAttachment' and table_schema = 'bullhorn1') or
entity in ('Distribution List') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'BH_DistributionList' and table_schema = 'bullhorn1') or
entity in ('Education') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'BH_UserEducation' and table_schema = 'bullhorn1') or
entity in ('Note') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'BH_UserComment' and table_schema = 'bullhorn1') or
entity in ('Refe=)))))rence') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'BH_UserReference' and table_schema = 'bullhorn1') or
entity in ('Task') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'View_Task' and table_schema = 'bullhorn1') or
--entity in ('Task') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = '' and table_schema = 'bullhorn1') or
entity in ('Work History') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'BH_userWorkHistory' and table_schema = 'bullhorn1')
) and columnName not like 'custom%'
UNION
select * from bullhorn1.BH_FieldMapList
where isHidden = 'false' and columnName like '%custom%' and display not like 'custom%'
and entity = 'Candidate' and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'Candidate' and table_schema = 'bullhorn1')


UNION
--PLACEMENT
select *
from bullhorn1.BH_FieldMapList where (
entity in ('Placement') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'View_Placement' and table_schema = 'bullhorn1') or
entity in ('Placement Commission') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'BH_Commission' and table_schema = 'bullhorn1') or
entity in ('Placement File Attachment') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'View_PlacementFile' and table_schema = 'bullhorn1')
) and columnName not like 'custom%'
UNION
select * from bullhorn1.BH_FieldMapList
where isHidden = 'false' and entity = 'Placement' and columnName like '%custom%' and display not like 'custom%'
and entity in ('Placement') and columnName in (select COLUMN_NAME from information_schema.columns where table_name = 'View_Placement' and table_schema = 'bullhorn1')
)



--select distinct entity, columnName, display, editType, isRequired, isHidden, valueList, description, hint, defaultValue --, count(*) 
select distinct entity, columnName, display, editType, isHidden, valueList, description, hint, defaultValue --, count(*) 
from t 
where isHidden = 'false'
order by entity asc, columnName asc

