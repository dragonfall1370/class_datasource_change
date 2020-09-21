select * from dbo.dt_journmap;
select * from dbo.DUP_SAFE_LK_ENTITIES_JOURNAL;
select * from dbo.JOURNAL_NOTES;
select *  FROM PROP_X_CLIENT_CON cc left join PROP_CLIENT_GEN cg on cg.reference = cc.client where cg.reference = 716701
select * from dbo.PROP_JOURNAL;
select OWNER_ID, DOC_CATEGORY, DOC_NAME, DOC_DESCRIPTION, FILE_EXTENSION, CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY, NOTES, "DEFAULT", SIZE, STATUS, OWNER_TYPE, PREVIEW_TYPE, DOCUMENT from DOCUMENTS where FILE_EXTENSION in ('txt','rtf') 
select distinct location from PROP_CAND_PREF
 from DOCUMENTS where DOC_NAME like '%recruit%'

create table position_candidate_feedback_content (
	id serial,
	candidate_externalId int4,
	user_account_id int4,
	comment_body text
)
alter table DOCUMENTS add column DOCUMENT text;
select * from DOCUMENTS where doc_category in (6532897,6532850) and document != ''  --doc_id = 1183554 --
select * from DOCUMENTS where  doc_id = 1008983
select count(*) from DOCUMENTS
select * from position_candidate_feedback;
delete from position_candidate_feedback_content where user_account_id = '-10';
select * from dbo.AVAILABLE_LANGUAGES where "LANGUAGE" is not null;
select * from dbo.PROP_CONT_GEN where "TOB_SENT" is not null;
select * from dbo.PROP_LIST_GEN where "TOB_RECD" is not null;
select * from dbo.PROP_X_ASSIG_TIME where "TIMESH" is not null;
select * from dbo.PROP_X_PAY_TIME where "TIMESH" is not null;
select * from PROP_INVP_GEN PROP_CLIENT_GEN where prop_client_gen.client_id = 1093627

with doc(OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ',' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE OWNER_ID = a.OWNER_ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
, with doc(OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STRING_AGG( concat(cast(DOC_ID as varchar(max)),'.',FILE_EXTENSION),',') WITHIN GROUP (ORDER BY DOC_ID) DOC FROM DOCUMENTS GROUP BY OWNER_ID)

select top 100 * from doc where doc.DOC_ID is null

select cg.client_id, cg.name, cg.REFERENCE, mn.* from PROP_CLIENT_GEN cg INNER JOIN MD_MULTI_NAMES MN ON MN.ID = cg.status where/* MN.ID is not null and LANGUAGE = 10010 and */cg.reference = 161385 /*and cg.name in ('Acxiom','Man Investments AG','LGT Group','SWX') and mn.description in ('Active','Do Not Use','Incomplete','Lapsed','Lead')*/
select cg.client_id, cg.name, cg.REFERENCE, mn.* from PROP_CLIENT_GEN cg INNER JOIN MD_MULTI_NAMES MN ON MN.ID = cg.client_type where MN.ID is not null and LANGUAGE = 10010 and cg.name in ('Acxiom','Man Investments AG','LGT Group','SWX')
select * from MD_MULTI_NAMES where id = 1303814
select * from PROP_CLIENT_GEN cg where cg.client_id in (42264, 161390,116659579857)
select distinct DESCRIPTION from PROP_CLIENT_GEN cg INNER JOIN MD_MULTI_NAMES MN ON MN.ID = cg.client_type where MN.ID is not null
select top 100 * from MD_MULTI_NAMES

, skill as (select distinct DESCRIPTION from PROP_SKILLS SKILL INNER JOIN MD_MULTI_NAMES MN ON MN.ID = SKILL.SKILL)
, location as (select REFERENCE,DESCRIPTION from PROP_LOCATIONS LOCATION INNER JOIN MD_MULTI_NAMES MN ON MN.ID = LOCATION.LOCATION)
, industry as (    select distinct DESCRIPTION from PROP_IND_SECT JOB_CAT INNER JOIN MD_MULTI_NAMES MN ON MN.ID = JOB_CAT.INDUSTRY )
, JobCategory as ( select distinct DESCRIPTION from PROP_JOB_CAT JOB_CAT INNER JOIN MD_MULTI_NAMES MN ON MN.ID = JOB_CAT.JOB_CATEGORY)
, SubCategory as ( select REFERENCE,DESCRIPTION from PROP_SUB_CAT SUB_CAT INNER JOIN MD_MULTI_NAMES MN ON MN.ID = SUB_CAT.SUB_CAT )

select * from dbo.DOCUMENTS_LOCAL where "CLIENT_ID" is not null;
select * from dbo.EMAIL_WEBMAIL_SETTINGS where "CLIENT_ID" is not null;
select * from dbo.ExagoActivity where "Client ID" is not null;
select * from dbo.ExagoClient where "Client ID" is not null;
select * from dbo.PROP_CLIENT_GEN where "CLIENT_ID" is not null;
select * from dbo.PROP_VARIABLES where "CLIENT_ID" is not null;
select * from dbo.PROP_VARIABLES where "CLIENT_IDNEW" is not null;

Select top 123 * from 	BUSINESS_OBJ_VERSIONS	;
Select top 123 * from 	select * from dbo.PROP_CLIENT_GEN where "BUREAU_ID_AW" is not null;	;
Select count(*) from 	BUSINESS_OBJECTS	;
select * from dbo.ExagoClient where client = 'CACI' "Owning Consultant (Perm)" is not null;
select * from dbo.ExagoContact where "Owning Consultant (Perm)" is not null;
select * from PROP_LOCATIONS where location in (20000587, 20000584, 1303626)
select * from  PROP_LOCATIONS_SAFE
select * from MD_NAMED_OCCS 
select * from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID INNER JOIN MD_MULTI_NAMES MN ON MN.ID = ADDRESS.COUNTRY where CONFIG_NAME = 'Primary'

select distinct jg.location_cd, cnt.DESCRIPTION
from PROP_X_CLIENT_JOB cj  --8036 rows
left join PROP_JOB_GEN jg on cj.JOB = jg.REFERENCE --where jg.JOB_ID = 882116
left join (select distinct REFERENCE, DESCRIPTION from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID INNER JOIN MD_MULTI_NAMES MN ON MN.ID = ADDRESS.COUNTRY where CONFIG_NAME = 'Primary' ) cnt ON jg.REFERENCE = cnt.REFERENCE
where jg.location_cd in (20000587, 20000584, 1303626)

select "Industry / Sector", count(*) from dbo.ExagoCandidateIndustrySector where "Industry / Sector" is not null group by "Industry / Sector";
select "Primary Industry/Sector", count(*) from dbo.ExagoClient where "Primary Industry/Sector" is not null group by "Primary Industry/Sector";
select "Industry / Sector", count(*) from dbo.ExagoClientIndustrySector where "Industry / Sector" is not null group by "Industry / Sector";
select "Industry / Sector", count(*) from dbo.ExagoContactIndustrySector where "Industry / Sector" is not null group by "Industry / Sector";
select "Industry / Sector", count(*) from dbo.ExagoJobIndustrySector where "Industry / Sector" is not null group by "Industry / Sector";
select "INDUSTRY", count(*) from dbo.PROP_ASSIG_GEN where "INDUSTRY" is not null group by "INDUSTRY";
select *, convert(nvarchar(max),convert(date,iv.IV_DATE)), CONVERT(VARCHAR,iv.IV_START,8) from PROP_IV_GEN iv
select distinct DESCRIPTION from PROP_IND_EXP IND_EXP INNER JOIN MD_MULTI_NAMES MN ON MN.ID = IND_EXP.IND_EXP --WHERE IND_EXP.REFERENCE

select "INDUSTRY", count(*) from dbo.PROP_CONT_INDSECT where "INDUSTRY" is not null group by "INDUSTRY";
select "INDUSTRY", count(*) from dbo.PROP_EMAIL_IND where "INDUSTRY" is not null group by "INDUSTRY";
select * from dbo.PROP_IND_SECT where "INDUSTRY" is not null group by "INDUSTRY";
select "INDUSTRY", count(*) from dbo.PROP_JOB_ACENTRAL where "INDUSTRY" is not null group by "INDUSTRY";
select "INDUSTRY", count(*) from dbo.PROP_JOB_BDBEAN where "INDUSTRY" is not null group by "INDUSTRY";
select "INDUSTRY", count(*) from dbo.PROP_JOB_JBINDSEC where "INDUSTRY" is not null group by "INDUSTRY";

select * from [dbo].[MD_MULTI_NAMES_OLLIE_SAFE_200811] where [DESCRIPTION] = 'Printing & Publishing';
select * from [dbo].[USER_MD_MULTI_NAMES] where [DESCRIPTION] = 'Printing & Publishing';
select * from [dbo].[zz_user_md_multi_names] where [DESCRIPTION] = 'Printing & Publishing';
select * from [dbo].[MD_MULTI_NAMES] where [DESCRIPTION] = 'Printing & Publishing';

delete from position_candidate_feedback where user_account_id = '28939';
select * from position_candidate_feedback;
select count(*) from position_candidate_feedback where user_account_id = '-10'; --21153

select count(*) from position_candidate_feedback_content where user_account_id = '-10'; --21153
select * from position_candidate_feedback_content where comment_body like '%interview%note%'

insert into position_candidate_feedback (candidate_id, user_account_id, comment_body )
( select ca.id, c.user_account_id,c.comment_body 
from position_candidate_feedback_content c 
left join candidate ca on cast(c.candidate_externalid as varchar(100)) = ca.external_id )
------------------

-- TESTING
select * from position_candidate_feedback where CANDIDATE_id IN ('38828','40185','43449','47693');
select first_name,last_name from candidate where ID IN ('38828','40185','43449','47693');

--------------------------------
select * from candidate_document_2 ;--27054
select count(*) from candidate_document_2 where candidate_id is null;  -- not null:26277   null:777
select count(*) from candidate_document_2 where uploaded_filename != '' ; -- value:13753 empty:13301

create table candidate_document_2 (
	candidate_externalid int,
	uploaded_filename character varying(400)
	)
	
ALTER TABLE candidate_document_2
ADD COLUMN candidate_id int;

select count (*) from candidate_document --11668
select count (*) from candidate_document where candidate_id = 0; --3002
select count (*) from candidate_document where candidate_id > 0; --8666

select * from candidate_document where candidate_id != 0
uploaded_filename in ('1181156.doc','1181545.docx');
select first_name,last_name from candidate where id in (41158,41180);
external_id = '474229';

select top 10 * from PROP_PERSON_HIST
select top 100 * from documents d left join PROP_PERSON_HIST ph on d.OWNER_ID = ph.REFERENCE where d.doc_description like '%***%' or d.doc_description like '%hist%' or d.doc_name like '%hist%' 
select count(*) from documents d left join PROP_PERSON_HIST ph on d.OWNER_ID = ph.REFERENCE
------------------

update candidate_document
set candidate_id = (
	select ca.id, cd2.candidate_externalid, cd2.uploaded_filename
	from candidate_document_2 cd2
	left join candidate ca on cast(cd2.candidate_externalid as varchar(100)) = cast(ca.external_id as varchar(100))
	left join candidate_document cd on cd2.uploaded_filename = cd.uploaded_filename
	);

with t as (
	select ca.id as candidate_id, cd2.uploaded_filename as uploaded_filename
	from candidate_document_2 cd2
	left join candidate ca on cast(cd2.candidate_externalid as varchar(100)) = cast(ca.external_id as varchar(100))
	left join candidate_document cd on cd2.uploaded_filename = cd.uploaded_filename
)
update candidate_document
set candidate_id = t.candidate_id
from t
where candidate_document.uploaded_filename = t.uploaded_filename ;
--where cast(t.uploaded_filename as varchar(100)) = cast(cd.uploaded_filename as varchar(100)) ;
--left join candidate_document_2 as cd2 on cd2.uploaded_filename = cd.uploaded_filename
--where cast(cd.uploaded_filename as varchar(100)) = cast(ca.external_id as varchar(100));

--===================
	select ca.id as candidate_id, cd2.uploaded_filename as uploaded_filename
	from candidate_document_2 cd2
	left join candidate ca on cast(cd2.candidate_externalid as varchar(100)) = cast(ca.external_id as varchar(100))
	left join candidate_document cd on cd2.uploaded_filename = cd.uploaded_filename
	where cd2.uploaded_filename = '1182309.rtf'
---------------------
update candidate_document_2 cd2
set candidate_id = c.id
from candidate c
where cd2.candidate_externalid = c.external_id::int

select c.id,c.external_id from candidate c
left join candidate_document_2 cd2 on cd2.candidate_externalid = c.external_id::int
-------------------
select * from candidate_document_2 cd2, candidate_document cd where cast(cd.uploaded_filename as varchar(100)) = cast(cd2.uploaded_filename as varchar(100))

update candidate_document as cd
set candidate_id = cd2.candidate_id
from candidate_document_2 cd2
where cast(cd.uploaded_filename as varchar(100)) = cast(cd2.uploaded_filename as varchar(100))
and cd2.candidate_id is not null;

-----------------

select --count(*)
	ca.id,ca.first_name,ca.last_name,ca.external_id,ca.email, cd.*
from candidate ca
left join candidate_document cd on ca.id = cd.candidate_id
where cd.uploaded_filename is null

value:8666
null:14540

----------------------
select * 
from candidate
where email like '%@staranise.com'
--id in (34363,34365,34361,34359,34360,34358)

update candidate
set email = concat('NOEMAIL.',external_id,'@staranise.com')
where email like '%@vincere.io'
----------------------------------------------------

select distinct CONTACT from PROP_X_CLIENT_CON --contact-externalId
select count(*) from PROP_X_CLIENT_CON --contact-externalId
select CONTACT   from PROP_X_CLIENT_CON group by CONTACT having count(*) > 1 order by CONTACT
select CLIENT   from PROP_X_CLIENT_CON group by CLIENT having count(*) > 1 order by CLIENT
select top 100 *   from PROP_X_CLIENT_CON

select * from PROP_X_CLIENT_CON WHERE CONTACT = 430093 order by CLIENT


--------------------------
select PERSON_ID from PROP_PERSON_GEN group by PERSON_ID having count(*) > 1 order by PERSON_ID
select REFERENCE from PROP_PERSON_GEN group by REFERENCE having count(*) > 1 order by REFERENCE
select top 100 * from PROP_PERSON_GEN  WHERE PERSON_ID = 200002
select top 100 * from PROP_PERSON_GEN  WHERE REFERENCE = 394899 order by REFERENCE

--candidate-homePhone	PROP_TELEPHONE.TEL_NUMBER	
select * from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Home' --and TEL.REFERENCE = <<PROP_PERSON_GEN.REFERENCE>>
--candidate-mobile	PROP_TELEPHONE.TEL_NUMBER	
select  * from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Mobile' --and TEL.REFERENCE = <<PROP_PERSON_GEN.REFERENCE>>
--candidate-phone	PROP_TELEPHONE.TEL_NUMBER	
select  * from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Work' --and TEL.REFERENCE = <<PROP_PERSON_GEN.REFERENCE>>
--candidate-workPhone		
select  * from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Work' --and TEL.REFERENCE = <<PROP_PERSON_GEN.REFERENCE>>


-----------
left join PROP_EMAIL email ON pg.REFERENCE = email.REFERENCE -- candidate-email & candidate-workEmail
left join (SELECT REFERENCE, EMAIL_LINK = STUFF((SELECT DISTINCT ', ' + EMAIL_LINK FROM PROP_EMAIL b WHERE b.EMAIL_LINK like '%@%' and b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM PROP_EMAIL a GROUP BY REFERENCE) mail ON ccc.CONTACT = mail.REFERENCE

-------------
select REFERENCE,DESCRIPTION from PROP_LOCATIONS LOCATION INNER JOIN MD_MULTI_NAMES MN ON MN.ID = LOCATION.LOCATION --WHERE LOCATION.REFERENCE = <<PROP_PERSON_GEN.REFERENCE>>

SELECT REFERENCE,DESCRIPTION FROM PROP_PERSON_GEN PERSON_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PERSON_GEN.CURRENCY
SELECT MN.DESCRIPTION as Currency FROM PROP_PERSON_GEN PERSON_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PERSON_GEN.CURRENCY

select * from MD_MULTI_NAMES
----------------------------

select * from PROP_QUALS QUAL INNER JOIN
MD_MULTI_NAMES MN ON MN.ID = QUAL.QUAL
WHERE QUAL.REFERENCE = 395007

select * from PROP_PERSON_GEN WHERE REFERENCE = 395007

select * from PROP_EDU_ESTAB



select CONTACT, max(BISUNIQUEID) as CLIENT from PROP_X_CLIENT_CON group by CONTACT

--select top 100 * from PROP_X_CLIENT_CON order by client

SELECT REFERENCE, EMAIL_LINK = STUFF((SELECT DISTINCT ', ' + EMAIL_LINK FROM PROP_EMAIL b WHERE b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM PROP_EMAIL a where REFERENCE = 395696 or REFERENCE = 395701 GROUP BY REFERENCE 
select top 100 * from PROP_EMAIL where REFERENCE = 395696 or REFERENCE = 395701

select top 100 * from PROP_CAND_PREF cp

select top 100 REFERENCE,SALARY_CURR,SALARY_DES from PROP_CAND_PREF cp
select REFERENCE from PROP_CAND_PREF group by REFERENCE having count(*) > 1 order by REFERENCE


select * from PROP_CLIENT_GEN --.CLIENT_ID
----------------------------
select COUNT(*)
select JOB
from PROP_X_CLIENT_JOB

select count(JOB) from PROP_X_CLIENT_JOB group by JOB having count(*) > 1 order by JOB

-------------------------
Industry
select distinct DESCRIPTION from PROP_IND_EXP IND_EXP INNER JOIN
MD_MULTI_NAMES MN ON MN.ID = IND_EXP.IND_EXP
--WHERE IND_EXP.REFERENCE = <<PROP_PERSON_GEN.REFERENCE>>

Qualification		
select  distinct DESCRIPTION from PROP_QUALS QUAL INNER JOIN
MD_MULTI_NAMES MN ON MN.ID = QUAL.QUAL
WHERE QUAL.REFERENCE = <<PROP_PERSON_GEN.REFERENCE>>

Skill		
select  distinct DESCRIPTION from PROP_SKILLS SKILL INNER JOIN
MD_MULTI_NAMES MN ON MN.ID = SKILL.SKILL
WHERE SKILL.REFERENCE = <<PROP_PERSON_GEN.REFERENCE>>

Location		
select  distinct DESCRIPTION from PROP_LOCATIONS LOCATION INNER JOIN
MD_MULTI_NAMES MN ON MN.ID = LOCATION.LOCATION
WHERE LOCATION.REFERENCE = <<PROP_PERSON_GEN.REFERENCE>>

General Notes
select top 12 * from DOCUMENTS where DOC_CATEGORY = 6532839 and OWNER_ID = <<PROP_PERSON_GEN.REFERENCE>>

SELECT top 123 OWNER_ID, cast(concat(DOC_ID,'.',FILE_EXTENSION) as nvarchar(max)) as  att from DOCUMENTS
SELECT top 123 OWNER_ID, STRING_AGG(cast(concat(DOC_ID,'.',FILE_EXTENSION) as nvarchar(max)),',') WITHIN GROUP (ORDER BY DOC_ID) att from DOCUMENTS GROUP BY OWNER_ID
SELECT top 123 OWNER_ID, STRING_AGG( DOC_ID,'|| ' ) WITHIN GROUP (ORDER BY dateadded) as name from DOCUMENTS GROUP BY OWNER_ID

select * from PROP_EDU_ESTAB

-----------------------------------------------------------------------------------------------------------------
select top 100 *  from MD_MULTI_NAMES
select DISTINCT DESCRIPTION from MD_MULTI_NAMES where DESCRIPTION like 
--'%level%'
'%Of-Counsel/%'
'%Paralegal%'

id = 6920549
--id = 1303879 -- Married
--id = 1303878 -- Single
--id in (300015935,300015936,300015937,300015938,300015939,300015940) -- <1,>15,10-15,1-3,3-5,5-10 --WORK_YEAR
--id in (300016102,300016103,300016104) -- No,Oversea Study,Oversea Working --OVERSEAS
--id in (0,300404300,300404301,300404302,300404303,300404304,300404305) --RATING

----------------------------------------------------------------
select distinct DESCRIPTION from PROP_IND_EXP IND_EXP INNER JOIN MD_MULTI_NAMES MN ON MN.ID = IND_EXP.IND_EXP --WHERE IND_EXP.REFERENCE = <<PROP_PERSON_GEN.REFERENCE>> -- Industry
select distinct DESCRIPTION from PROP_JOB_CAT JOB_CAT INNER JOIN MD_MULTI_NAMES MN ON MN.ID = JOB_CAT.JOB_CATEGORY --WHERE JOB_CAT.REFERENCE = <<PROP_PERSON_GEN.REFERENCE>> --Job Category
select distinct DESCRIPTION from PROP_SUB_CAT SUB_CAT INNER JOIN MD_MULTI_NAMES MN ON MN.ID = SUB_CAT.SUB_CAT --WHERE SUB_CAT.REFERENCE = <<PROP_PERSON_GEN.REFERENCE>> --Sub Category
select distinct DESCRIPTION from PROP_QUALS QUAL INNER JOIN MD_MULTI_NAMES MN ON MN.ID = QUAL.QUAL --WHERE QUAL.REFERENCE = <<PROP_PERSON_GEN.REFERENCE>> -- Qualification
select distinct DESCRIPTION from PROP_SKILLS SKILL INNER JOIN MD_MULTI_NAMES MN ON MN.ID = SKILL.SKILL --WHERE SKILL.REFERENCE = <<PROP_PERSON_GEN.REFERENCE>>" -- Skills
select distinct DESCRIPTION from PROP_LOCATIONS LOCATION INNER JOIN MD_MULTI_NAMES MN ON MN.ID = LOCATION.LOCATION --WHERE LOCATION.REFERENCE = <<PROP_PERSON_GEN.REFERENCE>> -- Candidate's Location
-------------------------
select distinct DESCRIPTION from LK_CATEGORIES_ROLE cr INNER JOIN MD_MULTI_NAMES MN ON MN.ID = cr.CATEGORY_ID
select distinct DESCRIPTION from LK_CATEGORIES_ROLE cr INNER JOIN MD_MULTI_NAMES MN ON MN.ID = cr.ROLE_ID
--------------------
SELECT DISTINCT MN.DESCRIPTION from dbo.PROP_JOB_GEN jg INNER JOIN MD_MULTI_NAMES MN ON MN.ID = jg.STATUS
SELECT DISTINCT MN.DESCRIPTION from dbo.PROP_JOB_GEN jg INNER JOIN MD_MULTI_NAMES MN ON MN.ID = jg.REASON
SELECT DISTINCT MN.DESCRIPTION from dbo.PROP_JOB_GEN jg INNER JOIN MD_MULTI_NAMES MN ON MN.ID = jg.JOB_TYPE --job-type
SELECT DISTINCT MN.DESCRIPTION from dbo.PROP_JOB_GEN jg INNER JOIN MD_MULTI_NAMES MN ON MN.ID = jg.cons1
--------------
select * from prop_client_gen
select DISTINCT MN.DESCRIPTION from dbo.PROP_CLIENT_GEN cg INNER JOIN MD_MULTI_NAMES MN ON MN.ID = cg.LOCATION --company location
select DISTINCT MN.DESCRIPTION from dbo.PROP_CLIENT_GEN cg INNER JOIN MD_MULTI_NAMES MN ON MN.ID = cg.status --company status
select DISTINCT MN.DESCRIPTION from dbo.PROP_CLIENT_GEN cg INNER JOIN MD_MULTI_NAMES MN ON MN.ID = cg.source --company source
select DISTINCT MN.DESCRIPTION from dbo.PROP_CLIENT_GEN cg INNER JOIN MD_MULTI_NAMES MN ON MN.ID = cg.law_type --company law_type
--------------
select * from dbo.PROP_PERSON_GEN

select distinct PQE from PROP_PERSON_GEN ORDER BY PQE
select distinct PQE_YEAR2 from PROP_PERSON_GEN order by PQE_YEAR2
select PQE_YEAR,DESCRIPTION from PROP_PERSON_GEN PG INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PG.PQE_YEAR ORDER BY PQE_YEAR
--------------
--------------
SELECT ENTITY_ID
       , JE.creation_date as 'Date'
       , bo.description as 'Workflow Name'
       , eg.name as 'User'
       , [dbo].[udf_StripHTML](JE.J_NOTES) as 'Notes'
       , pg1.fullname as 'Permanent Candidate'
       , pg2.fullname as 'Contact External'   
       --, '688882' as 'External Interview'
       , cg.name as 'Client'
       --, '1122965' as 'Progress'
       , jg.job_title as 'Contract Job'
       --,  'No documents available' as 'Documents'
       , JE.ENTITY_ID_1, JE.ENTITY_ID_2, JE.ENTITY_ID_3, JE.ENTITY_ID_4, JE.ENTITY_ID_5, JE.ENTITY_ID_6,  JE.ROLE_ID_1,  JE.ROLE_ID_2,  JE.ROLE_ID_3,  JE.ROLE_ID_4,  JE.ROLE_ID_5,  JE.ROLE_ID_6
FROM LK_ENTITIES_JOURNAL J INNER JOIN JOURNAL_ENTRIES JE ON JE.ID = J.JOURNAL_ID 
left join prop_employee_gen eg on eg.user_ref = je.creator_id
left join (select ID, description from MD_MULTI_NAMES MN where LANGUAGE = 10010) bo on bo.id = je.bo_id
left join dbo.PROP_PERSON_GEN pg1 on pg1.reference = je.entity_id_1
left join dbo.PROP_PERSON_GEN pg2 on pg2.reference = je.entity_id_2
left join PROP_CLIENT_GEN cg on cg.reference = je.entity_id_4
left join PROP_JOB_GEN jg on jg.reference = je.entity_id_6
WHERE --JE.J_NOTES != '' and JE.J_NOTES is not null --J.ENTITY_ID = <<PROP_JOB_GEN.REFERENCE>>
 J.ENTITY_ID = 116674384916

SELECT ENTITY_ID ,JE.J_NOTES 
FROM LK_ENTITIES_JOURNAL J INNER JOIN JOURNAL_ENTRIES JE ON JE.ID = J.JOURNAL_ID where JE.J_NOTES != '' and JE.J_NOTES is not null and ENTITY_ID = 395016  
group by ENTITY_ID having count(*) > 1

select distinct entity_type from LK_ENTITIES_JOURNAL
select pg.REFERENCE, pg.title, mn.DESCRIPTION from LK_ENTITIES_JOURNAL pg INNER JOIN MD_MULTI_NAMES MN ON MN.ID = pg.BI_ID where MN.ID is not null and LANGUAGE = 10010)
select * from MD_MULTI_NAMES where id = 6975832 TYPE in ('E','U')

with tmp (ENTITY_ID,notes) as (SELECT ENTITY_ID
  	, STUFF((
              SELECT distinct ', ' + JE.J_NOTES --+ REPLACE(JE.J_NOTES COLLATE Latin1_General_BIN, char(26), '') --REPLACE(ISNULL(NOTE, ''), CHAR(26), '')
              FROM LK_ENTITIES_JOURNAL J INNER JOIN JOURNAL_ENTRIES JE ON JE.ID = J.JOURNAL_ID WHERE JE.J_NOTES != '' and JE.J_NOTES is not null and ENTITY_ID = a.ENTITY_ID
              FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') As DESCRIPTION
        FROM LK_ENTITIES_JOURNAL as a GROUP BY a.ENTITY_ID)
select * from tmp where ENTITY_ID = 116658944942

with tmp (ENTITY_ID,notes) as (SELECT ENTITY_ID
        , STUFF((
              SELECT DISTINCT ', ' + JE.J_NOTES 
              FROM LK_ENTITIES_JOURNAL J INNER JOIN JOURNAL_ENTRIES JE ON JE.ID = J.JOURNAL_ID where JE.J_NOTES != '' and JE.J_NOTES is not null and ENTITY_ID = a.ENTITY_ID  
              FOR XML PATH ('')), 1, 1, '')  AS notes 
              FROM LK_ENTITIES_JOURNAL as a GROUP BY a.ENTITY_ID)
select * from tmp where ENTITY_ID = 116658944942 
      

        


select top 100 *
from PROP_PERSON_GEN pg 
left join (Select ASSIG_CAND.CANDIDATE,ASSIG_GEN.START_DT,ASSIG_GEN.END_DT,ASSIG_GEN.JOB_TITLE,ASSIG_GEN.PRV_CO from PROP_X_ASSIG_CAND ASSIG_CAND INNER JOIN PROP_ASSIG_GEN ASSIG_GEN  ON ASSIG_GEN.REFERENCE = ASSIG_CAND.ASSIGNMENT INNER JOIN MD_MULTI_NAMES MN ON MN.ID = ASSIG_GEN.ASSIG_TYPE WHERE MN.DESCRIPTION = 'Previous Work') wh on WH.CANDIDATE = PG.REFERENCE
WHERE PG.REFERENCE IN (204327,221109,203951,216074,220598)

----------------
select count(*) from bulk_upload_detail where bulk_upload_external_id ilike '92d5ae54-25c9-45b0-805e-9d8fdc34b75d';
delete from bulk_upload_detail where bulk_upload_external_id ilike '92d5ae54-25c9-45b0-805e-9d8fdc34b75d';
select * from bulk_upload_document_mapping;
select count(*) from candidate_document;
delete from candidate_document;
select count(*) from company where deleted_timestamp is null;
delete from company where deleted_timestamp is not null;
select * from contact where deleted_timestamp is not null;
select count(*) from contact where id < 10000;
---------------
truncate company cascade
truncate bulk_upload_document_mapping cascade

==============

SELECT c.first_name,c.last_name, bud.id, bud.entity_id, bud.document_bulk_upload_external_id, bud.entity_type, bud.file_name, -- select c.id, c.first_name,c.last_name, bud.* 
FROM bulk_upload_document_mapping bud
left join candidate c on c.id = bud.entity_id
left join candidate_document cd on bud.entity_id = cd.id
where bud.entity_type LIKE 'CANDIDATE' and cd.id is not null and cd.candidate_id != '0' and c.first_name like '%Gary Kwong-Fung%'

select * from candidate_document_tmp where candidate_id = 510850;
select candidate_id,c.first_name,c.last_name, count(*) from candidate_document cd,candidate c where cd.candidate_id=c.id group by candidate_id, first_name, last_name;

select * from bulk_upload_document_mapping where entity_id = 88724;
select * from bulk_upload

select * from bulk_upload_detail where entity_type = 'UPLOAD_FILE' --'CANDIDATE'
and bulk_upload_external_id = 'af722b97-975b-4be6-b8b1-d9213cf4c17b' --'bb194164-5e6b-4ef0-a4f3-ae3a838f16f1'
and entity_id = 88724;

insert into bulk_upload_detail(bulk_upload_external_id, entity_id, entity_type) values ('bb194164-5e6b-4ef0-a4f3-ae3a838f16f1',88724,'CANDIDATE')
set bulk_upload_external_id = 'bb194164-5e6b-4ef0-a4f3-ae3a838f16f1', entity_id = 88724 , entity_type = 'CANDIDATE'


select id,external_id from candidate where id in (108879,94949,94946,94909,94832,91578,94916,94921,94910,94918) --external_id::int = 510850 

insert into bulk_upload_document_mapping (entity_id,entity_type,file_name,document_type)
--set entity_id = 71194, entity_type = 'CANDIDATE', file_name = '1063482.pdf', document_type = 'resume'
select ca.id,'CANDIDATE',cdt.uploaded_filename, 'resume' --,cdt.document_type , ca.external_id
from candidate_document_tmp cdt
left join candidate ca 		on ca.external_id::int = cdt.candidate_id where ca.id not in (108879,94949,94946,94909,94832,91578,94916,94921,94910,94918) 

select count(*) from bulk_upload_document_mapping budm left join bulk_upload_detail bud on bud.entity_id = budm.entity_id

update bulk_upload_detail
set bulk_upload_external_id = '0e84f035-532c-4345-9328-91cec564377e' where bulk_upload_external_id = 'f177c96f-9146-4c29-aafe-baddf47b88b6'
from bulk_upload_detail bud left join bulk_upload_document_mapping budm on bud.entity_id = budm.entity_id where budm.entity_type = 'CANDIDATE' and bulk_upload_external_id = 'f177c96f-9146-4c29-aafe-baddf47b88b6' 

 --where bud.entity_id in (108879,94949
,94946
,94909,94832,91578,94916,94921,94910,94918)
select entity_id,entity_type,file_name,document_type from bulk_upload_document_mapping where entity_id = 108879
select * from bulk_upload_detail where entity_id = 108879
select entity_id,bulk_upload_external_id, entity_type from bulk_upload_detail where entity_id = 108879

where bud.entity_type LIKE 'CANDIDATE' and cd.id is not null and cd.candidate_id != '0' and c.first_name like '%Gary Kwong-Fung%'

------------
select * from contact_comment order by id desc

insert into contact_comment(contact_id,user_id,comment_content) --values(78784, -10, 'ABC')
select c.id,-10,d.document --,d.doc_id
from DOCUMENTS d
left join contact c on d.owner_id::int = c.external_id::int
where d.DOC_CATEGORY in (6532841) and d.document != '' and d.document is not null and c.external_id is not null --16861
order by d.updated_date desc;


SELECT * FROM LICENSE ;
UPDATE LICENSE SET VALUE = 'Staranise', KEY='companyName' WHERE KEY = 'Next Link';

-----------
delete from candidate_tmp_note where id > 0
select * from candidate_tmp_note where left(external_id,5)::int = 649429
select c.id,c.external_id,c.note, c.candidate_owner_json from candidate c

select c.id,c.note,  ctn.external_id, ctn.note 
--update candidate set note = ctn.note
--select count(*)
from candidate c
left join candidate_tmp_note ctn on c.external_id::int = ctn.external_id::int
where --ctn.external_id is not null
c.external_id::int in (397788,395041,848648)
--where c.note is not null

update candidate set note = '' from candidate c

select count(*) from candidate
select count(*) from candidate_tmp_note --21156

select count(distinct external_id) from candidate --20608
select count(distinct external_id) from candidate_tmp_note --21156

select c.id,c.note --,  ctn.external_id, ctn.note 
from candidate c
where c.external_id not in (select external_id from candidate_tmp_note)

select distinct candidate_id,count(*) from candidate_document group by candidate_id
select * from candidate_document


update candidate_document cd
set candidate_id = (
select ca.id, c.candidate_id, c.uploaded_filename from candidate ca
--select ca.id 
left join candidate_document_tmp cdt on cdt.candidate_id = ca.external_id::int
where cd.uploaded_filename = cdt.uploaded_filename
)
where c.candidate_id = 104287

select * from candidate_document where uploaded_filename = '_null.doc' or id > 160000 candidate_id in (104287);
select count (*) delete from candidate_document where candidate_id = 104287;
select * from bulk_upload_document_mapping where entity_type = 'CANDIDATE';
insert into bulk_upload_document_mapping(entity_id,entity_type,file_name,document_type) values ('111997','CANDIDATE','1540872.pdf','resume')
insert into bulk_upload_document_mapping(entity_id,entity_type,file_name,document_type) values ('111997','CANDIDATE','1540873.docx','resume')
select ca.id,cdt.* from candidate ca left join candidate_document_tmp cdt on cdt.candidate_id = ca.external_id::int where ca.id = 111997
insert into candidate_document (candidate_id,uploaded_filename,saved_filename,document_type) values ('111997','1540872.pdf','1540872.pdf','resume')
delete from candidate_document where id = 162021
=================

SELECT candidate_id, uploaded_filename, saved_filename, document_type FROM candidate_document
where candidate_id != '0'

CREATE TABLE
    candidate_document_tmp
    (
        candidate_id INTEGER DEFAULT 0,
        uploaded_filename CHARACTER VARYING(400),
        saved_filename CHARACTER VARYING(400),
        document_type CHARACTER VARYING(100) DEFAULT 'unknown'::CHARACTER VARYING NOT NULL
     );
     
-----------------------     
with
 resume (OWNER_ID, DOC_ID) as  (SELECT OWNER_ID, cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY = 6532857 AND FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip'))

select
	  pg.REFERENCE as 'candidate-externalId'
	, resume.DOC_ID as 'uploaded_filename'
	,'resume' as 'document_type'
-- select count(*)
-- select top 100 *
from PROP_PERSON_GEN pg --21158 rows
left join resume on pg.REFERENCE = resume.OWNER_ID
where resume.DOC_ID is not null

 -- delete from candidate_document_tmp
 
insert into candidate_document(candidate_id,uploaded_filename,saved_filename,document_type)
select cdt.candidate_id
       ,cdt.uploaded_filename
       ,cdt.saved_filename
       ,cdt.document_type
from candidate_document_tmp cdt

update candidate_document
set candidate_id = ca.id
-- select ca.id, cdt.candidate_id , ca.external_id::int
from candidate ca
left join candidate_document_tmp cdt on cdt.candidate_id = ca.external_id::int where cdt.candidate_id is not null

================================
select * from bulk_upload

select * delete from bulk_upload_document_mapping where entity_type = 'CANDIDATE' --entity_id = 93042

select * from bulk_upload_document_mapping where --entity_type = 'COMPANY'
entity_id = 104287;
--file_name = '_null.doc';
-- delete from bulk_upload_document_mapping where id = 72670
select * from bulk_upload_detail where 
--entity_id = 93042 
file_name = '887614.DOC';
 
select * from candidate_document where saved_filename = '1427186.docx';
select * from candidate_document cd left join bulk_upload_detail bud on bud.file_name = cd.uploaded_filename left join bulk_upload_document_mapping budm on budm.file_name = cd.uploaded_filename
where cd.id = 112008 --104287
---------------
select * from bulk_upload_document_mapping where file_name = '1427186.docx';
insert into bulk_upload_document_mapping(entity_id,entity_type,file_name,document_type) values ('104287','CANDIDATE','1427186.docx','resume')
insert into bulk_upload_document_mapping(entity_id,entity_type,file_name,document_type,bulk_upload_detail_id) values ('104287','CANDIDATE','998774.docx','resume','247943')
---------------
select * from bulk_upload_detail where file_name = '1427186.docx' -- entity_type = 'UPLOAD_FILE';
select distinct bulk_upload_external_id from bulk_upload_detail
update bulk_upload_detail set entity_id = 93042 where file_name = '887614.DOC'
update bulk_upload_detail set bulk_upload_external_id = 'bb194164-5e6b-4ef0-a4f3-ae3a838f16f1' where bulk_upload_external_id != 'af722b97-975b-4be6-b8b1-d9213cf4c17b'
insert into bulk_upload_detail(entity_id,bulk_upload_external_id,entity_type) values ('93042','b706f902-0c5b-4985-979b-01c57feeb88f','CANDIDATE')
set bulk_upload_external_id = 'bb194164-5e6b-4ef0-a4f3-ae3a838f16f1', entity_id = 88724 , entity_type = 'CANDIDATE'
-----------------
select id,candidate_id,uploaded_filename,document_type,* from candidate_document where candidate_id in (93042,107190,111997);
-- select count (*) delete from candidate_document where candidate_id in (104287);
-- select count(candidate_id) from candidate_document where document_type = 'resume';
-- select distinct candidate_id from candidate_document;
-- insert into candidate_document (candidate_id,uploaded_filename,document_type) values ('93042','1427186.docx','resume')
-- update bulk_upload_document_mapping set bulk_upload_detail_id = 247938 where bulk_upload_detail_id = 247938
--upload.do-- select * delete from bulk_upload_detail where entity_id = 107190 -- entity_type = 'CANDIDATE';
-- insert into bulk_upload_detail(bulk_upload_external_id, entity_id, entity_type) values ('b706f902-0c5b-4985-979b-01c57feeb88f',93042,'CANDIDATE')
select * from bulk_upload_document_mapping budm left join candidate_document cd on cd.uploaded_filename = budm.file_name where entity_type = 'CANDIDATE';

update bulk_upload_document_mapping budm
set document_id = cd.id
from candidate_document cd where cd.uploaded_filename = budm.file_name and budm.entity_type = 'CANDIDATE' and  budm.entity_id = 111997 

delete from candidate_document where candidate_id = 93042
insert into candidate_document(candidate_id,uploaded_filename,document_type)
select ca.id,cdt.uploaded_filename,cdt.document_type from candidate ca left join candidate_document_tmp cdt on cdt.candidate_id = ca.external_id::int where ca.id = 93042

update candidate_document c
set candidate_id = (
select ca.id from candidate ca
left join candidate_document_tmp cdt on cdt.candidate_id = ca.external_id::int
where c.uploaded_filename = cdt.uploaded_filename
)
where c.candidate_id = 104287
select distinct candidate_id from candidate_document

select ca.id,cdt.* from candidate ca left join candidate_document_tmp cdt on cdt.candidate_id = ca.external_id::int where ca.id = 104287
==========================
select c.id,c.note,  ctn.external_id, ctn.note 
--
update candidate c
set note = (select ctn.note from candidate_tmp_note ctn where c.external_id::int = ctn.external_id::int )
where c.external_id::int in (397788,395041,848648)

from candidate c
left join candidate_tmp_note ctn on c.external_id::int = ctn.external_id::int

select distinct c.note from candidate c

select c.id,c.external_id,c.note, c.candidate_owner_json from candidate c
--select count(*) from candidate c
where c.external_id::int in (397788,395041,848648)

where c.note like '%Salu%'

select c.id,c.note,  ctn.external_id, ctn.note 
--update candidate
--set note = ctn.note
from candidate_tmp_note ctn 
left join candidate c on c.external_id = ctn.external_id
where c.note is not null

select count(*) from candidate
select count(*) from candidate_document --15437

select count(distinct external_id) from candidate --20608
select count(distinct external_id) from candidate_tmp_note --21156

select * from candidate_document  where uploaded_filename like '1000015%' or candidate_id = 95014
select distinct candidate_id,count(*) from candidate_document group by candidate_id

insert into candidate_document (candidate_id,uploaded_filename,saved_filename,document_type)
select ca.id,cdt.uploaded_filename, cdt.saved_filename, cdt.document_type from candidate_document_tmp cdt
left join candidate ca on ca.external_id::int = cdt.candidate_id::int where ca.external_id is not null



select * from candidate_document where candidate_id = 95014
============
with
--resume (OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY in (6532839,31159) AND FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip') and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
resume (OWNER_ID, DOC_ID) as (SELECT OWNER_ID, cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY in (6532839,31159) AND FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip') )


--, tmp as (
select
	 pg.REFERENCE as 'candidate-externalId'
	 , replace(resume.DOC_ID,'.txt','.rtf') as 'candidate-resume'

from PROP_PERSON_GEN pg --21158 rows
left join resume on pg.REFERENCE = resume.OWNER_ID
where resume.DOC_ID is not null
--)
--select count('candidate-externalId') from tmp
 
 ================
 select * from invoice;
select * from candidate_user_added_document;
select * from careersite_terms_configuration;
select * from  company_legal_document_file_desc;
select * from import_files_temp;
select * from offer_document;
=============
select * from documents where owner_id = 397191