
/*
with tmp_1(userID, email) as 
(select userID, REPLACE(ISNULL(email,'') + ',' + ISNULL(email2,'') + ',' + ISNULL(email3,''), ',,', ',') as email
from bullhorn1.BH_UserContact
 )
 --select * from tmp_1
 --select userID, email, CHARINDEX(email,',',0) from tmp_1
 , tmp_2(userID, email) as (
select userID, CASE WHEN CHARINDEX(',',email,1) = 1 THEN RIGHT(email, len(email)-1)
	ELSE email END as email
from tmp_1
)
 , tmp_3(userID, email) as (
select userID, CASE WHEN CHARINDEX(',',email,len(email)) = len(email) 
	THEN LEFT(email, CASE WHEN len(email) < 1 THEN 0 ELSE len(email) - 1 END)
	ELSE email END as email
from tmp_2
)

, Note as (
select jobPostingID
, concat('BH Job ID:',jobPostingID
,'Employment Type: ',employmentType,char(10)
,'Priority: ',type,char(10)
, feeArrangement
, externalCategoryID
, publishedCategoryID
, skills
, yearsRequired
) as note
from bullhorn1.BH_JobPosting
)
*/


/* publicdoc_note (OWNER_ID, DOC_ID, NOTE) as (SELECT D.OWNER_ID, D.DOC_ID, DC.NOTE from DOCUMENTS D left join DOCUMENT_CONTENT DC on DC.DOC_ID = d.DOC_ID WHERE DOC_CATEGORY = 6532897 AND FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip'))
--select top 3 * from publicdoc_note where OWNER_ID = 394903
, publicdoc(OWNER_ID, NOTE) as (SELECT OWNER_ID
	, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
			left(STUFF((SELECT char(10) + NOTE from publicdoc_note WHERE NOTE != '' and NOTE is not null and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 0, ''),32765)
			,'&#x00;',''),'&#x01;',''),'&#x02;',''),'&#x03;',''),'&#x04;',''),'&#x05;',''),'&#x06;',''),'&#x07;',''),'&#x08;',''),'&#x09;','')
			,'&#x0A;',''),'&#x0B;',''),'&#x0C;',''),'&#x0D;',''),'&#x0E;',''),'&#x0F;','')
			,'&#x10;',''),'&#x11;',''),'&#x12;',''),'&#x13;',''),'&#x14;',''),'&#x15;',''),'&#x16;',''),'&#x17;',''),'&#x18;',''),'&#x19;','')
			,'&#x1a;',''),'&#x1b;',''),'&#x1c;',''),'&#x1D;',''),'&#x1E;',''),'&#x1f;',''),'&#x7f;',''),'?C','C')
			,'amp;',''),'?(','('),'?T','T') AS doc
	FROM publicdoc_note as a GROUP BY a.OWNER_ID)
--select top 20 * from publicdoc where NOTE is not null --and OWNER_ID = 394903

, internaldoc_note (OWNER_ID, DOC_ID, NOTE) as (SELECT D.OWNER_ID, D.DOC_ID, DC.NOTE from DOCUMENTS D left join DOCUMENT_CONTENT DC on DC.DOC_ID = d.DOC_ID WHERE DOC_CATEGORY = 6532850 AND FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip'))
--select top 100 * from internaldoc_note where OWNER_ID = 394903
, internaldoc(OWNER_ID, NOTE) as (SELECT OWNER_ID
	, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
			left(STUFF((SELECT char(10) + 'NOTE: ' + NOTE from internaldoc_note WHERE NOTE != '' and NOTE is not null and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 0, ''),32765)
			,'&#x00;',''),'&#x01;',''),'&#x02;',''),'&#x03;',''),'&#x04;',''),'&#x05;',''),'&#x06;',''),'&#x07;',''),'&#x08;',''),'&#x09;','')
			,'&#x0A;',''),'&#x0B;',''),'&#x0C;',''),'&#x0D;',''),'&#x0E;',''),'&#x0F;','')
			,'&#x10;',''),'&#x11;',''),'&#x12;',''),'&#x13;',''),'&#x14;',''),'&#x15;',''),'&#x16;',''),'&#x17;',''),'&#x18;',''),'&#x19;','')
			,'&#x1a;',''),'&#x1b;',''),'&#x1c;',''),'&#x1D;',''),'&#x1E;',''),'&#x1f;',''),'&#x7f;',''),'?C','C')
			,'amp;',''),'?(','('),'?T','T') AS doc
	FROM internaldoc_note as a GROUP BY a.OWNER_ID)
--select top 20 * from internaldoc where NOTE is not null --and OWNER_ID = 394903

--, publicdoc(OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY =6532897 AND FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip') and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
--, internaldoc(OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY =6532850 AND FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip') and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
--select * from doc
*/

with
--JOB DUPLICATION REGCONITION
job (JOB, contactId, companyid, JOB_TITLE, starDate, rn) as (
        SELECT  cj.JOB
                  , iif(cj.CONTACT is null, 'default', convert(varchar(200),cj.CONTACT)) as 'contactId'
                  , cj.CLIENT as 'companyid'
                  , iif(jg.JOB_TITLE is not null and jg.JOB_TITLE <> '', ltrim(rtrim(jg.JOB_TITLE)), 'No JobTitle') as JOB_TITLE
                , CONVERT(VARCHAR(10),jg.START_DT,120) as startDate
                , ROW_NUMBER() OVER(PARTITION BY cj.CONTACT, iif(jg.JOB_TITLE is not null and jg.JOB_TITLE <> '', ltrim(rtrim(jg.JOB_TITLE)), 'No JobTitle'), CONVERT(VARCHAR(10),jg.START_DT,120) ORDER BY cj.JOB) AS rn 
        from PROP_X_CLIENT_JOB cj  --8036 rows
        left join PROP_JOB_GEN jg on cj.JOB = jg.REFERENCE
        )
--select * from job where contactId like '%default%' or JOB in (192209,192213)
-- >>> CREATE DEFAULT CONTACT LIST FOR JOB <<< ---
--select distinct CompanyId as 'contact-companyId', concat('default',CompanyId) as 'contact-externalId', 'Default Contact' as 'contact-lastname' from job where contactId like 'default%' order by CompanyId desc

, owner as (select JOB_GEN.REFERENCE AS JOB_GEN_REFERENCE, JOB_GEN.JOB_TITLE, EMPLOYEE.REFERENCE AS EMPLOYEE_REFERENCE, EMPLOYEE.NAME as EMPLOYEE_NAME, tmp_email.EMAIL as EMAIL_ADD
	from PROP_JOB_GEN JOB_GEN 
	INNER JOIN PROP_OWN_CONS CONS ON JOB_GEN.REFERENCE = CONS.REFERENCE 
	INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID 
	INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT 
	left join tmp_email ON EMPLOYEE.REFERENCE = tmp_email.REFERENCE
	where CONFIG_NAME = 'Permanent')
--select * from owner

, doc(OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STRING_AGG( concat(cast(DOC_ID as varchar(max)),'.',FILE_EXTENSION),',') WITHIN GROUP (ORDER BY DOC_ID) DOC FROM DOCUMENTS GROUP BY OWNER_ID)
--DOC_CATEGORY: 6532922 --JOB DESCRIPTION

, skill as (select distinct REFERENCE,DESCRIPTION from PROP_SKILLS SKILL INNER JOIN MD_MULTI_NAMES MN ON MN.ID = SKILL.SKILL)
--select * from skill

, jobindsec as (SELECT distinct indsec.reference, string_agg(MN.DESCRIPTION,',') as indsec FROM PROP_IND_SECT indsec INNER JOIN MD_MULTI_NAMES MN ON MN.ID = indsec.industry where MN.ID is not null and LANGUAGE = 10010 group by indsec.reference)
--select * from jobindsec where rn = 1


select --top 10
	  cj.JOB as 'position-externalId', jg.JOB_ID as '#JOB_ID'
	, iif(job.contactId = 'default', concat('default',job.companyId), job.contactId) as 'position-contactId', pg.FIRST_NAME as '(contact-firstName)', pg.LAST_NAME as '(contact-lastName)' --, iif(cj.CONTACT is null, 'default', convert(varchar(200),cj.CONTACT))
	, cj.CLIENT as '(job-client)'
	, cg.NAME as '(company-name)'	
	--, jg.STATUS as '(STATUS)'
       , case when job.rn > 1 then concat(job.JOB_TITLE,' ',rn) else job.JOB_TITLE end as 'position-title' --, jg.JOB_TITLE as 'position-title'
	--, as 'position-headcount'
	--, jg.cons1, own.NAME as '(position-owners-name)'
	, case 
             when jobtype.DESCRIPTION in ('Contract','Lead Contract','Lead Temp','Temp Regular','Temp Shift') then 'CONTRACT'
             when jobtype.DESCRIPTION in ('Direct','Lead Direct Job') then 'PERMANENT'
	      end as 'position-type' --[PERMANENT, INTERIM_PROJECT_CONSULTING,TEMPORARY,CONTRACT, TEMPORARY_TO_PERMANENT]
	--, as 'position-employmentType'	
	, case 
	      when mn.DESCRIPTION = 'Angola, Kwanza' then 'USD'
	      when mn.DESCRIPTION = 'Euro' then 'EUR'
	      when mn.DESCRIPTION = 'Switzerland, Francs' then 'CHF'
	      when mn.DESCRIPTION = 'United Kingdom, Pounds' then 'GBP'
	      end as 'position-currency'
	--, pjg.SAL_FROM as 'position-actualSalary'
	, salary.SALARY_FROM as 'Salary From'
	, salary.SALARY_TO as 'Salary To'
	, own.EMAIL_ADD as 'position-owners'
--	, salaryto.SALARY_FROM as 'position-actualSalary'
	--, publicdoc.NOTE as 'position-publicDescription'
	--, internaldoc.NOTE as 'position-internalDescription'
--       , Stuff( + Coalesce('Min. Educ. Level: ' + NULLIF(convert(nvarchar(max),jg.JOB_ID), '') + char(10), '')
--                + Coalesce('Fixed Term: ' + NULLIF(convert(nvarchar(max),status.status), '') + char(10), '')
--                + Coalesce('Working Hours: ' + NULLIF(convert(nvarchar(max),pe.fee_perc), '') + char(10), '')
--                + Coalesce('OTE / Package: ' + NULLIF(convert(nvarchar(max),pe.calc_fee), '') + char(10), '')
--                + Coalesce('Bonus: ' + NULLIF(convert(nvarchar(max),pe.act_fee), '') + char(10), '')
--                , 1, 0, '') as 'position-internalDescription'
	, CONVERT(VARCHAR(10),jg.START_DT,120) as 'position-startDate'
	, CONVERT(VARCHAR(10),jg.END_DT,120) as 'position-endDate' --, convert(varchar(10),iif(START_DT = 'xyz',getdate()-1,dateClosed),120) as 'position-endDate'

       , Stuff( + Coalesce('Job ID: ' + NULLIF(convert(nvarchar(max),jg.JOB_ID), '') + char(10), '')
                + Coalesce('Owner: ' + NULLIF(convert(nvarchar(max),own.NAME), '') + char(10), '')
                + Coalesce('Status: ' + NULLIF(convert(nvarchar(max),status.status), '') + char(10), '')
                + Coalesce('Fee %: ' + NULLIF(convert(nvarchar(max),pe.fee_perc), '') + char(10), '')
                + Coalesce('Calculated Fee: ' + NULLIF(convert(nvarchar(max),pe.calc_fee), '') + char(10), '')
                + Coalesce('Actual Fee: ' + NULLIF(convert(nvarchar(max),pe.act_fee), '') + char(10), '')
                + Coalesce('Currency: ' + NULLIF(convert(nvarchar(max),mn.DESCRIPTION), '') + char(10), '')       
                + Coalesce('Updated Date: ' + NULLIF(convert(nvarchar(max),et.updateddate), '') + char(10), '')
                + Coalesce('Updated By: ' + NULLIF(convert(nvarchar(max),et.UPDATED_BY_NAME), '') + char(10), '')
                + Coalesce('Interview Date and Time: ' + NULLIF( concat(convert(nvarchar(max),convert(date,iv.IV_DATE)), CONVERT(VARCHAR,iv.IV_START,8)) , '') + char(10), '')
--                + Coalesce('Industry Sectors: ' + NULLIF(convert(nvarchar(max),jobindsec.indsec), '') + char(10), '')
--                + Coalesce('No. of CVs/Profiles Sent: ' + NULLIF(convert(nvarchar(max),jh.NO_CV_SENT), '') + char(10), '')
--                + Coalesce('No. of Int''s Arranged: ' + NULLIF(convert(nvarchar(max),jh.NO_IV_ARR), '') + char(10), '')
                --+ Coalesce('No. of Int''s Attended: ' + NULLIF(convert(nvarchar(max),jh.NO_IV_ATT), '') + char(10), '')
                , 1, 0, '') as 'position-note'
--	, concat(
--		concat('Job ID: ',cj.JOB,char(10))
--		, case when (owner.EMPLOYEE_NAME = '' or owner.EMPLOYEE_NAME is null) then '' else concat('Job Owner: ',owner.EMPLOYEE_NAME,char(10)) end
--		, iif(jobcategory.DESCRIPTION = '' or jobcategory.DESCRIPTION is null, '', concat('Job Category: ',jobcategory.DESCRIPTION,char(10)))
--		, iif(salaryto.SALARY_FROM is null, '', concat('Salary To: ',salaryto.SALARY_FROM,char(10))) --salaryto.SALARY_FROM = '' or
--		, iif(salarypackage.Package = '' or salarypackage.Package is null, '', concat('Salary Package: ',salarypackage.Package,char(10)))
--		, iif(pqe.PQE = '' or pqe.PQE is null, '', concat('PQE: ',pqe.PQE,char(10)))
--		, iif(jobsec.JOBSECTOR = '' or jobsec.JOBSECTOR is null, '', concat('Job Sector: ',jobsec.JOBSECTOR,char(10)))
--		, iif(jobsource.Source = '' or jobsource.Source is null, '', concat('Job Source: ',jobsource.Source,char(10)))
--		, iif(referral.Referral = '' or referral.Referral is null, '', concat('Referral: ',referral.Referral,char(10)))
--		--, iif(journalnotes.J_NOTES = '' or journalnotes.J_NOTES is null, '', concat('Journal Notes: ',journalnotes.J_NOTES,char(10)))
--		, iif(posttowebsite.EXP_TO_HOME = '' or posttowebsite.EXP_TO_HOME is null, '', concat('Post to Website: ',posttowebsite.EXP_TO_HOME,char(10)))
--		, iif(date.START_DATE = '' or date.START_DATE is null, '', concat('Date: ',date.START_DATE,char(10))) ----<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
--		, iif(status.Status = '' or status.Status is null, '', concat('Status: ',status.Status,char(10)))
--		, iif(journalnotes.J_NOTES = '' or journalnotes.J_NOTES is null, '', concat('Journal Notes: ',journalnotes.J_NOTES,char(10)))
--		) as 'position-Note'
       , replace(doc.DOC_ID,'.txt','.rtf') as 'position-document'


       , et.createddate as "insert_timestamp"
--       , et.updateddate as "CUSTOM FIELD > Date Updated"
--       , et.UPDATED_BY_NAME as "CUSTOM FIELD > Updated By"
	, source.DESCRIPTION as 'source'
	, skills.DESCRIPTION 'skills'
	, jh.filled_dt as 'Date Filled', CONVERT(VARCHAR(5),jh.filled_tm,108) as 'Time Filled'
	--, jh.NO_CV_SENT as 'No. of CVs/Profiles Sent', jh.NO_IV_ARR as 'No. of Int''s Arranged', jh.NO_IV_ATT as 'No. of Int''s Attended'
	
	, indsec.indsec as 'INDUSTRY SECTORS'
	, jobcategory.DESCRIPTION as 'JOB CATEGORY'

--select COUNT(*) -- select distinct status.status  --jg.CURRENCY -- select top 100 * 
from PROP_X_CLIENT_JOB cj  --8076 rows
left join PROP_JOB_GEN jg on cj.JOB = jg.REFERENCE --where jg.JOB_ID = 880568
left join job on job.JOB = cj.JOB
left join (select pg.REFERENCE, pg.person_id, pg.FIRST_NAME, pg.LAST_NAME, pg.MIDDLE_NAME from PROP_PERSON_GEN pg) pg on pg.reference = cj.CONTACT
--left join (SELECT JC.PRIMREF,START_DATE from PROP_JOB_JOBBOARD JC) date on cj.JOB = date.PRIMREF  --Date
left join (select * from MD_MULTI_NAMES where LANGUAGE = 10010) MN ON MN.ID = jg.CURRENCY
left join PROP_CLIENT_GEN cg on cj.CLIENT = cg.REFERENCE
left join (SELECT REFERENCE, string_agg( MN.DESCRIPTION, ',') as DESCRIPTION FROM PROP_JOB_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_JOB_GEN.JOB_TYPE where MN.ID is not null and LANGUAGE = 10010 GROUP BY REFERENCE) jobtype on jobtype.REFERENCE = cj.JOB
--left join owner on jg.REFERENCE = owner.JOB_GEN_REFERENCE
--left join (select CONS.REFERENCE, EMPLOYEE.NAME as EMPLOYEE_NAME, CLIENT_GEN.NAME as CLIENT_GEN_NAME from PROP_CLIENT_GEN CLIENT_GEN INNER JOIN PROP_OWN_CONS CONS ON CLIENT_GEN.REFERENCE = CONS.REFERENCE INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT where CONFIG_NAME = 'Permanent') own ON jg.REFERENCE = own.REFERENCE
left join (SELECT PRIMREF,SALARY_FROM,SALARY_TO,RATE_FROM,RATE_TO,CONTACT_USER FROM PROP_JOB_JOBBOARD /*where PRIMREF = 116684819944*/) salary on cj.JOB = salary.PRIMREF
left join PROP_PJOB_GEN PJG on cj.JOB = pjg.REFERENCE
left join (
       select distinct CONS.CONSULTANT
              , EMPLOYEE.REFERENCE, EMPLOYEE.NAME, tmp_email.EMAIL as EMAIL_ADD --, mail.email_add 
       from PROP_OWN_CONS CONS
	INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT
	--left join (SELECT a.REFERENCE, string_agg(a.email_add,',') as email_add FROM (SELECT distinct REFERENCE, email_add from PROP_EMAIL WHERE email_add like '%@%') a GROUP BY REFERENCE) mail ON mail.REFERENCE = EMPLOYEE.REFERENCE
	left join tmp_email ON EMPLOYEE.REFERENCE = tmp_email.REFERENCE
	--where CONS.CONSULTANT = 391527411
	) own ON own.CONSULTANT = jg.cons1
left join (SELECT REFERENCE, string_agg( MN.DESCRIPTION, ',') as DESCRIPTION FROM PROP_JOB_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_JOB_GEN.job_src where MN.ID is not null and LANGUAGE = 10010 GROUP BY REFERENCE) source on source.REFERENCE = cj.JOB
left join (select REFERENCE,FILLED_DT,FILLED_TM,NO_CV_SENT,NO_IV_ARR,NO_IV_ATT from PROP_JOB_HIST) jh on jh.REFERENCE = cj.JOB
left join (SELECT REFERENCE, string_agg( MN.DESCRIPTION, char(10)) as DESCRIPTION FROM PROP_JOB_CAT JC INNER JOIN MD_MULTI_NAMES MN ON MN.ID = JC.JOB_CATEGORY where MN.ID is not null and LANGUAGE = 10010 GROUP BY REFERENCE) jobcategory on jobcategory.REFERENCE = cj.JOB
left join (SELECT JC.REFERENCE,MN.DESCRIPTION as Status FROM PROP_JOB_GEN JC INNER JOIN MD_MULTI_NAMES MN ON MN.ID = JC.STATUS where MN.ID is not null and LANGUAGE = 10010) status on status.REFERENCE = cj.JOB --Status
left join PROP_PJOB_FEE pe on pe.reference = cj.JOB
left join (SELECT distinct indsec.reference, string_agg(MN.DESCRIPTION,', ') as indsec FROM PROP_IND_SECT indsec INNER JOIN MD_MULTI_NAMES MN ON MN.ID = indsec.industry where MN.ID is not null and LANGUAGE = 10010 group by indsec.reference) indsec on indsec.reference = cj.JOB --Job Sector
left join (SELECT REFERENCE, string_agg( DESCRIPTION, char(10)) as DESCRIPTION FROM skill GROUP BY REFERENCE) skills on skills.REFERENCE = cj.JOB
left join doc on doc.OWNER_ID = cj.JOB
left join (select et.ENTITY_ID, et.createddate, et.UPDATEDDATE, et.UPDATED_BY, eg.name as UPDATED_BY_NAME from ENTITY_TABLE et left join PROP_EMPLOYEE_GEN eg on eg.user_ref = et.UPDATED_BY) et on et.ENTITY_ID = jg.REFERENCE
left join PROP_IV_GEN iv on iv.reference = jg.reference
--where jg.JOB_ID in (887856)
--or jg.reference in (116672505237)
--or indsec.indsec is not null
--skills.DESCRIPTION is not null
--or iv.IV_DATE is not null
--or jg.CURRENCY is not null
--or skills.REFERENCE is not null
--or doc.OWNER_ID is not null
--where salary.SALARY_FROM is not null and salary.SALARY_TO is not null


/*
-------------------------

select *
from PROP_JOB_GEN jg where jg.job_title = 'Senior Insights Manager'
116681494331

select distinct mn.description
from PROP_X_CLIENT_JOB cj  --8036 rows
left join PROP_JOB_GEN jg on cj.JOB = jg.REFERENCE --where jg.JOB_ID = 882116
left join (select * from MD_MULTI_NAMES where LANGUAGE = 10010) MN ON MN.ID = jg.CURRENCY

select top 100 *
from PROP_X_CLIENT_JOB cj  --8036 rows
left join PROP_JOB_GEN jg on cj.JOB = jg.REFERENCE where jg.JOB_ID in (880517)

with t as (SELECT distinct indsec.reference, string_agg(MN.DESCRIPTION,',') as indsec FROM PROP_IND_SECT indsec INNER JOIN MD_MULTI_NAMES MN ON MN.ID = indsec.industry where MN.ID is not null and LANGUAGE = 10010 group by indsec.reference)
select reference from t group by reference having count(*) > 1
SELECT DISTINCT MN.DESCRIPTION FROM PROP_JOB_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_JOB_GEN.JOB_TYPE where MN.ID is not null and LANGUAGE = 10010
with owner as ( 

select * from owner where PROP_PERSON_GEN_REFERENCE = 391527601
left join owner ON ccc.CONTACT = owner.PROP_PERSON_GEN_REFERENCE

-----------------------


--left join publicdoc on jg.REFERENCE = publicdoc.OWNER_ID
--left join internaldoc on jg.REFERENCE = internaldoc.OWNER_ID

--left join (SELECT PRIMREF,SALARY_FROM FROM PROP_JOB_JOBBOARD) status on cj.JOB = status.REFERENCE
left join (SELECT JC.PRIMREF,MN.DESCRIPTION as Package FROM PROP_JOB_JOBBOARD JC INNER JOIN MD_MULTI_NAMES MN ON MN.ID = JC.S_PACKAGE where JC.PRIMREF = 116684819944) salarypackage on cj.JOB = salarypackage.PRIMREF  --Salary Package
left join (SELECT JC.PRIMREF,MN.DESCRIPTION as PQE FROM PROP_JOB_JOBBOARD JC INNER JOIN MD_MULTI_NAMES MN ON MN.ID = JC.CAREERLEVEL) pqe on cj.JOB = pqe.PRIMREF  --PQE

left join (SELECT JG.REFERENCE,PG.CHIFULLNAME as Referral FROM PROP_JOB_GEN JG INNER JOIN PROP_PERSON_GEN PG ON PG.REFERENCE = JG.REFERRAL) referral on cj.JOB = referral.REFERENCE  --Referral

left join (
	--SELECT J.ENTITY_ID,JE.J_NOTES FROM LK_ENTITIES_JOURNAL J INNER JOIN JOURNAL_ENTRIES JE ON JE.ID = J.JOURNAL_ID
	SELECT ENTITY_ID
  	, STUFF((
	   SELECT distinct ', ' + REPLACE(JE.J_NOTES COLLATE Latin1_General_BIN, char(26), '') --REPLACE(ISNULL(NOTE, ''), CHAR(26), '')
	   FROM LK_ENTITIES_JOURNAL J INNER JOIN JOURNAL_ENTRIES JE ON JE.ID = J.JOURNAL_ID WHERE JE.J_NOTES != '' and JE.J_NOTES is not null and ENTITY_ID = a.ENTITY_ID
	   FOR XML PATH(''), TYPE
	  ).value('.', 'nvarchar(MAX)')
	  , 1, 1, '') As J_NOTES
        FROM LK_ENTITIES_JOURNAL as a GROUP BY a.ENTITY_ID	
	) journalnotes on cj.JOB = journalnotes.ENTITY_ID -- WHERE J.ENTITY_ID = <<PROP_JOB_GEN.REFERENCE>> --Journal Notes --DUPLICATE 60122
left join (SELECT JC.PRIMREF,EXP_TO_HOME from PROP_JOB_JOBBOARD JC) posttowebsite on cj.JOB = posttowebsite.PRIMREF  --Post to Website
--left join (select REFERENCE,CONFIG_NAME,STREET1,STREET2,TOWN,STATE,COUNTRY,POST_CODE from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID where CONFIG_NAME = 'Primary') address ON cg.REFERENCE = address.REFERENCE
--where cj.job = 721396
--where journalnotes.J_NOTES is not null -- cj.JOB = 395016
--publicdoc.NOTE is not null
--jobcategory.DESCRIPTION is not null 
--and jobsource.Source is not null
--and salaryto.SALARY_FROM > 0
--cj.client = 435015
--select TOP 20 * from PROP_PJOB_GEN
--where 1=1 and b.isPrimaryOwner = 1

*/
