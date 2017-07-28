
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

with
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
 owner as (select JOB_GEN.REFERENCE AS JOB_GEN_REFERENCE, JOB_GEN.JOB_TITLE, EMPLOYEE.REFERENCE AS EMPLOYEE_REFERENCE, EMPLOYEE.NAME as EMPLOYEE_NAME
	from PROP_JOB_GEN JOB_GEN INNER JOIN PROP_OWN_CONS CONS ON JOB_GEN.REFERENCE = CONS.REFERENCE INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT where CONFIG_NAME = 'Permanent')
--select * from owner

--select top 100
select
	cj.JOB as 'position-externalId' 
	, cj.CONTACT as 'position-contactId'
	--, jg.STATUS as '(STATUS)'
	, jg.JOB_TITLE as 'position-title'
	--, as 'position-headcount'
	, own.EMPLOYEE_NAME as 'position-owners' 
	--, JG.JOB_TYPE as '(position-type)'
	--, as 'position-employmentType'	
	, mn.DESCRIPTION as 'position-currency'
	--, pjg.SAL_FROM as 'position-actualSalary'
	, salaryto.SALARY_FROM as 'position-actualSalary'
	--, publicdoc.NOTE as 'position-publicDescription'
	--, internaldoc.NOTE as 'position-internalDescription'
	, CONVERT(VARCHAR(10),date.START_DATE,120) as 'position-startDate'
	, CONVERT(VARCHAR(10),jg.END_DT,120) as 'position-endDate'
	--, convert(varchar(10),iif(START_DT = 'xyz',getdate()-1,dateClosed),120) as 'position-endDate'
	, concat(
		concat('Job ID: ',cj.JOB,char(10))
		, case when (owner.EMPLOYEE_NAME = '' or owner.EMPLOYEE_NAME is null) then '' else concat('Job Owner: ',owner.EMPLOYEE_NAME,char(10)) end
		, iif(jobcategory.DESCRIPTION = '' or jobcategory.DESCRIPTION is null, '', concat('Job Category: ',jobcategory.DESCRIPTION,char(10)))
		, iif(/*salaryto.SALARY_FROM = '' or*/ salaryto.SALARY_FROM is null, '', concat('Salary To: ',salaryto.SALARY_FROM,char(10)))
		, iif(salarypackage.Package = '' or salarypackage.Package is null, '', concat('Salary Package: ',salarypackage.Package,char(10)))
		, iif(pqe.PQE = '' or pqe.PQE is null, '', concat('PQE: ',pqe.PQE,char(10)))
		, iif(jobsec.JOBSECTOR = '' or jobsec.JOBSECTOR is null, '', concat('Job Sector: ',jobsec.JOBSECTOR,char(10)))
		, iif(jobsource.Source = '' or jobsource.Source is null, '', concat('Job Source: ',jobsource.Source,char(10)))
		, iif(referral.Referral = '' or referral.Referral is null, '', concat('Referral: ',referral.Referral,char(10)))
		--, iif(journalnotes.J_NOTES = '' or journalnotes.J_NOTES is null, '', concat('Journal Notes: ',journalnotes.J_NOTES,char(10)))
		, iif(posttowebsite.EXP_TO_HOME = '' or posttowebsite.EXP_TO_HOME is null, '', concat('Post to Website: ',posttowebsite.EXP_TO_HOME,char(10)))
		, iif(date.START_DATE = '' or date.START_DATE is null, '', concat('Date: ',date.START_DATE,char(10))) ----<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
		, iif(status.Status = '' or status.Status is null, '', concat('Status: ',status.Status,char(10)))
		, iif(journalnotes.J_NOTES = '' or journalnotes.J_NOTES is null, '', concat('Journal Notes: ',journalnotes.J_NOTES,char(10)))
		) as 'position-Note'
	, cj.CLIENT as '(job-client)'
	, cg.NAME as '(company-name)'
	--, 
-- select top 100 * -- select COUNT(*)
from PROP_X_CLIENT_JOB cj --3095 rows
left join PROP_JOB_GEN jg on cj.JOB = jg.REFERENCE
--left join PROP_PJOB_GEN PJG on cj.JOB = pjg.REFERENCE
left join MD_MULTI_NAMES MN ON MN.ID = jg.CURRENCY
left join (select CONS.REFERENCE, EMPLOYEE.NAME as EMPLOYEE_NAME, CLIENT_GEN.NAME as CLIENT_GEN_NAME from PROP_CLIENT_GEN CLIENT_GEN INNER JOIN PROP_OWN_CONS CONS ON CLIENT_GEN.REFERENCE = CONS.REFERENCE INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT where CONFIG_NAME = 'Permanent') own ON jg.REFERENCE = own.REFERENCE
--left join publicdoc on jg.REFERENCE = publicdoc.OWNER_ID
--left join internaldoc on jg.REFERENCE = internaldoc.OWNER_ID
left join PROP_CLIENT_GEN cg on cj.CLIENT = cg.REFERENCE
left join owner on jg.REFERENCE = owner.JOB_GEN_REFERENCE
left join (
	--SELECT REFERENCE,MN.DESCRIPTION as jobcat FROM PROP_JOB_CAT JC INNER JOIN MD_MULTI_NAMES MN ON MN.ID = JC.JOB_CATEGORY
	SELECT REFERENCE
  	, STUFF((
	   SELECT distinct ', ' + REPLACE(MN.DESCRIPTION COLLATE Latin1_General_BIN, char(26), '') --REPLACE(ISNULL(NOTE, ''), CHAR(26), '')
	   FROM PROP_JOB_CAT JC INNER JOIN MD_MULTI_NAMES MN ON MN.ID = JC.JOB_CATEGORY where REFERENCE = a.REFERENCE
	   FOR XML PATH(''), TYPE
	  ).value('.', 'nvarchar(MAX)')
	  , 1, 1, '') As DESCRIPTION
        FROM PROP_JOB_CAT as a GROUP BY a.REFERENCE
	) jobcategory on cj.JOB = jobcategory.REFERENCE --Job Category --DUPLICATE 2828
left join (SELECT PRIMREF,SALARY_FROM FROM PROP_JOB_JOBBOARD) salaryto on cj.JOB = salaryto.PRIMREF --Salary To
--left join (SELECT PRIMREF,SALARY_FROM FROM PROP_JOB_JOBBOARD) status on cj.JOB = status.REFERENCE
left join (SELECT JC.PRIMREF,MN.DESCRIPTION as Package FROM PROP_JOB_JOBBOARD JC INNER JOIN MD_MULTI_NAMES MN ON MN.ID = JC.S_PACKAGE) salarypackage on cj.JOB = salarypackage.PRIMREF  --Salary Package
left join (SELECT JC.PRIMREF,MN.DESCRIPTION as PQE FROM PROP_JOB_JOBBOARD JC INNER JOIN MD_MULTI_NAMES MN ON MN.ID = JC.CAREERLEVEL) pqe on cj.JOB = pqe.PRIMREF  --PQE
left join (
	--SELECT JC.REFERENCE,MN.DESCRIPTION as Source FROM PROP_JOB_GEN JC INNER JOIN MD_MULTI_NAMES MN ON MN.ID = JC.JOB_SRC
	SELECT REFERENCE
  	, STUFF((
	   SELECT distinct ', ' + REPLACE(MN.DESCRIPTION COLLATE Latin1_General_BIN, char(26), '') --REPLACE(ISNULL(NOTE, ''), CHAR(26), '')
	   FROM PROP_JOB_GEN JC INNER JOIN MD_MULTI_NAMES MN ON MN.ID = JC.JOB_SRC where REFERENCE = a.REFERENCE
	   FOR XML PATH(''), TYPE
	  ).value('.', 'nvarchar(MAX)')
	  , 1, 1, '') As Source
        FROM PROP_JOB_GEN as a GROUP BY a.REFERENCE
	) jobsource on cj.JOB = jobsource.REFERENCE  --Job Source --DUPLICATE 4119
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
left join (SELECT JC.PRIMREF,START_DATE from PROP_JOB_JOBBOARD JC) date on cj.JOB = date.PRIMREF  --Date
left join (SELECT SECTOR.JOB_SECTOR,MN.DESCRIPTION AS JOBSECTOR FROM PROP_X_JOB_SECTOR AS SECTOR INNER JOIN MD_MULTI_NAMES AS MN ON MN.ID = SECTOR.JOB_SECTOR) jobsec on cj.JOB = jobsec.JOB_SECTOR  --Job Sector
left join (SELECT JC.REFERENCE,MN.DESCRIPTION as Status FROM PROP_JOB_GEN JC INNER JOIN MD_MULTI_NAMES MN ON MN.ID = JC.STATUS) status on cj.JOB = status.REFERENCE --Status
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
