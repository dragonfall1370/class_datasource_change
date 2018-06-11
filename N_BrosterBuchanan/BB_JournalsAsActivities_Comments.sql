/*	SELECT OWNER_ID, STUFF((SELECT DISTINCT ',' + cast(DOC_ID as varchar(max)) + '_' 
						+ replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(DOC_NAME,'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':',''),char(10),''),char(13),'')
						+ '.' + replace(coalesce(nullif(FILE_EXTENSION,''),PREVIEW_TYPE),'txt','rtf')
						from DOCUMENTS 
						WHERE OWNER_ID = a.OWNER_ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS docs
FROM DOCUMENTS as a
WHERE a.OWNER_ID = 116662820196 or a.OWNER_ID = 116662888140 or a.OWNER_ID = 116667442875 or a.OWNER_ID = 116664504104                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
GROUP BY a.OWNER_ID

select je.*, r1.CONFIG_ROLE_NAME role1, r2.CONFIG_ROLE_NAME role2, r3.CONFIG_ROLE_NAME role3
	, r4.CONFIG_ROLE_NAME role4,r5.CONFIG_ROLE_NAME role5, r6.CONFIG_ROLE_NAME role6, mn.DESCRIPTION
from  JOURNAL_ENTRIES je left join MD_ROLES r1 on je.ROLE_ID_1 = r1.ROLE_ID
						 left join MD_ROLES r2 on je.ROLE_ID_2 = r2.ROLE_ID
						 left join MD_ROLES r3 on je.ROLE_ID_3 = r3.ROLE_ID
						 left join MD_ROLES r4 on je.ROLE_ID_4 = r4.ROLE_ID
						 left join MD_ROLES r5 on je.ROLE_ID_5 = r5.ROLE_ID
						 left join MD_ROLES r6 on je.ROLE_ID_6 = r6.ROLE_ID
						 left join MD_MULTI_NAMES mn on je.BO_ID = mn.ID
where je.ID = 4310146276 and--je.ROLE_ID_4 is not null and je.ROLE_ID_4 <> 0 and 
mn.LANGUAGE=1 and mn.type = 'BO'-- and ENTITY_ID_5 <> 0
--mn.LANGUAGE=1 and (ENTITY_ID_1 = 116669809495 or ENTITY_ID_2 = 116669809495 or ENTITY_ID_3 = 116669809495 or ENTITY_ID_4 = 116669809495)

select * from LK_ENTITIES_JOURNAL where JOURNAL_ID = 4316494027
select * from ENTITY_TABLE
select * from LK_ENTITY_ROLE
select * from JOURNAL_ENTRIES where id = 4310146276--bo_id is null
select distinct JOURNAL_ID from LK_ENTITIES_JOURNAL

select * from MD_ROLES where role_id = 6990593

select * from PROP_EMPLOYEE_GEN where user_ref = 600083253

select * from PROP_EMPLOYEE_GEN where USER_REF = 600032024
select * from PROP_X_CLIENT_JOB*/

with
  contact0 (CLIENT,CONTACT,rn) as (SELECT CLIENT,CONTACT,ROW_NUMBER() OVER(PARTITION BY cg.CONTACT ORDER BY cg.BISUNIQUEID DESC) AS rn FROM PROP_X_CLIENT_CON cg)

, contact1 as (select CLIENT,CONTACT from contact0 where rn = 1)

, contact as(
select
	ccc.CONTACT,ccc.CLIENT, pg.FIRST_NAME, pg.LAST_NAME, pg.MIDDLE_NAME
	, Stuff(
			  Coalesce(' ' + NULLIF(pg.FIRST_NAME, ''), '')
			+ Coalesce(' ' + NULLIF(pg.MIDDLE_NAME, ''), '')
			+ Coalesce(' ' + NULLIF(pg.LAST_NAME, ''), '')
			, 1, 1, '') as contactFullName
from PROP_X_CLIENT_CON cc
left join contact1 ccc on cc.CONTACT = ccc.CONTACT
left join (select REFERENCE,FIRST_NAME,LAST_NAME, MIDDLE_NAME, JOB_TITLE, LINKEDIN, SALUTATION from PROP_PERSON_GEN) pg on ccc.CONTACT = pg.REFERENCE
)
--select * from contact

, tempJournal as (
 select ej.JOURNAL_ID,ej.ENTITY_ID,ej.ENTITY_TYPE
		,je.CREATION_DATE,je.CREATOR_ID,je.J_NOTES,je.J_DOCUMENT
		,mn.DESCRIPTION workflow
		,r.CONFIG_ROLE_NAME as role
		,comp.REFERENCE companyID
		,comp.name companyName
		,con.contact contactID
		,con.contactFullName
		, cj.JOB jobID
		, jg.JOB_TITLE
		, cp.REFERENCE candID
		, Stuff(
			  Coalesce(' ' + NULLIF(pg.FIRST_NAME, ''), '')
			+ Coalesce(' ' + NULLIF(pg.MIDDLE_NAME, ''), '')
			+ Coalesce(' ' + NULLIF(pg.LAST_NAME, ''), '')
			, 1, 1, '') as candFullName
from LK_ENTITIES_JOURNAL ej left join JOURNAL_ENTRIES je on ej.JOURNAL_ID = je.ID
							left join MD_MULTI_NAMES mn on je.BO_ID = mn.ID
							left join LK_ENTITY_ROLE AS er ON ej.ENTITY_ID = er.ENTITY_ID
							left join MD_ROLES AS r ON er.ROLE_ID = r.ROLE_ID
							left join PROP_CLIENT_GEN comp on ej.ENTITY_ID = comp.REFERENCE
							left join contact con on ej.ENTITY_ID = con.CONTACT
							left join PROP_X_CLIENT_JOB cj on ej.ENTITY_ID = cj.JOB--3095 rows
							left join PROP_JOB_GEN jg on cj.JOB = jg.REFERENCE
							left join PROP_CAND_PREF cp on ej.ENTITY_ID = cp.REFERENCE
							left join PROP_PERSON_GEN pg on cp.REFERENCE = pg.REFERENCE
where mn.LANGUAGE=1 and mn.type = 'BO' and r.CONFIG_ROLE_NAME is not null
--order by JOURNAL_ID
)--trong bang LK_ENTITIES_JOURNAL, journal cung link vs user, nhung lai link vs user_ref, trong khi trong bang JOURNAL_ENTRIES, joural do' link vs reference cua user
--select * from tempJournal where JOURNAL_ID = 4325187318--4323922531


, tempJournal1 as (select t.*, t1.companyID,t1.companyName,t2.contactID,t2.contactFullName,t3.jobID,t3.JOB_TITLE,t4.candID,t4.candFullName
from (select distinct journal_id, workflow,ENTITY_TYPE from tempJournal) t
							 left join (select * from tempJournal where companyId is not null) t1 on t1.JOURNAL_ID = t.JOURNAL_ID
							 left join (select * from tempJournal where contactID is not null) t2 on t2.JOURNAL_ID = t.JOURNAL_ID
							 left join (select * from tempJournal where jobID is not null) t3 on t3.JOURNAL_ID = t.JOURNAL_ID
							 left join (select * from tempJournal where candID is not null) t4 on t4.JOURNAL_ID = t.JOURNAL_ID
 where t.entity_type <> 'U')

 , tempJournal2 as (select j1.*, je.CREATION_DATE,eg.NAME createdUser, je.J_NOTES,je.J_DOCUMENT, jn.J_DOCUMENT as document2
 from tempJournal1 j1 left join JOURNAL_ENTRIES je on j1.JOURNAL_ID = je.ID
						left join JOURNAL_NOTES jn on j1.JOURNAL_ID = jn.JOURNAL_ID
						left join PROP_EMPLOYEE_GEN eg on je.CREATOR_ID = eg.USER_REF)
--where jn.J_DOCUMENT is not null

, companyComments as (select concat('BB',companyID) as CompanyExternalId, -10 as userId
		, CREATION_DATE as InsertTimeStamp, -10 as AssignedUserId, 'comment' as category, 'company' as type
		, J_DOCUMENT
		, concat(
				'-----MIGRATED FROM JOURNAL ENTRIES-----',
				iif(workflow = '' or workflow is null, '', concat(char(10),'Work Flow: ',workflow)),
				iif(companyName = '' or companyName is null, '', concat(char(10),'Company: ',companyName)),
				iif(contactFullName = '' or contactFullName is null, '', concat(char(10),'Contact: ',contactFullName)),
				iif(JOB_TITLE = '' or JOB_TITLE is null, '', concat(char(10),'Job: ',JOB_TITLE)),
				iif(candFullName = '' or candFullName is null, '', concat(char(10),'Candidate: ',candFullName)),
				iif(J_NOTES like '' or candFullName is null, '', concat(char(10),'Notes: ',replace(replace(J_NOTES,'<p>',''),'</p>',''))),
				iif(J_DOCUMENT like '' or J_DOCUMENT is null,iif(document2 like '' or document2 is null,'',concat(char(10),'Documents: ',replace(replace(document2,'<p>',''),'</p>',''))),concat(char(10),'Documents: ',replace(replace(replace(replace(convert(nvarchar(max),j_DOCUMENT),'<p>',''),'</p>',''),'&nbsp;',' '),'  ',' '))),
				iif(CREATION_DATE is null, '', concat(CHAR(10),'Created Date: ',convert(varchar(20),CREATION_DATE,120))),
				iif(createdUser = '' or createdUser is null, '', concat(char(10),'Created By: ',createdUser)),
				concat(char(10),'JE ID: ', JOURNAL_ID)
				) as commentContent
from tempJournal2
where companyID is not null) --and j_DOCUMENT is not null and j_DOCUMENT not like '')

, contactcomment as (select concat('BB',contactID) as ContactExternalId, -10 as userId
		, CREATION_DATE as InsertTimeStamp, -10 as AssignedUserId, 'comment' as category, 'contact' as type
		, J_DOCUMENT
		, concat(
				'-----MIGRATED FROM JOURNAL ENTRIES-----',
				iif(workflow = '' or workflow is null, '', concat(char(10),'Work Flow: ',workflow)),
				iif(companyName = '' or companyName is null, '', concat(char(10),'Company: ',companyName)),
				iif(contactFullName = '' or contactFullName is null, '', concat(char(10),'Contact: ',contactFullName)),
				iif(JOB_TITLE = '' or JOB_TITLE is null, '', concat(char(10),'Job: ',JOB_TITLE)),
				iif(candFullName = '' or candFullName is null, '', concat(char(10),'Candidate: ',candFullName)),
				iif(J_NOTES like '' or candFullName is null, '', concat(char(10),'Notes: ',replace(replace(J_NOTES,'<p>',''),'</p>',''))),
				iif(J_DOCUMENT like '' or J_DOCUMENT is null,iif(document2 like '' or document2 is null,'',concat(char(10),'Documents: ',replace(replace(document2,'<p>',''),'</p>',''))),concat(char(10),'Documents: ',replace(replace(replace(replace(convert(nvarchar(max),j_DOCUMENT),'<p>',''),'</p>',''),'&nbsp;',' '),'  ',' '))),
				iif(CREATION_DATE is null, '', concat(CHAR(10),'Created Date: ',convert(varchar(20),CREATION_DATE,120))),
				iif(createdUser = '' or createdUser is null, '', concat(char(10),'Created By: ',createdUser)),
				concat(char(10),'JE ID: ', JOURNAL_ID)
				) as commentContent
from tempJournal2
where contactID is not null)


, jobcomment as (select concat('BB',jobID) as JobExternalId, -10 as userId
		, CREATION_DATE as InsertTimeStamp, -10 as AssignedUserId, 'comment' as category, 'job' as type
		, J_DOCUMENT
		, concat(
				'-----MIGRATED FROM JOURNAL ENTRIES-----',
				iif(workflow = '' or workflow is null, '', concat(char(10),'Work Flow: ',workflow)),
				iif(companyName = '' or companyName is null, '', concat(char(10),'Company: ',companyName)),
				iif(contactFullName = '' or contactFullName is null, '', concat(char(10),'Contact: ',contactFullName)),
				iif(JOB_TITLE = '' or JOB_TITLE is null, '', concat(char(10),'Job: ',JOB_TITLE)),
				iif(candFullName = '' or candFullName is null, '', concat(char(10),'Candidate: ',candFullName)),
				iif(J_NOTES like '' or candFullName is null, '', concat(char(10),'Notes: ',replace(replace(J_NOTES,'<p>',''),'</p>',''))),
				iif(J_DOCUMENT like '' or J_DOCUMENT is null,iif(document2 like '' or document2 is null,'',concat(char(10),'Documents: ',replace(replace(document2,'<p>',''),'</p>',''))),concat(char(10),'Documents: ',replace(replace(replace(replace(convert(nvarchar(max),j_DOCUMENT),'<p>',''),'</p>',''),'&nbsp;',' '),'  ',' '))),
				iif(CREATION_DATE is null, '', concat(CHAR(10),'Created Date: ',convert(varchar(20),CREATION_DATE,120))),
				iif(createdUser = '' or createdUser is null, '', concat(char(10),'Created By: ',createdUser)),
				concat(char(10),'JE ID: ', JOURNAL_ID)
				) as commentContent
from tempJournal2
where jobID is not null)

, candidatecomment as (select concat('BB',candID) as CanExternalId, -10 as userId
		, CREATION_DATE as InsertTimeStamp, -10 as AssignedUserId, 'comment' as category, 'candidate' as type
		, J_DOCUMENT
		, concat(
				'-----MIGRATED FROM JOURNAL ENTRIES-----',
				iif(workflow = '' or workflow is null, '', concat(char(10),'Work Flow: ',workflow)),
				iif(companyName = '' or companyName is null, '', concat(char(10),'Company: ',companyName)),
				iif(contactFullName = '' or contactFullName is null, '', concat(char(10),'Contact: ',contactFullName)),
				iif(JOB_TITLE = '' or JOB_TITLE is null, '', concat(char(10),'Job: ',JOB_TITLE)),
				iif(candFullName = '' or candFullName is null, '', concat(char(10),'Candidate: ',candFullName)),
				iif(J_NOTES like '' or candFullName is null, '', concat(char(10),'Notes: ',replace(replace(J_NOTES,'<p>',''),'</p>',''))),
				iif(J_DOCUMENT like '' or J_DOCUMENT is null,iif(document2 like '' or document2 is null,'',concat(char(10),'Documents: ',replace(replace(document2,'<p>',''),'</p>',''))),concat(char(10),'Documents: ',replace(replace(replace(replace(convert(nvarchar(max),j_DOCUMENT),'<p>',''),'</p>',''),'&nbsp;',' '),'  ',' '))),
				iif(CREATION_DATE is null, '', concat(CHAR(10),'Created Date: ',convert(varchar(20),CREATION_DATE,120))),
				iif(createdUser = '' or createdUser is null, '', concat(char(10),'Created By: ',createdUser)),
				concat(char(10),'JE ID: ', JOURNAL_ID)
				) as commentContent
from tempJournal2
where candID is not null)

select * from candidatecomment
