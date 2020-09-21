/*
mail1 as (
       select REFERENCE
	, replace(translate(value, '!'':"<>[]();,+/\&?•*|‘','                     '),char(9),'') as email --to translate special characters
	from PROP_EMAIL
	cross apply string_split(EMAIL_ADD,' ')
	where EMAIL_ADD like '%_@_%.__%'
	and REFERENCE in (61065,43945)
	)
select * from mail1 where email <> ''
*/

with
-- tel(REFERENCE, TEL_NUMBER) as (SELECT REFERENCE, STUFF((SELECT DISTINCT ', ' + TEL_NUMBER from  PROP_TELEPHONE WHERE REFERENCE = a.REFERENCE FOR XML PATH ('')), 1, 2, '')  AS URLList FROM PROP_TELEPHONE as a GROUP BY a.REFERENCE)

-- EMAIL
  mail1 (ID,email) as (select REFERENCE, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(EMAIL_ADD
        ,'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),CHAR(9),' ') as mail from PROP_EMAIL where EMAIL_ADD like '%_@_%.__%' )
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, e1 (ID,email) as (select ID, email from mail4 where rn = 1)
, ed (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 (ID,email) as (select ID, email from mail4 where rn = 2)
, e3 (ID,email) as (select ID, email from mail4 where rn = 3)
, e4 (ID,email) as (select ID, email from mail4 where rn = 4)
--select * from ed where ID in (45315)

, tel (REFERENCE, TEL_NUMBER, config_name) as (select REFERENCE,TEL_NUMBER, OCC.config_name from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID) --Fax Home Mobile MobileWork Pref Work

--, dob (BISUNIQUEID,REFERENCE,DT_OF_BIRTH,rn) as (SELECT BISUNIQUEID,REFERENCE,DT_OF_BIRTH,ROW_NUMBER() OVER(PARTITION BY cg.REFERENCE ORDER BY cg.BISUNIQUEID DESC) AS rn FROM PROP_CAND_GEN cg WHERE cg.DT_OF_BIRTH IS NOT NULL)
--, dob as (REFERENCE,DT_OF_BIRTH) as (SELECT REFERENCE,DT_OF_BIRTH FROM PROP_CAND_GEN cg WHERE cg.DT_OF_BIRTH IS NOT NULL)

--, photo  (OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY = 6532918 AND FILE_EXTENSION in ('gif','jpeg','jpg','png') and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
--, photo as (OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE FILE_EXTENSION in ('gif','jpeg','jpg','png') and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
--, resume (OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY = 31159 AND FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip') and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
, resume (OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE OWNER_ID = a.OWNER_ID and DOC_CATEGORY not in (6532841/*for contact*/, 6532839 /*for profile*/, 6532840 /*for experience*/  ) FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)


, profile0 (owner_id, content, CREATED_DATE) as (SELECT owner_id, ltrim(replace(replace( [dbo].[udf_StripHTML](convert(varchar(max),convert(varbinary(max),DOCUMENT))) ,'Â',''),'ï»¿','')) as 'content', CREATED_DATE FROM DOCUMENTS where DOC_CATEGORY = 6532839 )
, profile (owner_id, content) as (
       SELECT owner_id
              , STRING_AGG( coalesce( concat('Created Date: ',CREATED_DATE,char(10)) + nullif(content, '') + char(10), '') ,char(10) ) WITHIN GROUP (ORDER BY CREATED_DATE desc) content       
       FROM profile0 where content <> '' GROUP BY owner_id
       )
--select * from profile where owner_id = 96624


, experience0 (owner_id, content, CREATED_DATE) as (SELECT owner_id, ltrim(replace(replace( [dbo].[udf_StripHTML](convert(varchar(max),convert(varbinary(max),DOCUMENT))) ,'Â',''),'ï»¿','')) as 'content', CREATED_DATE FROM DOCUMENTS where DOC_CATEGORY = 6532840 )
, experience (owner_id, content) as (
       SELECT owner_id
              , STRING_AGG( coalesce( concat('Created Date: ',CREATED_DATE,char(10)) + nullif(content, '') + char(10), '') ,char(10) ) WITHIN GROUP (ORDER BY CREATED_DATE desc) content       
       FROM experience0 where content <> '' GROUP BY owner_id
       )
--select * from experience where owner_id = 208680


, assignments(reference, status, assignments) as (
       SELECT pg.reference, status.DESCRIPTION as status
         , STRING_AGG(
                            stuff(
                              coalesce('Job Title: ' + nullif(cast(ag.job_title as nvarchar(max)), '') + char(10), '')
                            + coalesce('Start Date: ' + nullif(cast(convert(date,ag.start_dt) as nvarchar(max)), '') + char(10), '')
                            + coalesce('End Date: ' + nullif(cast(convert(date,ag.end_dt) as nvarchar(max)), '') + char(10), '')
                            + coalesce('Latest Ext. End Date: ' + nullif(cast(convert(date,ag.l_ext_end_dt) as nvarchar(max)), '') + char(10), '')
                            + coalesce('Status: ' + nullif(cast(status.DESCRIPTION as nvarchar(max)), '') + char(10), '')
                            + coalesce('Type: ' + nullif(cast(type.DESCRIPTION as nvarchar(max)), '') + char(10), '')
                            + coalesce('Rate/Salary: ' + nullif(cast(ag.cand_rate as nvarchar(max)), '') + char(10), '')
                            + coalesce('Client: ' + nullif(cast(cg.name as nvarchar(max)), '') + char(10), '')
                            + coalesce('Agency: ' + nullif(cast(ag.PRV_AGENCY as nvarchar(max)), '') + char(10), '')
                            , 1, 0, '')
              ,char(10) ) WITHIN GROUP (ORDER BY ag.start_dt desc) assignments
       -- select pg.reference, pg.person_id, pg.first_name, pg.last_name, ag.job_title, convert(date,ag.start_dt) as start_dt, convert(time,ag.start_tm) as start_tm, convert(date,ag.end_dt) as end_dt, convert(time,ag.end_tm) as end_tm, convert(date,ag.l_ext_end_dt) as l_ext_end_dt, ag.REASON_LEAV, status.DESCRIPTION as status, type.DESCRIPTION as type, ag.cand_rate, cg.name, ag.PRV_AGENCY  , ag.CLIENT_NOTIC, ag.NOTIFICAT_AW, ag.NO_WEEKS, ag.NO_DAYS, ag.CH_IN_DATE, ag.ORIG_START, ag.*
       -- select distinct status.DESCRIPTION
       from PROP_X_ASSIG_CAND xag
       left join PROP_ASSIG_GEN ag on ag.reference = xag.assignment
       left join ( SELECT id, DESCRIPTION from MD_MULTI_NAMES MN where LANGUAGE = 10010 ) status on status.id = ag.status
       left join ( SELECT id, DESCRIPTION from MD_MULTI_NAMES MN where LANGUAGE = 10010 ) type on type.id = ag.ASSIG_TYPE
       left join PROP_CLIENT_GEN cg on cg.client_id = ag.assig_id
       left join PROP_PERSON_GEN pg on pg.reference = xag.candidate
       --where pg.person_id in (1110897)
       --where status.DESCRIPTION in ('Unbook') --('Past','Unbook')
       GROUP BY pg.reference, status.DESCRIPTION
       )
--select top 10 * assignments;
, CURRENTEMPLOYMENT (reference, assignments) as (
       SELECT reference, STRING_AGG(assignments,char(10) ) WITHIN GROUP (ORDER BY status desc) assignments
       from assignments
       where status in ('Future','Current')
       GROUP BY reference
       )
--select top 10 * from CURRENTEMPLOYMENT;
, PREVIOUSEMPLOYMENT (reference, assignments) as (
       SELECT reference, STRING_AGG(assignments,char(10) ) WITHIN GROUP (ORDER BY status desc) assignments
       from assignments
       where status in ('Unbook','Past')
       GROUP BY reference
       )
--select top 10 * from PREVIOUSEMPLOYMENT;

, skill as (select REFERENCE,DESCRIPTION from PROP_SKILLS SKILL INNER JOIN MD_MULTI_NAMES MN ON MN.ID = SKILL.SKILL where LANGUAGE = 10010)
, location as (select REFERENCE, string_agg(DESCRIPTION,', ') WITHIN GROUP (ORDER BY DESCRIPTION asc) location from PROP_LOCATIONS LOCATION INNER JOIN MD_MULTI_NAMES MN ON MN.ID = LOCATION.LOCATION where LANGUAGE = 10010 GROUP BY reference)
, indsec as (select REFERENCE,DESCRIPTION from PROP_IND_SECT indsec INNER JOIN MD_MULTI_NAMES MN ON MN.ID = indsec.industry where LANGUAGE = 10010)
, cat as (select REFERENCE,DESCRIPTION from PROP_JOB_CAT JOB_CAT INNER JOIN MD_MULTI_NAMES MN ON MN.ID = JOB_CAT.JOB_CATEGORY where LANGUAGE = 10010)

, owner0 as ( 
       select PERSON_GEN.REFERENCE as PROP_PERSON_GEN_REFERENCE, PERSON_GEN.FIRST_NAME, PERSON_GEN.LAST_NAME, EMPLOYEE.REFERENCE as PROP_EMPLOYEE_GEN_REFERENCE, EMPLOYEE.NAME, CONFIG_NAME, trim(tmp_email.EMAIL) as EMAIL_ADD, EMPLOYEE.USER_REF --mail.EMAIL_ADD
       from PROP_PERSON_GEN PERSON_GEN 
	INNER JOIN PROP_OWN_CONS CONS ON PERSON_GEN.REFERENCE = CONS.REFERENCE 
	INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID 
	INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT 
	--left join (SELECT a.REFERENCE, string_agg(a.EMAIL_ADD,', ') as EMAIL_ADD FROM (SELECT distinct REFERENCE, email_add from PROP_EMAIL WHERE email_add like '%_@_%.__%') a GROUP BY a.REFERENCE) mail ON EMPLOYEE.REFERENCE = mail.REFERENCE
	left join tmp_email ON EMPLOYEE.REFERENCE = tmp_email.REFERENCE
	where CONFIG_NAME in ('Permanent','Contract')
	--and PERSON_GEN.FIRST_NAME like '%Christian%' and PERSON_GEN.LAST_NAME like '%Kwan%'
	)
--select * from owner where EMAIL_ADD like '%david.fleming@venetian.com.mo%'

/*owner0 as ( 
       select PERSON_GEN.REFERENCE as PROP_PERSON_GEN_REFERENCE, PERSON_GEN.FIRST_NAME, PERSON_GEN.LAST_NAME, CONFIG_NAME, trim(tmp_email.EMAIL) as EMAIL_ADD
              , EMPLOYEE.REFERENCE as PROP_EMPLOYEE_GEN_REFERENCE
              , EMPLOYEE.NAME, EMPLOYEE.USER_REF --mail.email_add ,
       -- select distinct OCC.CONFIG_NAME
       from PROP_PERSON_GEN PERSON_GEN
	INNER JOIN PROP_OWN_CONS CONS ON PERSON_GEN.REFERENCE = CONS.REFERENCE 
	INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID 
	INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT 
	--left join (SELECT REFERENCE, email_add = STUFF((SELECT DISTINCT ', ' + email_add FROM PROP_EMAIL b WHERE b.email_add like '%@%' and b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM PROP_EMAIL a GROUP BY REFERENCE) mail ON EMPLOYEE.REFERENCE = mail.REFERENCE
	--left join (SELECT a.REFERENCE, string_agg(a.email_add,',') as email_add FROM (SELECT distinct REFERENCE, email_add from PROP_EMAIL WHERE email_add like '%@%') a GROUP BY REFERENCE) mail ON EMPLOYEE.REFERENCE = mail.REFERENCE
	left join tmp_email ON EMPLOYEE.REFERENCE = tmp_email.REFERENCE
	where CONFIG_NAME in ('Permanent','Contract')
	--and PERSON_GEN.FIRST_NAME like '%Christian%' and PERSON_GEN.LAST_NAME like '%Kwan%'
	)
--select PROP_PERSON_GEN_REFERENCE from owner where EMAIL_ADD is 40342 null group by PROP_PERSON_GEN_REFERENCE having count(*) > 1
--select * from owner where PROP_PERSON_GEN_REFERENCE in (40240,40355)
*/

, owner (PROP_PERSON_GEN_REFERENCE, EMAIL_ADD) as (
       SELECT PROP_PERSON_GEN_REFERENCE
              , STRING_AGG( EMAIL_ADD, ',' ) WITHIN GROUP (ORDER BY EMAIL_ADD desc) EMAIL_ADD       
       FROM (select distinct PROP_PERSON_GEN_REFERENCE, EMAIL_ADD from owner0 where EMAIL_ADD <> '') t
       GROUP BY PROP_PERSON_GEN_REFERENCE
       )
--select * from owner where PROP_PERSON_GEN_REFERENCE in (40240,40355)


-- SKILL
, ce (reference, skillName) as (
       SELECT 
                reference
              , STRING_AGG(
                     stuff(
                              coalesce('Job Title: ' + nullif(cast(job_title as nvarchar(max)), '') + char(10), '')                   
                            + coalesce('Start Date: ' + nullif(cast(start_dt as nvarchar(max)), '') + char(10), '')
                            + coalesce('Client: ' + nullif(cast(name as nvarchar(max)), '') + char(10), '')
                            + coalesce('Agency: ' + nullif(cast(PRV_AGENCY as nvarchar(max)), '') + char(10), '')
                            + coalesce('Type: ' + nullif(cast(type as nvarchar(max)), '') + char(10), '')
                            + coalesce('Status: ' + nullif(cast(status as nvarchar(max)), '') + char(10), '')
                     , 1, 0, '')
              ,char(10) ) WITHIN GROUP (ORDER BY status) content
       from (
              select pg.reference, pg.person_id, pg.first_name, pg.last_name
                     , ag.job_title, convert(date,ag.start_dt) as start_dt, convert(time,ag.start_tm) as start_tm, convert(date,ag.end_dt) as end_dt, convert(time,ag.end_tm) as end_tm, convert(date,ag.l_ext_end_dt) as l_ext_end_dt, ag.REASON_LEAV, status.DESCRIPTION as status, type.DESCRIPTION as type, ag.cand_rate, cg.name, ag.PRV_AGENCY --, ag.CLIENT_NOTIC, ag.NOTIFICAT_AW, ag.NO_WEEKS, ag.NO_DAYS, ag.CH_IN_DATE, ag.ORIG_START
                     --, ag.*
              -- select distinct status.id, status.DESCRIPTION           
              from PROP_X_ASSIG_CAND xag
              left join PROP_ASSIG_GEN ag on ag.reference = xag.assignment
              left join ( SELECT id, DESCRIPTION from MD_MULTI_NAMES MN where LANGUAGE = 10010 ) status on status.id = ag.status
              left join ( SELECT id, DESCRIPTION from MD_MULTI_NAMES MN where LANGUAGE = 10010 ) type on type.id = ag.ASSIG_TYPE
              left join PROP_CLIENT_GEN cg on cg.client_id = ag.assig_id
              left join PROP_PERSON_GEN pg on pg.reference = xag.candidate
              where status.DESCRIPTION in ('Current','Future')
              --where ag.ORIG_START is not null
              --where xag.candidate
              and pg.person_id in (1091898)
       ) t
       GROUP BY reference )
--select top 10 * from ce where reference in (165199)



select --top 123
         pg.REFERENCE as 'candidate-externalId', pg.person_id
	, Coalesce(NULLIF(replace(pg.FIRST_NAME,'?',''), ''), 'No Firstname') as 'candidate-firstName'
	, Coalesce(NULLIF(replace(pg.LAST_NAME,'?',''), ''), 'No Lastname') as 'candidate-lastName'
	--, pg.MIDDLE_NAME as 'candidate-middleName'
	, cp.P_TEMP, cp.P_PERM, cp.P_CONTR
	, CONVERT(VARCHAR(10),cand_gen.DT_OF_BIRTH,110) as 'candidate-dob'
	--, title.TITLE as 'candidate-title'
	--, pg.JOB_TITLE as '(candidate-title)'
	--, pg.TITLE as 'candidate-title'
       , case
              when title.DESCRIPTION in ('Doctor') then ''
              when title.DESCRIPTION in ('Sir','Mr') then 'MALE'
              when title.DESCRIPTION in ('Ms') then 'FEMALE'
              when title.DESCRIPTION in ('Miss') then 'FEMALE'
              when title.DESCRIPTION in ('Mrs') then 'FEMALE'
              end as 'candidate-gender'
       , case
              when title.DESCRIPTION in ('Doctor') then 'DR'
              when title.DESCRIPTION in ('Sir','Mr') then 'MR'
              when title.DESCRIPTION in ('Ms') then 'MS'
              when title.DESCRIPTION in ('Miss') then 'MISS'
              when title.DESCRIPTION in ('Mrs') then 'MRS'
              end as 'candidate-title'

	--, concat(address.STREET1,iif(street2.STREET2 = '' OR street2.STREET2 is NULL,'',concat(', ',street2.STREET2))) as '(company-address)'
	--, Stuff( Coalesce(' ' + NULLIF(address.STREET1, ''), '') + Coalesce(', ' + NULLIF(street2.STREET2, ''), ''), 1, 1, '') as 'company-address'
	, ltrim(Stuff( Coalesce(' ' + NULLIF(address.STREET1, ''), '') + Coalesce(', ' + NULLIF(address.STREET2, ''), '') , 1, 1, '') ) as 'candidate-address'
	, address.TOWN as 'candidate-city'
	, address.county as 'candiadte-state'
	, address.POST_CODE as 'candidate-zipCode'
       , address.country_name as 'candidate-country'
       , Nationality.DESCRIPTION as 'candidate-citizenship'
	
	, iif(ed.rn > 1, concat(ed.email,'_',ed.rn), ed.email) as 'candidate-email' --, e1.email 
	, e2.email as 'candidate-workEmail'
	
	, case 
                when (cast(telmobile.TEL_NUMBER  as varchar(max)) != '' and telmobile.TEL_NUMBER is not null) then telmobile.TEL_NUMBER
                when (cast(telhome.TEL_NUMBER as varchar(max)) != '' and telhome.TEL_NUMBER is not null) then telhome.TEL_NUMBER
                else telwork.TEL_NUMBER
                end as 'candidate-phone' --primary phone
	
	, telhome.TEL_NUMBER as 'candidate-homePhone'
	, telmobile.TEL_NUMBER as 'candidate-mobile'	 --candidate-phone
	, telwork.TEL_NUMBER as 'candidate-workPhone'
	
--	, case
--		when cur.DESCRIPTION like '%Australia%' then 'AUD'
--		when cur.DESCRIPTION like '%Chinese%' then 'CNY'
--		when cur.DESCRIPTION like '%Euro%' then 'EUR'
--		when cur.DESCRIPTION like '%Hong%' then 'HKD'
--		when cur.DESCRIPTION like '%Japan%' then 'JPY'
--		when cur.DESCRIPTION like '%Kuwait%' then 'KWD'
--		when cur.DESCRIPTION like '%New%' then 'NZD'
--		when cur.DESCRIPTION like '%Romania%' then 'RON'
--		when cur.DESCRIPTION like '%Singapore%' then 'SGD'
--		when cur.DESCRIPTION like '%United Kingdom%' then 'GBP'
--		when cur.DESCRIPTION like '%United States%' then 'USD'
--		end as 'candidate-currency'

	--, 'PERMANENT' as 'candidate-jobTypes'
--	, cp.SALARY_CURR as 'candidate-currentSalary'
--	, cp.SALARY_DES as 'candidate-desiredSalary'
       , cp.RATE_REQ, cp.SALARY_REQ as 'Min. Salary', cp.OTE_REQ, cp.RATE_DES, cp.SALARY_DES
	, pg.linkedin as 'candidate-linkedin'
       , pg.facebook as 'facebook' --*
       , pg.skype_id as 'skype' --*
       , pg.salutation as 'Preferred Name' --*
       , source.DESCRIPTION as 'Source' --*
       , cand_gen.e_shot as 'Mail Subscribed' --*
	
	, stuff(
                + Coalesce('CURRENT EMPLOYMENT: ' + char(10) + NULLIF(convert(nvarchar(max),CURRENTEMPLOYMENT.assignments), '') + char(10), '')
                + Coalesce('PREVIOUS EMPLOYMENT: ' + char(10) + NULLIF(convert(nvarchar(max),PREVIOUSEMPLOYMENT.assignments), '') + char(10), '')               
                + Coalesce(char(10) +'Notice Period: ' + NULLIF(convert(nvarchar(max),Noticeperiod.DESCRIPTION), '') + char(10), '')
                + Coalesce(char(10) +'Available From: ' + NULLIF(convert(nvarchar(max),cand_gen.AVAIL_FROM), '') + char(10), '')
                , 1, 0, '') as 'candidate-workHistory' --***

	--, ee2.ESTAB as 'candidate-schoolName' --'candidate-education'
	--, ee3.QUAL as 'candidate-degreeName'
	--, ee.TO_DATE as 'candidate-graduationDate'
       --, ee3.DEGREE as 'candidate-degreeName'
	--, ee.gpa 'candidate-gpa'
	--, skills.DESCRIPTION 'candidate-skills'

	, cg.name as 'candidate-employer1'
       , pg.job_title as 'candidate-jobtitle1'
       , cg.name as 'candidate-company1'
	--, left(st1.StartDate,10) as 'candidate-startdate1'
	--, left(et01.EndDate,10) as 'candidate-enddate1'

	, owner.EMAIL_ADD as 'candidate-owners'
	--, t4.finame as 'Candidate File'
	
	, replace(resume.DOC_ID,'.txt','.rtf') as 'candidate-resume'
	, et.createddate as "insert_timestamp"
	--, photo.DOC_ID as 'candidate-photo'
       , Stuff( Coalesce('Person ID: ' + NULLIF(convert(nvarchar(max),pg.person_id), '') + char(10), '')
              --+ Coalesce('Owner: ' + NULLIF(convert(nvarchar(max),owner.name), '') + char(10), '')
              + Coalesce('Twitter: ' + NULLIF(convert(nvarchar(max),pg.twitter), '') + char(10), '')
              + Coalesce('Last Contacted: ' + NULLIF(convert(nvarchar(max),convert(date,ph.last_cont_dt)), '') + char(10), '')
              + Coalesce('By: ' + NULLIF(convert(nvarchar(max),eg1.name), '') + char(10), '')
              + Coalesce('Last Updated: ' + NULLIF(convert(nvarchar(max),et.updateddate), '') + char(10), '')
              + Coalesce('By: ' + NULLIF(convert(nvarchar(max),et.updated_by_name), '') + char(10), '')
              + Coalesce('Created Date: ' + NULLIF(convert(nvarchar(max),et.createddate), '') + char(10), '')
              + Coalesce('By: ' + NULLIF(convert(nvarchar(max),et.created_by_name), '') + char(10), '')
              + Coalesce('UK Resident: ' + NULLIF(convert(nvarchar(max),pg.res_pool), '') + char(10), '')
              + Coalesce('Status: ' + NULLIF(convert(nvarchar(max),status.DESCRIPTION), '') + char(10), '')
              + Coalesce('Looking For: ' + NULLIF(convert(nvarchar(max),cp.look_for), '') + char(10), '')
              + Coalesce('Country of Residence: ' + NULLIF(convert(nvarchar(max),pg.nationality), '') + char(10), '')
              + Coalesce('Preferred Locations: ' + NULLIF(convert(nvarchar(max),location.location), '') + char(10), '')
              + Coalesce('Minimum Salary: ' + NULLIF(convert(nvarchar(max),cp.salary_req), '') + char(10), '')
              + Coalesce('Willing to Relocate: ' + NULLIF( iif(cp.RELOCATE = 'Y','Yes','No'), '') + char(10), '')
              + Coalesce('Hot Candidate: ' + NULLIF(convert(nvarchar(max),cand_gen.hot), '') + char(10), '')
              + Coalesce(char(10) + 'EXPERIENCE: ' + char(10) + NULLIF(convert(nvarchar(max),experience.content), '') + char(10), '')
              + Coalesce(char(10) +'PROFILE: ' + char(10) + NULLIF(convert(nvarchar(max),profile.content), '') + char(10), '')
                , 1, 0, '') as 'candidate-note'
--	, replace(concat(
--		 iif(pg.Salutation = '' OR pg.Salutation is NULL,'',concat ('Salutation: ',pg.Salutation,char(10)))
--		, iif(pg.CHINESENAME = '' OR pg.CHINESENAME is NULL,'',concat ('Chinese Name: ',pg.CHINESENAME,char(10)))
--		, iif(pg.ChiFullName = '' OR pg.ChiFullName is NULL,'',concat ('Full Name: ',pg.ChiFullName,char(10)))
--		, iif(emailh2.EMAIL_ADD = '' OR emailh2.EMAIL_ADD is NULL,'',concat ('Home Email: ',emailh2.EMAIL_ADD,char(10)))
--		--, iif(mail5.email3 = '' or mail5.email3 is null,'',concat('Other Email: ',mail5.email3,char(10)))
--		, Coalesce('Other email: ' + NULLIF(concat(e3.email,' ',e4.email), '') + char(10), '')
--		, case when (pl.DESCRIPTION = '' or pl.DESCRIPTION is null) then '' else concat('Location: ',pl.DESCRIPTION,char(10)) end
--		, iif(status.Status = '' OR status.Status is NULL,'',concat ('Status: ',status.Status,char(10)))
--		, iif(rating.Ranking = '' OR rating.Ranking is NULL,'',concat ('Ranking: ',rating.Ranking,char(10)))
--		, iif(cur.DESCRIPTION = '' or cur.DESCRIPTION is null,'',concat('Currency: ',case
--			when cur.DESCRIPTION like '%Australia%' then 'AUD'
--			when cur.DESCRIPTION like '%Chinese%' then 'CNY'
--			when cur.DESCRIPTION like '%Euro%' then 'EUR'
--			when cur.DESCRIPTION like '%Hong%' then 'HKD'
--			when cur.DESCRIPTION like '%Japan%' then 'JPY'
--			when cur.DESCRIPTION like '%Kuwait%' then 'KWD'
--			when cur.DESCRIPTION like '%New%' then 'NZD'
--			when cur.DESCRIPTION like '%Romania%' then 'RON'
--			when cur.DESCRIPTION like '%Singapore%' then 'SGD'
--			when cur.DESCRIPTION like '%United Kingdom%' then 'GBP'
--			when cur.DESCRIPTION like '%United States%' then 'USD'
--			end,char(10)))
--		, iif(/*sal.SALARY_DES = '' OR*/ sal.SALARY_DES is NULL,'',concat ('Salary Desired: ',cast(sal.SALARY_DES as varchar(max)),char(10)))
--		, iif(sal.CurrentSalaryMonth = '' OR sal.CurrentSalaryMonth is NULL,'',concat ('Current Salary Month: ',sal.CurrentSalaryMonth,char(10)))
--		, iif(sal.ExpectedSalaryMonth = '' OR sal.ExpectedSalaryMonth is NULL,'',concat ('Expected Salary Month: ',sal.ExpectedSalaryMonth,char(10)))	
--		, case when (cast(pg.PERSON_ID as varchar(max)) = '' or pg.PERSON_ID is null) then '' else concat('Personal ID: ',pg.PERSON_ID,char(10)) end
--		, iif(pg.ID_Passport = '' OR pg.ID_Passport is NULL,'',concat ('ID_Passport: ',pg.ID_Passport,char(10)))
--		, iif(visa.VISA = '' OR visa.VISA is NULL,'',concat ('VISA: ',visa.VISA,char(10)))
--		, iif(websiteprofile.PORTFOLIO = '' OR websiteprofile.PORTFOLIO is NULL,'',concat ('Website Profile: ',websiteprofile.PORTFOLIO,char(10)))
--		, iif(otheronlineprofile.ONLINE_PRO = '' OR otheronlineprofile.ONLINE_PRO is NULL,'',concat ('Other Online Profile: ',otheronlineprofile.ONLINE_PRO,char(10)))
--		, iif(consname.ConsName = '' OR consname.ConsName is NULL,'',concat ('Consultant: ',consname.ConsName,char(10)))
--		, case when (owner.NAME = '' or owner.NAME is null) then '' else concat('Candidate Owner: ',owner.NAME,char(10)) end
--		, iif(source.Source = '' OR source.Source is NULL,'',concat ('Source: ',source.Source,char(10)))
--		, iif(referral.Referral = '' OR referral.Referral is NULL,'',concat ('Referral: ',referral.Referral,char(10)))
--		--, case when (industry.DESCRIPTION = '' or industry.DESCRIPTION is null) then '' else concat('Industry: ',industry.DESCRIPTION,char(10)) end
--		, case when (JOBCATEGORY.DESCRIPTION = '' or JOBCATEGORY.DESCRIPTION is null) then '' else concat('Industry: ',JOBCATEGORY.DESCRIPTION,char(10)) end
--		, case when (JOBCATEGORY.DESCRIPTION = '' or JOBCATEGORY.DESCRIPTION is null) then '' else concat('Job Category: ',JOBCATEGORY.DESCRIPTION,char(10)) end
--		, case when (SubCategory.DESCRIPTION = '' or SubCategory.DESCRIPTION is null) then '' else concat('Sub Category: ',SubCategory.DESCRIPTION,char(10)) end
--		, Coalesce('Notice Period: ' + NULLIF(cast(Noticeperiod.Noticeperiod as varchar(max)), '') + char(10), '')
--		, Coalesce('Willing to Relocate: ' + NULLIF(cast(relocate.Relocate as varchar(max)), '') + char(10), '')
--		--, note.NOTE,char(10)
--		),'&amp;','') as 'candidate-note'
	--, comment.NOTE as 'candidate-comments'
-- select count(*) -- select top 10 *
-- select top 1000 RATING,PQE,PQE_YEAR,PQE_YEAR2
-- select distinct cp.location
from PROP_PERSON_GEN pg --where pg.person_id = 1136371 --44889 rows
left join PROP_CAND_PREF cp on pg.REFERENCE = cp.REFERENCE

left join (SELECT REFERENCE, MN.DESCRIPTION FROM PROP_PERSON_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_PERSON_GEN.TITLE where MN.ID is not null and LANGUAGE = 10010) title on pg.REFERENCE = title.REFERENCE
--left join (select REFERENCE, CONFIG_NAME,STREET1,TOWN,STATE,COUNTRY,POST_CODE from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID where CONFIG_NAME = 'Primary') address ON pg.REFERENCE = address.REFERENCE
left join ( 
       SELECT REFERENCE, STREET1, STREET2, LOCALITY, TOWN, county, POST_CODE, COUNTRY
              , case
		when DESCRIPTION like 'AFGHANI%' then 'AF'
		when DESCRIPTION like 'ALBANIA%' then 'AL'
		when DESCRIPTION like 'ARGENTI%' then 'AR'
		when DESCRIPTION like 'AUSTRAL%' then 'AU'
		when DESCRIPTION like 'AUSTRIA%' then 'AT'
		when DESCRIPTION like 'BELARUS%' then 'BY'
		when DESCRIPTION like 'BELGIUM%' then 'BE'
		when DESCRIPTION like 'BRAZIL%' then 'BR'
		when DESCRIPTION like 'BULGARI%' then 'BG'
		when DESCRIPTION like 'CANADA%' then 'CA'
		when DESCRIPTION like 'CHILE%' then 'CL'
		when DESCRIPTION like 'CHINA%' then 'CN'
		when DESCRIPTION like 'COLOMBI%' then 'CO'
		when DESCRIPTION like 'CYPRUS%' then 'CY'
		when DESCRIPTION like 'CZECH%' then 'CZ'
		when DESCRIPTION like 'DENMARK%' then 'DK'
		when DESCRIPTION like 'ECUADOR%' then 'EC'
		when DESCRIPTION like 'EGYPT%' then 'EG'
		when DESCRIPTION like 'ESTONIA%' then 'EE'
		when DESCRIPTION like 'FINLAND%' then 'FI'
		when DESCRIPTION like 'FRANCE%' then 'FR'
		when DESCRIPTION like 'GERMANY%' then 'DE'
		when DESCRIPTION like 'GIBRALT%' then 'ES'
		when DESCRIPTION like 'GREECE%' then 'GR'
		when DESCRIPTION like 'HUNGARY%' then 'HU'
		when DESCRIPTION like 'ICELAND%' then 'IS'
		when DESCRIPTION like 'INDIA%' then 'IN'
		when DESCRIPTION like 'INDONES%' then 'ID'
		when DESCRIPTION like 'IRELAND%' then 'IE'
		when DESCRIPTION like 'ISRAEL%' then 'IL'
		when DESCRIPTION like 'ITALY%' then 'IT'
		when DESCRIPTION like 'JORDAN%' then 'JO'
		when DESCRIPTION like 'KAZAKHS%' then 'KZ'
		when DESCRIPTION like 'KENYA%' then 'KE'
		when DESCRIPTION like 'LATVIA%' then 'LV'
		when DESCRIPTION like 'LEBANON%' then 'LB'
		when DESCRIPTION like 'LIECHTE%' then 'LI'
		when DESCRIPTION like 'LITHUAN%' then 'LT'
		when DESCRIPTION like 'LUXEMBO%' then 'LU'
		when DESCRIPTION like 'MACEDON%' then 'MK'
		when DESCRIPTION like 'MALAYSI%' then 'MY'
		when DESCRIPTION like 'MALTA%' then 'MT'
		when DESCRIPTION like 'MOLDOVA%' then 'MD'
		when DESCRIPTION like 'NETHERL%' then 'NL'
		when DESCRIPTION like 'NEW ZEALAND%' then 'NZ'
		when DESCRIPTION like 'NORWAY%' then 'NO'
		when DESCRIPTION like 'PANAMA%' then 'PA'
		when DESCRIPTION like 'PHILIPP%' then 'PH'
		when DESCRIPTION like 'POLAND%' then 'PL'
		when DESCRIPTION like 'PORTUGA%' then 'PT'
		when DESCRIPTION like 'QATAR%' then 'QA'
		when DESCRIPTION like 'ROMANIA%' then 'RO'
		when DESCRIPTION like 'RUSSIAN%' then 'RU'
		when DESCRIPTION like 'SAUDI%' then 'SA'
		when DESCRIPTION like 'SERBIA%' then 'RS'
		when DESCRIPTION like 'SINGAPO%' then 'SG'
		when DESCRIPTION like 'SLOVAKI%' then 'SK'
		when DESCRIPTION like 'SLOVENI%' then 'SI'
		when DESCRIPTION like 'SOUTH AFRICA%' then 'ZA'
		when DESCRIPTION like 'SPAIN%' then 'ES'
		when DESCRIPTION like 'SRI%' then 'LK'
		when DESCRIPTION like 'SWAZILA%' then 'SZ'
		when DESCRIPTION like 'SWEDEN%' then 'SE'
		when DESCRIPTION like 'SWITZER%' then 'CH'
		when DESCRIPTION like 'TAIWAN%' then 'TW'
		when DESCRIPTION like 'TAJIKIS%' then 'TJ'
		when DESCRIPTION like 'TURKEY%' then 'TR'
		when DESCRIPTION like 'UKRAINE%' then 'UA'
		when DESCRIPTION like '%UNITED%ARAB%' then 'AE'
		when DESCRIPTION like '%UAE%' then 'AE'
		when DESCRIPTION like '%U.A.E%' then 'AE'
		when DESCRIPTION like '%UNITED%KINGDOM%' then 'GB'
		when DESCRIPTION like '%UNITED%STATES%' then 'US'
		when DESCRIPTION like '%US%' then 'US'
              end as 'country_name'
       from (
              select ADDRESS.REFERENCE, ADDRESS.STREET1, ADDRESS.STREET2, ADDRESS.LOCALITY, ADDRESS.TOWN, ADDRESS.county, ADDRESS.POST_CODE, ADDRESS.COUNTRY, MN.DESCRIPTION , rn = ROW_NUMBER() OVER (PARTITION BY REFERENCE, STREET1, STREET2, LOCALITY, TOWN, county, POST_CODE, MN.DESCRIPTION ORDER BY CONFIG_NAME desc)
              from PROP_ADDRESS ADDRESS 
              left JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID = ADDRESS.OCC_ID 
              left JOIN MD_MULTI_NAMES MN ON MN.ID = ADDRESS.COUNTRY
              where OCC.config_name = 'Primary' --and ADDRESS.REFERENCE in (44361)
              ) a
       where a.rn = 1 --and REFERENCE in (45315)
       ) address ON address.REFERENCE = pg.REFERENCE
--left join (SELECT * from dob where rn = 1) dob on pg.REFERENCE = dob.REFERENCE
--left join (select REFERENCE, DESCRIPTION from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID INNER JOIN MD_MULTI_NAMES MN ON MN.ID = ADDRESS.COUNTRY where CONFIG_NAME = 'Primary' and MN.ID is not null and LANGUAGE = 10010) cnt ON pg.REFERENCE = cnt.REFERENCE
left join (SELECT REFERENCE, string_agg(MN.DESCRIPTION,',') as DESCRIPTION FROM PROP_CAND_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_CAND_GEN.NATION GROUP BY REFERENCE) Nationality on pg.REFERENCE = Nationality.REFERENCE --21167 --Need to convert value to 2 digits country code 

--left join (select REFERENCE, TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Home') telhome on pg.REFERENCE = telhome.REFERENCE --candidate-homePhone
--left join (select REFERENCE, TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Mobile') telmobile on pg.REFERENCE = telmobile.REFERENCE --candidate-mobile
left join (select REFERENCE, string_agg( ltrim(rtrim( Stuff( coalesce(' ' + NULLIF(replace(TEL_NUMBER,',',' '), ''), '') + coalesce(' - ext ' + NULLIF(extension, ''), '') , 1, 1, '') )), ',') as TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Work' and (TEL_NUMBER <> '' and TEL_NUMBER is not null) GROUP BY REFERENCE) telwork on pg.REFERENCE = telwork.REFERENCE --candidate-phone & candidate-workPhone
left join (select * from tel where CONFIG_NAME = 'Home') telhome on telhome.REFERENCE = pg.REFERENCE --candidate-homePhone
left join (select * from tel where CONFIG_NAME = 'Mobile') telmobile on telmobile.REFERENCE = pg.REFERENCE --candidate-mobile

--left join (SELECT REFERENCE, iif(RELOCATE = 'Y','Yes','No') AS Relocate FROM PROP_CAND_PREF) relocate on pg.REFERENCE = relocate.REFERENCE
left join (SELECT REFERENCE, string_agg(MN.DESCRIPTION,',') as DESCRIPTION FROM PROP_CAND_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_CAND_GEN.SOURCE where MN.ID is not null and LANGUAGE = 10010 GROUP BY REFERENCE) source on pg.REFERENCE = source.REFERENCE --21159
left join (SELECT REFERENCE, string_agg(MN.DESCRIPTION,',') as DESCRIPTION FROM PROP_CAND_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_CAND_GEN.NOT_PERIOD where MN.ID is not null and LANGUAGE = 10010 GROUP BY REFERENCE) Noticeperiod on pg.REFERENCE = Noticeperiod.REFERENCE --22765
left join (SELECT REFERENCE,hot,DT_OF_BIRTH, AVAIL_FROM, e_shot FROM PROP_CAND_GEN) cand_gen on cand_gen.REFERENCE = pg.REFERENCE
--left join PROP_CAND_GEN eshot on eshot.reference = pg.reference

left join owner ON owner.PROP_PERSON_GEN_REFERENCE = pg.REFERENCE
left join ed ON ed.id = pg.REFERENCE -- DUPLICATED-EMAIL
left join e2 ON e2.id = pg.REFERENCE -- Other-EMAIL
--left join e3 ON ccc.CONTACT = e3.id -- Other-EMAIL
--left join e4 ON ccc.CONTACT = e4.id -- Other-EMAIL
--left join (SELECT REFERENCE, EMAIL_ADD from PROP_EMAIL where OCC_ID = 100000217) emailh2 on pg.REFERENCE = emailh2.REFERENCE
left join resume on pg.REFERENCE = resume.OWNER_ID
left join experience on experience.OWNER_ID = pg.REFERENCE
left join profile on profile.OWNER_ID = pg.REFERENCE
left join (SELECT REFERENCE, name FROM PROP_CLIENT_GEN where REFERENCE is not null) ce on ce.REFERENCE = pg.employment
left join PROP_CLIENT_GEN cg on cg.reference = pg.cur_empl
left join PROP_PERSON_HIST ph on ph.reference = pg.reference left join PROP_EMPLOYEE_GEN eg1 on eg1.user_ref = ph.last_cont_by
left join (SELECT REFERENCE, string_agg(MN.DESCRIPTION,', ') as DESCRIPTION FROM PROP_CAND_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_CAND_GEN.STATUS where MN.ID is not null and LANGUAGE = 10010 GROUP BY REFERENCE) status on pg.REFERENCE = status.REFERENCE
left join (select PERSON_GEN.REFERENCE, EMPLOYEE.NAME as ConsName from PROP_PERSON_GEN PERSON_GEN INNER JOIN PROP_OWN_CONS CONS ON PERSON_GEN.REFERENCE = CONS.REFERENCE INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT where CONFIG_NAME = 'Permanent') consName on pg.REFERENCE = consname.REFERENCE
left join (
       select et.ENTITY_ID, et.createddate, et.created_by, eg1.name as created_by_name, et.UPDATEDDATE, et.UPDATED_BY, eg2.name as UPDATED_BY_NAME 
       from ENTITY_TABLE et 
       left join PROP_EMPLOYEE_GEN eg1 on eg1.user_ref = et.created_by
       left join PROP_EMPLOYEE_GEN eg2 on eg2.user_ref = et.UPDATED_BY
       ) et on et.ENTITY_ID = pg.reference
left join CURRENTEMPLOYMENT on CURRENTEMPLOYMENT.reference = pg.reference
left join PREVIOUSEMPLOYMENT on PREVIOUSEMPLOYMENT.reference = pg.reference
left join location on location.reference = pg.reference
where
pg.reference in (40498,116659009709,63816,116656559606,116688555147)
--pg.person_id in (1105381) --,1096967,1131988, 1094197,1131624, 1131780, 1131833, 1098818,1103933,1120519, 1092301, 1097091, 1103039, 1110897, 1124389, 1134289)
--where pg.person_id in (107796,105767,75209,89420,79136)
--pg.res_pool is not null
--cand_gen.hot is not null
--Nationality.DESCRIPTION is not null 
--pg.twitter is not null
--pg.linkedin is not null 
--pg.facebook is not null 
--pg.skype_id is not null 
--source.DESCRIPTION is not null
--pg.FIRST_NAME like '%G%' and pg.LAST_NAME like '%Poon%'
--resume.DOC_ID like '%.txt'


/*
select * from PROP_PERSON_GEN pg where pg.person_id in (1102307)

--left join comment on pg.REFERENCE = comment.OWNER_ID
--left join note on pg.REFERENCE = note.OWNER_ID
left join photo on pg.REFERENCE = photo.OWNER_ID

left join (SELECT REFERENCE, DESCRIPTION FROM PROP_PERSON_GEN PERSON_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PERSON_GEN.CURRENCY where MN.ID is not null and LANGUAGE = 10010) cur on pg.REFERENCE = cur.REFERENCE

left join (SELECT REFERENCE, PORTFOLIO FROM PROP_PORTFOLIO) websiteprofile on pg.REFERENCE = websiteprofile.REFERENCE
left join (SELECT REFERENCE, ONLINE_PRO from PROP_ONLINE) otheronlineprofile on pg.REFERENCE = otheronlineprofile.REFERENCE
left join (select REFERENCE, STREET2 from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID where CONFIG_NAME = 'Primary') street2 on pg.REFERENCE = street2.REFERENCE --and ADDRESS.REFERENCE = <<PROP_PERSON_GEN.REFERENCE>>
left join (SELECT REFERENCE, MN.DESCRIPTION as Ranking from PROP_PERSON_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_PERSON_GEN.RATING) rating on pg.REFERENCE = rating.REFERENCE
left join (SELECT REFERENCE, SALARY_DES, MN.DESCRIPTION as CurrentSalaryMonth,MN2.DESCRIPTION as ExpectedSalaryMonth FROM PROP_CAND_PREF CAND_PREF LEFT JOIN MD_MULTI_NAMES MN ON MN.ID = CAND_PREF.SALARY_TYPE LEFT JOIN MD_MULTI_NAMES MN2 ON MN2.ID = CAND_PREF.EXPSAL_TYPE) sal on pg.REFERENCE = sal.REFERENCE --where SALARY_DES is not null and MN.DESCRIPTION is not null and MN2.DESCRIPTION is not null
left join (SELECT REFERENCE, DESCRIPTION = STUFF((SELECT DESCRIPTION + char(10) FROM skill b WHERE b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 0, '') FROM skill a GROUP BY REFERENCE) skills ON pg.REFERENCE = skills.REFERENCE -- candidate-email & candidate-workEmail
left join (SELECT REFERENCE, DESCRIPTION = STUFF((SELECT DISTINCT ', ' + DESCRIPTION FROM location b WHERE DESCRIPTION != '' and b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM location a GROUP BY REFERENCE) pl on pg.REFERENCE = pl.REFERENCE
--left join (SELECT REFERENCE, DESCRIPTION = STUFF((SELECT DISTINCT ', ' + DESCRIPTION FROM industry b WHERE b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM industry a GROUP BY REFERENCE) industry ON pg.REFERENCE = industry.REFERENCE -- industry
left join (SELECT REFERENCE, DESCRIPTION = STUFF((SELECT DISTINCT ', ' + DESCRIPTION FROM JobCategory b WHERE b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM JobCategory a GROUP BY REFERENCE) JobCategory ON pg.REFERENCE = JobCategory.REFERENCE -- JobCategory
left join (SELECT REFERENCE, DESCRIPTION = STUFF((SELECT DISTINCT ', ' + DESCRIPTION FROM SubCategory b WHERE b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM SubCategory a GROUP BY REFERENCE) SubCategory ON pg.REFERENCE = SubCategory.REFERENCE -- SubCategory
left join (SELECT REFERENCE, ESTAB = STUFF((SELECT DISTINCT '; ' + ESTAB FROM PROP_EDU_ESTAB b WHERE b.ESTAB != '' and b.REFERENCE = a.REFERENCE FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2,'') FROM PROP_EDU_ESTAB a GROUP BY REFERENCE) ee2 on pg.REFERENCE = ee2.REFERENCE
left join (SELECT REFERENCE, QUAL = STUFF((SELECT DISTINCT '; ' + QUAL FROM PROP_EDU_ESTAB b WHERE b.qual != '' and b.REFERENCE = a.REFERENCE FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') FROM PROP_EDU_ESTAB a GROUP BY REFERENCE) ee3 on pg.REFERENCE = ee3.REFERENCE
left join (SELECT REFERENCE, STUFF((SELECT distinct ', ' + REPLACE(MN.DESCRIPTION COLLATE Latin1_General_BIN, char(26), '') FROM PROP_CAND_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_CAND_GEN.VISA_TYPE where REFERENCE = a.REFERENCE FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') As VISA FROM PROP_CAND_GEN as a GROUP BY a.REFERENCE) visa on pg.REFERENCE = visa.REFERENCE
left join (SELECT REFERENCE, STUFF((SELECT distinct ', ' + REPLACE(PG.CHIFULLNAME COLLATE Latin1_General_BIN, char(26), '') FROM PROP_CAND_GEN INNER JOIN PROP_PERSON_GEN PG ON PG.REFERENCE = PROP_CAND_GEN.REFERRAL where PG.REFERENCE = a.REFERENCE FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') As referral FROM PROP_CAND_GEN as a GROUP BY a.REFERENCE) referral on pg.REFERENCE = referral.REFERENCE --21193
where pg.REFERENCE in (668143)

--select * from PROP_PERSON_GEN where FIRST_NAME = 'Andrew' and LAST_NAME = 'Wang'
*/