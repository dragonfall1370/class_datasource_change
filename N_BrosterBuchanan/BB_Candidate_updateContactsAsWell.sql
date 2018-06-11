	
with
-- tel(REFERENCE, TEL_NUMBER) as (SELECT REFERENCE, STUFF((SELECT DISTINCT ', ' + TEL_NUMBER from  PROP_TELEPHONE WHERE REFERENCE = a.REFERENCE FOR XML PATH ('')), 1, 2, '')  AS URLList FROM PROP_TELEPHONE as a GROUP BY a.REFERENCE)
-- EMAIL
  mail1 (ID,email) as (select REFERENCE, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(EMAIL_ADD
        ,'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),CHAR(9),' ') as mail from PROP_EMAIL where EMAIL_ADD like '%_@_%.__%' ) -- from bullhorn1.Candidate
, mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
/*, mail5 (ID,email1,email2) as (
		select pe.ID, email as email1, we.email2 as email2 from mail4 pe
		left join (select ID, email as email2 from mail4 where rn = 2) we on we.ID = pe.ID
		where pe.rn = 1 ) */
, ed0 (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM mail4) --DUPLICATION
, ed1 (ID,email,rn) as (select distinct ID,email,rn from ed0 where rn > 1)
, e1 as (select ID, email from mail4 where rn = 1)
, e2 as (select ID, email from mail4 where rn = 2)
, e3 as (select ID, email from mail4 where rn = 3)
, e4 as (select ID, email from mail4 where rn = 4)
--select distinct ID,email from maildup where rn > 2 --20313
--select * from mail4 where email in ('gburch@lockelord.com')

--, photo  (OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY = 6532918 AND FILE_EXTENSION in ('gif','jpeg','jpg','png') and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
--, photo  (OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE FILE_EXTENSION in ('gif','jpeg','jpg','png') and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
--, resume (OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY = 31159 AND FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip') and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
--, resume (OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
, doc as (
	SELECT OWNER_ID, STUFF((SELECT DISTINCT ',' + cast(DOC_ID as varchar(max)) + '_' 
						+ replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(DOC_NAME,'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':',''),char(10),''),char(13),'')
						+ '.' + replace(coalesce(nullif(FILE_EXTENSION,''),PREVIEW_TYPE),'txt','rtf')
						from DOCUMENTS 
						WHERE OWNER_ID = a.OWNER_ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS docs
FROM DOCUMENTS as a
--WHERE DOC_ID = 101883385                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
GROUP BY a.OWNER_ID)
, location as (select REFERENCE,DESCRIPTION from PROP_LOCATIONS LOCATION INNER JOIN MD_MULTI_NAMES MN ON MN.ID = LOCATION.LOCATION and MN.LANGUAGE=1)

--, industry    as (select REFERENCE,DESCRIPTION from PROP_IND_SECT JOB_CAT INNER JOIN MD_MULTI_NAMES MN ON MN.ID = JOB_CAT.INDUSTRY)
--, JobCategory as (select REFERENCE,DESCRIPTION from PROP_JOB_CAT JOB_CAT INNER JOIN MD_MULTI_NAMES MN ON MN.ID = JOB_CAT.JOB_CATEGORY)
--, SubCategory as (select REFERENCE,DESCRIPTION from PROP_SUB_CAT SUB_CAT INNER JOIN MD_MULTI_NAMES MN ON MN.ID = SUB_CAT.SUB_CAT)

, tempowners as ( 
	select PERSON_GEN.REFERENCE as PROP_PERSON_GEN_REFERENCE,PERSON_GEN.FIRST_NAME,PERSON_GEN.LAST_NAME,EMPLOYEE.REFERENCE as PROP_EMPLOYEE_GEN_REFERENCE,EMPLOYEE.NAME,mail.EMAIL_ADD,EMPLOYEE.USER_REF 
	from PROP_PERSON_GEN PERSON_GEN INNER JOIN PROP_OWN_CONS CONS ON PERSON_GEN.REFERENCE = CONS.REFERENCE 
									INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID 
									INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT 
	left join (SELECT REFERENCE, EMAIL_ADD = STUFF((SELECT DISTINCT ', ' + EMAIL_ADD FROM PROP_EMAIL b WHERE b.EMAIL_ADD like '%_@_%.__%' and b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM PROP_EMAIL a GROUP BY REFERENCE) mail ON EMPLOYEE.REFERENCE = mail.REFERENCE
	where CONFIG_NAME = 'Permanent'
	--and PERSON_GEN.FIRST_NAME like '%Christian%' and PERSON_GEN.LAST_NAME like '%Kwan%'
	)

, owner as(
	select *, case 
	when NAME like '%Alistair Illstone%' then 'alistairillstone@brosterbuchanan.com'
	when NAME like '%Andrew Broster%' then 'andrewbroster@brosterbuchanan.com'
	when NAME like '%Antony Clish%' then 'antonyclish@brosterbuchanan.com'
	when NAME like '%Antony Marchant%' then 'antonymarchant@brosterbuchanan.com'
	when NAME like '%Bruce Hopkin%' then 'brucehopkin@brosterbuchanan.com'
	when NAME like '%Charles Ford%' then 'charlesford@brosterbuchanan.com'
	when NAME like '%Chloe Hawkins%' then 'rachelpike@brosterbuchanan.com'
	when NAME like '%Chris Batters%' then 'chrisbatters@brosterbuchanan.com'
	when NAME like '%Christian Fell%' then 'christianfell@brosterbuchanan.com'
	when NAME like '%Dominic Cassidy%' then 'dominiccassidy@brosterbuchanan.com'
	when NAME like '%Gemma Ingram%' then 'gemmaingram@brosterbuchanan.com'
	when NAME like '%Hilary Marshall%' then 'hilarymarshall@brosterbuchanan.com'
	when NAME like '%Kevin Moran%' then 'kevinmoran@brosterbuchanan.com'
	when NAME like '%Joel Shewell%' then 'kevinmoran@brosterbuchanan.com'
	when NAME like '%Lenna Thompson%' then 'lennathompson@brosterbuchanan.com'
	when NAME like '%Lucy Tavender%' then 'lucytavender@brosterbuchanan.com'
	when NAME like '%Marie Brocklehurst%' then 'dominiccassidy@brosterbuchanan.com'
	when NAME like '%Nancy Storey%' then 'nancystorey@brosterbuchanan.com'
	when NAME like '%Nick Parry%' then 'charlesford@brosterbuchanan.com'
	when NAME like '%Patrick Smith%' then 'patricksmith@brosterbuchanan.com'
	when NAME like '%Rachel Payne%' then 'rachelpayne@brosterbuchanan.com'
	when NAME like '%Rachel Pike%' then 'rachelpike@brosterbuchanan.com'
	when NAME like '%Sean Hynan%' then 'rachelpike@brosterbuchanan.com'
	else '' end as owmerEmail
from tempOwners)

, title as (
	SELECT REFERENCE, MN.DESCRIPTION as TITLE 
	FROM PROP_PERSON_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_PERSON_GEN.TITLE 
	where MN.LANGUAGE=1)

, gender as (
	SELECT REFERENCE, MN.DESCRIPTION as GENDER 
	FROM PROP_CAND_GEN cg INNER JOIN MD_MULTI_NAMES MN ON MN.ID = cg.GENDER 
	where MN.LANGUAGE=1)

, skill as (
	select REFERENCE,DESCRIPTION from PROP_SKILLS SKILL INNER JOIN MD_MULTI_NAMES MN ON MN.ID = SKILL.SKILL where MN.LANGUAGE=1)
	
, skills as (
	SELECT REFERENCE, DESCRIPTION = STUFF((SELECT '; ' + replace(DESCRIPTION,'&amp;','&')
	FROM skill b WHERE b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM skill a GROUP BY REFERENCE)

, tempEducation as (select *, ROW_NUMBER() OVER(PARTITION BY REFERENCE ORDER BY BISUNIQUEID ASC) AS rn
from PROP_EDU_ESTAB)

 , tempEducation1 as(
	select REFERENCE, 
	concat(
	  iif(BISUNIQUEID = '' or BISUNIQUEID is NULL,'',concat(rn,'. '))
	, iif(ESTAB = '' or ESTAB is NULL,'',ESTAB)
	, iif(From_Date is NULL,'',concat(char(10),'From: ',convert(varchar(10),From_Date, 120)))
	, iif(To_Date is NULL,'',concat(char(10),'To: ',convert(varchar(10),To_Date, 120)))
	, iif(QUAL = '' or QUAL is NULL,'',concat(char(10),'Qualification: ',QUAL ))
	) as education
	, rn
	from tempEducation)

--select * from tempEducation1

, education as (SELECT REFERENCE, 
     STUFF(
         (SELECT char(10) + char(10) + education
          from  tempEducation1
          WHERE REFERENCE =te.REFERENCE
    order by REFERENCE asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,2, '')  AS canEducation
FROM tempEducation1 as te
GROUP BY te.REFERENCE)

, tempaddress as (
	select REFERENCE,CONFIG_NAME,STREET1,STREET2,TOWN,COUNTY,COUNTRY,POST_CODE, DESCRIPTION
		, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(STREET1, ''), '')
			+ Coalesce(', ' + NULLIF(STREET2, ''), '')
			+ Coalesce(', ' + NULLIF(TOWN, ''), '')
			+ Coalesce(', ' + NULLIF(COUNTY, ''), '')
			+ Coalesce(', ' + NULLIF(DESCRIPTION, ''), '')
			+ Coalesce(', ' + NULLIF(POST_CODE, ''), '')
			, 1, 1, '')) as 'locationName'
	from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID
								LEFT JOIN MD_MULTI_NAMES MN ON MN.ID = ADDRESS.COUNTRY  and MN.LANGUAGE = 1
	where CONFIG_NAME = 'Primary')
	
, address1 as (select * from tempaddress where locationName is not null)

, address2 as (
	select REFERENCE,CONFIG_NAME,STREET1,STREET2,TOWN,COUNTY,COUNTRY,POST_CODE, DESCRIPTION
		, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(STREET1, ''), '')
			+ Coalesce(', ' + NULLIF(STREET2, ''), '')
			+ Coalesce(', ' + NULLIF(TOWN, ''), '')
			+ Coalesce(', ' + NULLIF(COUNTY, ''), '')
			+ Coalesce(', ' + NULLIF(DESCRIPTION, ''), '')
			+ Coalesce(', ' + NULLIF(POST_CODE, ''), '')
			, 1, 1, '')) as 'locationName'
	from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID
								LEFT JOIN MD_MULTI_NAMES MN ON MN.ID = ADDRESS.COUNTRY  and MN.LANGUAGE = 1
	where CONFIG_NAME <> 'Primary')

, Nationality as (
	SELECT pg.REFERENCE, MN.DESCRIPTION as nationality
	FROM PROP_PERSON_GEN pg INNER JOIN MD_MULTI_NAMES MN ON MN.ID = pg.NATIONALITY 
	where MN.LANGUAGE=1 and NATIONALITY is not null)

-----------------------------------Email using my way: Get wwork email and home email to note
, WorkEmail as (select REFERENCE,replace(EMAIL_ADD,',','.') EMAIL_ADD
from PROP_EMAIL TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID 
where CONFIG_NAME = 'work' and EMAIL_ADD like '%_@_%.__%')

, homeEmail as (select REFERENCE,replace(EMAIL_ADD,',','.') EMAIL_ADD
from PROP_EMAIL TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID 
where CONFIG_NAME = 'home' and EMAIL_ADD like '%_@_%.__%')
-------------each contact has only 1 email so no need to combine 

, tempEmail as (select cp.REFERENCE, replace(coalesce(he.EMAIL_ADD,we.EMAIL_ADD),' ','') as email
from PROP_CAND_PREF cp left join WorkEmail we on cp.REFERENCE = we.REFERENCE
				left join homeEmail he on cp.REFERENCE = he.REFERENCE)

, tempEmail1 as (select REFERENCE, email, ROW_NUMBER() OVER(PARTITION BY email ORDER BY REFERENCE ASC) AS rn
from tempEmail where email is not null)

, CandEmail as (select REFERENCE, 
case 
when rn=1 then email
else concat('DUPLICATE',rn,'_',(email))
end as Email
from tempEmail1)

, createdUpdatedInfo as (select et.*, eg.NAME CreatedBy, eg1.NAME UpdatedBy
from ENTITY_TABLE et left join PROP_EMPLOYEE_GEN eg on et.CREATED_BY = eg.USER_REF
					left join PROP_EMPLOYEE_GEN eg1 on et.UPDATED_BY = eg1.USER_REF)

, contactedInfo as (select ph.*, eg.NAME contactedBy
from PROP_PERSON_HIST ph left join PROP_EMPLOYEE_GEN eg on ph.LAST_CONT_BY = eg.USER_REF)

, curEmployer as (select pg.REFERENCE,pg.CUR_EMPL, cg.name currentEmployer
from PROP_PERSON_GEN pg left join PROP_CLIENT_GEN cg on pg.CUR_EMPL = cg.REFERENCE
where pg.CUR_EMPL is not null and cg.NAME is not null)

, employment as (select pg.reference as canID,pg.EMPLOYMENT,ag.*, ROW_NUMBER() OVER(PARTITION BY ag.reference ORDER BY ag.BISUNIQUEID DESC) AS rn, MN.DESCRIPTION as empStatus,MN1.DESCRIPTION as employmentType--,mn2.DESCRIPTION as agency
from  PROP_PERSON_GEN pg left join PROP_ASSIG_GEN ag on pg.EMPLOYMENT = ag.REFERENCE
							left join PROP_CAND_GEN cg on pg.REFERENCE = cg.REFERENCE
							left join ENTITY_TABLE et on cg.REFERENCE = et.ENTITY_ID
							left join MD_MULTI_NAMES MN on ag.STATUS = MN.ID and MN.LANGUAGE = 1
							left join MD_MULTI_NAMES MN1 on ag.ASSIG_TYPE = MN1.ID and MN1.LANGUAGE = 1
							--left join MD_MULTI_NAMES MN2 on ag.PREV_AGENCY = MN2.ID and MN2.LANGUAGE = 1
where ag.REFERENCE is not null and cg.REFERENCE is not null)-- and et.CREATEDDATE > '2017-04-30')

-------------------------------------------------MAIN SCRIPT
, candidate as (
select
	-- pg.REFERENCE as 'candidate-externalId'
	concat('BB',cg.REFERENCE) as 'candidate-externalId'
	, Coalesce(NULLIF(replace(pg.FIRST_NAME,'?',''), ''), 'No Firstname') as 'candidate-firstName'
	, Coalesce(NULLIF(replace(pg.LAST_NAME,'?',''), ''), 'No Lastname') as 'candidate-lastName'
	, pg.MIDDLE_NAME as 'candidate-middleName' 
	--, CONVERT(VARCHAR(10),dob.DT_OF_BIRTH,110) as 'candidate-dob'
	, CONVERT(VARCHAR(10),cg.DT_OF_BIRTH,120) as 'candidate-dob'
	--, title.TITLE as 'candidate-title'
	, case
		when title.TITLE like 'Sir' then 'MR'
		when title.TITLE like 'Doctor' then 'DR'
		else upper(title.TITLE) end as 'candidate-title'
	--, pg.JOB_TITLE as '(candidate-title)'
	--, pg.TITLE as 'candidate-title'
	--, case title.TITLE 
	--	when 'Mr' then 'MALE' 
	--	when 'Ms' then 'FEMALE'
	--	when 'Mrs' then 'FEMALE'
	--	when 'Miss' then 'FEMALE'
	--	when 'Doctor' then ''
	--	when 'Not Active' then ''
	--  end as 'candidate-gender'
	, upper(gender.GENDER) as 'candidate-gender'
	, replace(replace(replace(replace(replace(address1.locationName,',,',','),', ,',', '),'  ',' '),' ,',','),'[:, image, ','') as 'candidate-address'
	--,address2.locationName locationName2
	--, concat(address.STREET1,iif(street2.STREET2 = '' OR street2.STREET2 is NULL,'',concat(', ',street2.STREET2))) as '(company-address)'
	--, Stuff( Coalesce(' ' + NULLIF(address.STREET1, ''), '') + Coalesce(', ' + NULLIF(street2.STREET2, ''), ''), 1, 1, '') as 'company-address'
	, replace(address1.TOWN,'image','') as 'candidate-city'
	, address1.county as 'candiadate-state'
	, address1.POST_CODE as 'candidate-zipCode'
	--, Nationality.Nationality as 'candidate-citizenship'
	, case
		when nal.Nationality like 'United Kingdom%' then 'GB'
		when nal.Nationality like 'Spain%' then 'ES'
		else ''	end as 'candidate-citizenship'
	, case
        when address1.DESCRIPTION like '%AUSTRALIA%' then 'AU'
		when address1.DESCRIPTION like '%GERMANY%' then 'DE'
		when address1.DESCRIPTION like '%IRELAND%' then 'IE'
		when address1.DESCRIPTION like '%ITALY%' then 'IT'
		when address1.DESCRIPTION like '%UNITED KINGDOM%' then 'GB'
		when address1.DESCRIPTION like '%UNITED STATES%' then 'US'
		else 'GB' end as 'candidate-country'
	, e1.email as 'oldcandEmail'
	, iif(cem.email = '' or cem.email is null, concat('CandidateID-',cast(cg.REFERENCE  as varchar(max)),'@noemail.com'),cem.email) as 'candidate-email'
	, e2.email as oldWorkEmail
	, we.EMAIL_ADD as 'candidate-workEmail'
	, he.EMAIL_ADD as homeemail
	, case 
          when (cast(telmobile.TEL_NUMBER  as varchar(max)) != '' and telmobile.TEL_NUMBER is not null) then telmobile.TEL_NUMBER
          when (cast(telhome.TEL_NUMBER as varchar(max)) != '' and telhome.TEL_NUMBER is not null) then telhome.TEL_NUMBER
          else telwork.TEL_NUMBER
		end as 'candidate-phone' --primary phone
	, telhome.TEL_NUMBER as 'candidate-homePhone'
	, telmobile.TEL_NUMBER as 'candidate-mobile'	 --candidate-phone
	, telwork.TEL_NUMBER as 'candidate-workPhone'
	--, 'PERMANENT' as 'candidate-jobTypes'
	--, cp.SALARYCURR as 'candidate-currentSalary'
	, ltrim(Stuff( 
			  Coalesce(',' + NULLIF(replace(P_TEMP,'Y','TEMPORARY'), 'N'), '')
			+ Coalesce(',' + NULLIF(replace(P_PERM,'Y','PERMANENT'), 'N'), '')
			+ Coalesce(',' + NULLIF(replace(P_CONTR,'Y','CONTRACT'), 'N'), '')
			, 1, 1, '')) as 'candidate-jobType'
	, cp.SALARY_REQ as 'candidate-currentSalary'
	, cp.SALARY_DES as 'candidate-desiredSalary'
	, cp.RATE_REQ as 'candidate-contractRate'
	--, ee2.ESTAB as 'candidate-schoolName' --'candidate-education'
	, ee3.QUAL as 'candidate-degreeName'
	--, ee.TO_DATE as 'candidate-graduationDate'
        --, ee3.DEGREE as 'candidate-degreeName'
	--, ee.gpa 'candidate-gpa'
	, pg.LINKEDIN as 'candidate-linkedin'
	, pg.SKYPE_ID as 'candidate-skype'
	, replace(skills.DESCRIPTION,'&amp;','&') 'candidate-skills'
	--, pg.EMPLOYER as 'candidate-employer1' 
    , pg.JOB_TITLE as 'candidate-jobtitle1'
    , ce.currentEmployer as 'candidate-employer1'
	--, left(st1.StartDate,10) as 'candidate-startdate1'
	--, left(et01.EndDate,10) as 'candidate-enddate1'
	, owner.owmerEmail as 'candidate-owners'
	--, t4.finame as 'Candidate File'
	--, replace(resume.DOC_ID,'.txt','.rtf') as 'candidate-resume1'
	, doc.docs as 'candidate-resume'
	--, photo.DOC_ID as 'candidate-photo'
	, e.canEducation as 'candidate-education'
	, replace(concat(
		concat('Candidate External ID: BB',cp.REFERENCE,char(10))
		, iif(pg.Salutation = '' OR pg.Salutation is NULL,'',concat ('Salutation: ',pg.Salutation,char(10)))
		, iif(he.EMAIL_ADD = '' OR he.EMAIL_ADD is NULL,'',concat ('Home Email: ',he.EMAIL_ADD,char(10)))
		, iif(we.EMAIL_ADD = '' OR we.EMAIL_ADD is NULL,'',concat ('Work Email: ',we.EMAIL_ADD,char(10)))
		, iif(status.Status = '' OR status.Status is NULL,'',concat ('Status: ',status.Status,char(10)))
		--, iif(rating.Ranking = '' OR rating.Ranking is NULL,'',concat ('Ranking: ',rating.Ranking,char(10)))
		--, iif(/*sal.SALARY_DES = '' OR*/ sal.SALARY_DES is NULL,'',concat ('Salary Desired: ',cast(sal.SALARY_DES as varchar(max)),char(10)))
		--, iif(sal.CurrentSalaryMonth = '' OR sal.CurrentSalaryMonth is NULL,'',concat ('Current Salary Month: ',sal.CurrentSalaryMonth,char(10)))
		--, iif(sal.ExpectedSalaryMonth = '' OR sal.ExpectedSalaryMonth is NULL,'',concat ('Expected Salary Month: ',sal.ExpectedSalaryMonth,char(10)))	
		--, iif(pg.ID_Passport = '' OR pg.ID_Passport is NULL,'',concat ('ID_Passport: ',pg.ID_Passport,char(10)))
		, iif(address2.locationName = '' or address2.locationName is null,'',concat('Address Line 2: ', replace(replace(replace(replace(address2.locationName,',,',','),', ,',', '),'  ',' '),' ,',','),char(10)))
		, iif(consname.ConsName = '' OR consname.ConsName is NULL,'',concat ('Consultant (Owner): ',consname.ConsName,char(10)))
		--, case when (owner.NAME = '' or owner.NAME is null) then '' else concat('Candidate Owner: ',owner.NAME,char(10)) end
		, iif(source.Source = '' OR source.Source is NULL,'',concat ('Source: ',source.Source,char(10)))
		, Coalesce('Imported From: ' + NULLIF(cg.IMPORT_FROM, '') + char(10), '')
		--, iif(referral.Referral = '' OR referral.Referral is NULL,'',concat ('Referral: ',referral.Referral,char(10)))
		--, case when (industry.DESCRIPTION = '' or industry.DESCRIPTION is null) then '' else concat('Industry: ',industry.DESCRIPTION,char(10)) end
		--, case when (JOBCATEGORY.DESCRIPTION = '' or JOBCATEGORY.DESCRIPTION is null) then '' else concat('Industry: ',JOBCATEGORY.DESCRIPTION,char(10)) end
		--, case when (JOBCATEGORY.DESCRIPTION = '' or JOBCATEGORY.DESCRIPTION is null) then '' else concat('Job Category: ',JOBCATEGORY.DESCRIPTION,char(10)) end
		--, case when (SubCategory.DESCRIPTION = '' or SubCategory.DESCRIPTION is null) then '' else concat('Sub Category: ',SubCategory.DESCRIPTION,char(10)) end
		, Coalesce('Looking for: ' + NULLIF(cp.LOOK_FOR, '') + char(10), '')
		, Coalesce('Notice Period: ' + NULLIF(cast(Noticeperiod.Noticeperiod as varchar(max)), '') + char(10), '')
		, Coalesce('Willing to Relocate: ' + NULLIF(replace(replace(cp.RELOCATE,'Y','Yes'),'N','No'), ''), '')
		, iif(et.CREATEDDATE  is NULL,'',concat ('Created Date: ',convert(varchar(30),et.CREATEDDATE,120),char(10)))
		, iif(et.CreatedBy = '' or et.CreatedBy is NULL,'',concat ('Created By: ',et.CreatedBy,char(10)))
		, iif(et.UPDATEDDATE  is NULL,'',concat ('Updated Date: ',convert(varchar(30),et.UPDATEDDATE,120),char(10)))
		, iif(et.UpdatedBy = '' or et.UpdatedBy is NULL,'',concat ('Updated By: ',et.UpdatedBy,char(10)))
		, iif(ci.LAST_CONT_DT  is NULL,'',concat ('Last Contacted Date: ',convert(varchar(10),ci.LAST_CONT_DT,120),char(10)))
		, iif(ci.ContactedBy = '' or ci.ContactedBy is NULL,'',concat ('Last Contacted By: ',ci.ContactedBy,char(10)))
		, iif(emp.REFERENCE is null,'',
			concat('-----EMPLOYMENT-----',char(10)
					, iif(emp.employmentType = '' or emp.employmentType is null,'',concat('  + Type: ',emp.employmentType,char(10)))
					, iif(emp.empStatus = '' or emp.empStatus is null,'',concat('  + Status: ',emp.empStatus,char(10)))
					, iif(emp.JOB_TITLE = '' or emp.JOB_TITLE is null,'',concat('  + JOb Title: ',emp.JOB_TITLE,char(10)))
					, iif(emp.PRV_CO = '' or emp.PRV_CO is null,'',concat('  + Prv Company: ',emp.PRV_CO,char(10)))
					, iif(emp.START_DT is null,'',concat('  + Start Date: ',convert(varchar(10),emp.START_DT,120),char(10)))
					, iif(emp.END_DT is null,'',concat('  + End Date: ',convert(varchar(10),emp.END_DT,120),char(10)))
			))
		, case when (cast(pg.PERSON_ID as varchar(max)) = '' or pg.PERSON_ID is null) then '' else concat('Personal ID: ',pg.PERSON_ID,char(10)) end
		--, note.NOTE,char(10)
		),'&amp;','') as 'candidate-note'
		, status.Status
	--, comment.NOTE as 'candidate-comments'

from PROP_CAND_GEN cg left join PROP_PERSON_GEN pg on cg.REFERENCE = pg.REFERENCE--23170 rows
left join PROP_CAND_PREF cp on cp.REFERENCE = cg.REFERENCE
left join title on cg.REFERENCE = title.REFERENCE
left join gender on cg.REFERENCE = gender.REFERENCE
--left join dob on cp.REFERENCE = dob.REFERENCE
left join WorkEmail we on cg.REFERENCE = we.REFERENCE
left join homeEmail he on cg.REFERENCE = he.REFERENCE
left join CandEmail cem on cg.REFERENCE = cem.REFERENCE
left join e1 ON cg.REFERENCE = e1.ID -- candidate-email
left join e2 ON cg.REFERENCE = e2.ID -- candidate-email
left join e3 ON cg.REFERENCE = e3.ID -- candidate-email
left join e4 ON cg.REFERENCE = e4.ID -- candidate-email
left join (SELECT REFERENCE, EMAIL_ADD from PROP_EMAIL where OCC_ID = 100000217) emailh2 on cg.REFERENCE = emailh2.REFERENCE
left join owner ON cg.REFERENCE = owner.PROP_PERSON_GEN_REFERENCE
--left join comment on pg.REFERENCE = comment.OWNER_ID
--left join note on pg.REFERENCE = note.OWNER_ID
--left join photo on cp.REFERENCE = photo.OWNER_ID
--left join resume on cp.REFERENCE = resume.OWNER_ID
left join doc on cg.REFERENCE = doc.OWNER_ID
--left join (select REFERENCE, CONFIG_NAME,STREET1,TOWN,COUNTY,COUNTRY,POST_CODE from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID where CONFIG_NAME = 'Primary') address ON cp.REFERENCE = address.REFERENCE
left join address1 ON cg.REFERENCE = address1.REFERENCE
left join address2 ON cg.REFERENCE = address2.REFERENCE
--left join (select REFERENCE, DESCRIPTION from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID INNER JOIN MD_MULTI_NAMES MN ON MN.ID = ADDRESS.COUNTRY where CONFIG_NAME = 'Primary') cnt ON cp.REFERENCE = cnt.REFERENCE
left join (select REFERENCE, TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Home') telhome on cg.REFERENCE = telhome.REFERENCE --candidate-homePhone
left join (select REFERENCE, TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Mobile') telmobile on cg.REFERENCE = telmobile.REFERENCE --candidate-mobile
left join (select REFERENCE, TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Work') telwork on cg.REFERENCE = telwork.REFERENCE --candidate-phone & candidate-workPhone
left join (select PERSON_GEN.REFERENCE, EMPLOYEE.NAME as ConsName from PROP_PERSON_GEN PERSON_GEN INNER JOIN PROP_OWN_CONS CONS ON PERSON_GEN.REFERENCE = CONS.REFERENCE INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT where CONFIG_NAME = 'Permanent') consName on cg.REFERENCE = consname.REFERENCE
left join (select REFERENCE, STREET2 from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID where CONFIG_NAME = 'Primary') street2 on cp.REFERENCE = street2.REFERENCE --and ADDRESS.REFERENCE = <<PROP_PERSON_GEN.REFERENCE>>
--left join (SELECT REFERENCE, iif(RELOCATE = 'Y','Yes','No') AS Relocate FROM PROP_CAND_PREF) relocate on cp.REFERENCE = relocate.REFERENCE
--left join (SELECT REFERENCE, MN.DESCRIPTION as Ranking from PROP_PERSON_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_PERSON_GEN.RATING) rating on pg.REFERENCE = rating.REFERENCE
--left join (SELECT REFERENCE, SALARY_DES, MN.DESCRIPTION as CurrentSalaryMonth,MN2.DESCRIPTION as ExpectedSalaryMonth FROM PROP_CAND_PREF CAND_PREF LEFT JOIN MD_MULTI_NAMES MN ON MN.ID = CAND_PREF.SALARY_TYPE LEFT JOIN MD_MULTI_NAMES MN2 ON MN2.ID = CAND_PREF.EXPSAL_TYPE) sal on pg.REFERENCE = sal.REFERENCE --where SALARY_DES is not null and MN.DESCRIPTION is not null and MN2.DESCRIPTION is not null
left join skills ON cg.REFERENCE = skills.REFERENCE -- candidate-email & candidate-workEmail
--left join (SELECT REFERENCE, DESCRIPTION = STUFF((SELECT DISTINCT ', ' + DESCRIPTION FROM location b WHERE DESCRIPTION != '' and b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM location a GROUP BY REFERENCE) pl on cp.REFERENCE = pl.REFERENCE
--left join (SELECT REFERENCE, DESCRIPTION = STUFF((SELECT DISTINCT ', ' + DESCRIPTION FROM industry b WHERE b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM industry a GROUP BY REFERENCE) industry ON pg.REFERENCE = industry.REFERENCE -- industry
--left join (SELECT REFERENCE, DESCRIPTION = STUFF((SELECT DISTINCT ', ' + DESCRIPTION FROM JobCategory b WHERE b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM JobCategory a GROUP BY REFERENCE) JobCategory ON pg.REFERENCE = JobCategory.REFERENCE -- JobCategory
--left join (SELECT REFERENCE, DESCRIPTION = STUFF((SELECT DISTINCT ', ' + DESCRIPTION FROM SubCategory b WHERE b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM SubCategory a GROUP BY REFERENCE) SubCategory ON pg.REFERENCE = SubCategory.REFERENCE -- SubCategory
left join (SELECT REFERENCE, ESTAB = STUFF((SELECT DISTINCT '; ' + ESTAB FROM PROP_EDU_ESTAB b WHERE b.ESTAB != '' and b.REFERENCE = a.REFERENCE FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2,'') FROM PROP_EDU_ESTAB a GROUP BY REFERENCE) ee2 on pg.REFERENCE = ee2.REFERENCE
left join (SELECT REFERENCE, QUAL = STUFF((SELECT DISTINCT '; ' + QUAL FROM PROP_EDU_ESTAB b WHERE b.qual != '' and b.REFERENCE = a.REFERENCE FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') FROM PROP_EDU_ESTAB a GROUP BY REFERENCE) ee3 on pg.REFERENCE = ee3.REFERENCE
--left join (SELECT REFERENCE, STUFF((SELECT distinct ', ' + REPLACE(MN.DESCRIPTION COLLATE Latin1_General_BIN, char(26), '') FROM PROP_CAND_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_CAND_GEN.VISA_TYPE where REFERENCE = a.REFERENCE FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') As VISA FROM PROP_CAND_GEN as a GROUP BY a.REFERENCE) visa on cp.REFERENCE = visa.REFERENCE
left join (SELECT REFERENCE, STUFF((SELECT distinct ', ' + REPLACE(MN.DESCRIPTION COLLATE Latin1_General_BIN, char(26), '') FROM PROP_CAND_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_CAND_GEN.STATUS and MN.LANGUAGE = 1 where REFERENCE = a.REFERENCE FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') As Status FROM PROP_CAND_GEN as a GROUP BY a.REFERENCE) status on cg.REFERENCE = status.REFERENCE
left join (SELECT REFERENCE, STUFF((SELECT distinct ', ' + REPLACE(MN.DESCRIPTION COLLATE Latin1_General_BIN, char(26), '') FROM PROP_CAND_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_CAND_GEN.SOURCE and MN.LANGUAGE = 1 where REFERENCE = a.REFERENCE FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') As Source FROM PROP_CAND_GEN as a GROUP BY a.REFERENCE) source on cg.REFERENCE = source.REFERENCE --21159
left join (SELECT REFERENCE, STUFF((SELECT distinct ', ' + REPLACE(MN.DESCRIPTION COLLATE Latin1_General_BIN, char(26), '') FROM PROP_CAND_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_CAND_GEN.NOT_PERIOD and MN.LANGUAGE = 1 where REFERENCE = a.REFERENCE FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') As Noticeperiod FROM PROP_CAND_GEN as a GROUP BY a.REFERENCE) Noticeperiod on cp.REFERENCE = Noticeperiod.REFERENCE --22765
left join Nationality nal on cg.REFERENCE = nal.REFERENCE
--left join (SELECT REFERENCE, STUFF((SELECT distinct ', ' + REPLACE(PG.CHIFULLNAME COLLATE Latin1_General_BIN, char(26), '') FROM PROP_CAND_GEN INNER JOIN PROP_PERSON_GEN PG ON PG.REFERENCE = PROP_CAND_GEN.REFERRAL where PG.REFERENCE = a.REFERENCE FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') As referral FROM PROP_CAND_GEN as a GROUP BY a.REFERENCE) referral on pg.REFERENCE = referral.REFERENCE --21193
left join createdUpdatedInfo et on cg.REFERENCE = et.ENTITY_ID
left join contactedInfo ci on cg.REFERENCE = ci.REFERENCE 
left join curEmployer ce on cg.REFERENCE = ce.REFERENCE
left join employment emp on cg.REFERENCE = emp.canID
left join education e on cg.REFERENCE = e.REFERENCE
where et.CREATEDDATE > '2017-04-30'--and emp.REFERENCE is not null--and ce.currentEmployer is not null--and ci.contactedBy is not null\
or status.Status in ('Temping For Us','Working For A Competitor','Placed By Us')
)
--and cg.IMPORT_FROM is not null 
--and cp.REFERENCE = 116711097855
--and (pg.SKYPE_ID is not null or pg.SKYPE_ID <>'')
--status.Status like '%Temping For Us%' or status.Status like '%Working For A Competitor%' or status.Status like '%Placed By Us%'

select * from candidate where [candidate-email] like 'DUPLICATE%'

--select concat('BB',ins.REFERENCE) as exId
--from PROP_IND_SECT ins INNER JOIN MD_MULTI_NAMES MN ON MN.ID = ins.INDUSTRY
--where concat('BB',ins.REFERENCE) in (select [candidate-externalId] from candidate) and LANGUAGE =1