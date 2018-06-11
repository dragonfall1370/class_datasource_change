
with
  contact0 (CLIENT,CONTACT,rn) as (SELECT CLIENT,CONTACT,ROW_NUMBER() OVER(PARTITION BY cg.CONTACT ORDER BY cg.BISUNIQUEID DESC) AS rn FROM PROP_X_CLIENT_CON cg)

, contact as (select CLIENT,CONTACT from contact0 where rn = 1)


-- EMAIL
, mail1 (ID,email) as (
	select REFERENCE, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(EMAIL_ADD
        ,'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),CHAR(9),' ') as mail 
		from PROP_EMAIL where EMAIL_ADD like '%_@_%.__%' ) -- from bullhorn1.Candidate

, mail2 (ID,email) as (
	SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String 
	FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))

, mail3 (ID,email) as (
	SELECT ID, 
			case 
				when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) 
				when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) 
				else email end as email 
	from mail2 
	WHERE email like '%_@_%.__%')

, mail4 (ID,email,rn) as ( 
	SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) 
	FROM mail3 )
/*, mail5 (ID,email1,email2) as (
		select pe.ID, email as email1, we.email2 as email2 from mail4 pe
		left join (select ID, email as email2 from mail4 where rn = 2) we on we.ID = pe.ID
		where pe.rn = 1 ) */
, mail5 as (
	select ID, email from mail4 where rn = 1)

, maildup0 (ID,email,rn) as (
	SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn 
	FROM mail4)

, maildup (ID,email,rn) as (
	select distinct ID,email,rn 
	from maildup0 
	where rn > 1)

, oe2 as (
	select ID, email 
	from mail4 
	where rn = 2)

, oe3 as (
	select ID, email 
	from mail4 
	where rn = 3)

, oe4 as (
	select ID, email 
	from mail4 
	where rn = 4)
--select distinct ID,email from maildup where rn > 2 --20313

-----------------------------------Email using my way: Get wwork email and home email to note
, WorkEmail as (select REFERENCE,replace(EMAIL_ADD,',','.') EMAIL_ADD
from PROP_EMAIL TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID 
where CONFIG_NAME = 'work' and EMAIL_ADD like '%_@_%.__%')

, homeEmail as (select REFERENCE,replace(EMAIL_ADD,',','.') EMAIL_ADD
from PROP_EMAIL TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID 
where CONFIG_NAME = 'home' and EMAIL_ADD like '%_@_%.__%')
-------------each contact has only 1 email so no need to combine 

, tempEmail as (select ccc.CONTACT, replace(coalesce(we.EMAIL_ADD,he.EMAIL_ADD),' ','') as email
from contact ccc left join WorkEmail we on ccc.CONTACT = we.REFERENCE
				left join homeEmail he on ccc.CONTACT = he.REFERENCE)

, tempEmail1 as (select CONTACT, email, ROW_NUMBER() OVER(PARTITION BY email ORDER BY CONTACT ASC) AS rn
from tempEmail where email is not null)

, ContactEmail as (select CONTACT, 
case 
when rn=1 then email
else concat(rn,'_',(email))
end as Email
from tempEmail1)

, tel(REFERENCE, TEL_NUMBER) as (
	SELECT REFERENCE, STUFF((SELECT DISTINCT ',' + ltrim(rtrim(replace(TEL_NUMBER,',',' '))) from PROP_TELEPHONE WHERE TEL_NUMBER != '' and TEL_NUMBER is not null and TEL_NUMBER <> '' and REFERENCE = a.REFERENCE FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS URLList FROM PROP_TELEPHONE as a GROUP BY a.REFERENCE)
--select * from tel

/*
--select top 100 * from DOCUMENTS where updated_date is null or updated_date = ''
, with doc_note (OWNER_ID, DOC_ID, NOTE) as (
        SELECT D.OWNER_ID, D.DOC_ID, D.DOC_NAME --,DC.NOTE 
        from DOCUMENTS D 
        --left join DOCUMENT_CONTENT DC on DC.DOC_ID = d.DOC_ID 
        WHERE D.DOC_CATEGORY = 6532841 
        AND D.FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip'))
--select top 100 * from doc_note where OWNER_ID = 394903
, doc(OWNER_ID, NOTE) as (SELECT OWNER_ID, STUFF((SELECT char(10) + 'NOTE: ' + NOTE from doc_note WHERE NOTE != '' and NOTE is not null and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 0, '')  AS doc FROM doc_note as a GROUP BY a.OWNER_ID)
select top 50 * from doc where OWNER_ID = 394903
*/
, doc as (
	SELECT OWNER_ID, STUFF((SELECT DISTINCT ',' + cast(DOC_ID as varchar(max)) + '_' 
						+ replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(DOC_NAME,'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':',''),char(10),''),char(13),'')
						+ '.' + replace(coalesce(nullif(FILE_EXTENSION,''),PREVIEW_TYPE),'txt','rtf')
						from DOCUMENTS 
						WHERE OWNER_ID = a.OWNER_ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS docs
FROM DOCUMENTS as a
--WHERE DOC_ID = 101883385                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
GROUP BY a.OWNER_ID)

, dob (BISUNIQUEID,REFERENCE,DT_OF_BIRTH,rn) as (SELECT BISUNIQUEID,REFERENCE,DT_OF_BIRTH,ROW_NUMBER() OVER(PARTITION BY cg.REFERENCE ORDER BY cg.BISUNIQUEID DESC) AS rn FROM PROP_CAND_GEN cg)

, tempowners as ( select PERSON_GEN.REFERENCE as PROP_PERSON_GEN_REFERENCE,PERSON_GEN.FIRST_NAME,PERSON_GEN.LAST_NAME,EMPLOYEE.REFERENCE as PROP_EMPLOYEE_GEN_REFERENCE,EMPLOYEE.NAME,mail.EMAIL_ADD,EMPLOYEE.USER_REF from PROP_PERSON_GEN PERSON_GEN 
	INNER JOIN PROP_OWN_CONS CONS ON PERSON_GEN.REFERENCE = CONS.REFERENCE INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT 
	left join (SELECT REFERENCE, EMAIL_ADD = STUFF((SELECT DISTINCT ', ' + EMAIL_ADD FROM PROP_EMAIL b WHERE b.EMAIL_ADD like '%@%' and b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM PROP_EMAIL a GROUP BY REFERENCE) mail ON EMPLOYEE.REFERENCE = mail.REFERENCE
	where CONFIG_NAME = 'Permanent'
	--and PERSON_GEN.FIRST_NAME like '%Christian%' and PERSON_GEN.LAST_NAME like '%Kwan%'
	)
--select * from owner

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

, tempaddress as (
	select REFERENCE,CONFIG_NAME,STREET1,STREET2,TOWN,COUNTY,COUNTRY,LOCALITY,POST_CODE, DESCRIPTION
		, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(STREET1, ''), '')
			+ Coalesce(', ' + NULLIF(STREET2, ''), '')
			+ Coalesce(', ' + NULLIF(TOWN, ''), '')
			+ Coalesce(', ' + NULLIF(LOCALITY, ''), '')
			+ Coalesce(', ' + NULLIF(COUNTY, ''), '')
			+ Coalesce(', ' + NULLIF(POST_CODE, ''), '')
			+ Coalesce(', ' + NULLIF(DESCRIPTION, ''), '')
			, 1, 1, '')) as 'locationName'
	from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID
								LEFT JOIN MD_MULTI_NAMES MN ON MN.ID = ADDRESS.COUNTRY  and MN.LANGUAGE = 1
	where CONFIG_NAME = 'Primary')	

, address as (select * from tempaddress where locationName is not null)

, address2 as (
	select REFERENCE,CONFIG_NAME,STREET1,STREET2,TOWN,COUNTY,COUNTRY,LOCALITY,POST_CODE, DESCRIPTION
		, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(STREET1, ''), '')
			+ Coalesce(', ' + NULLIF(STREET2, ''), '')
			+ Coalesce(', ' + NULLIF(TOWN, ''), '')
			+ Coalesce(', ' + NULLIF(LOCALITY, ''), '')
			+ Coalesce(', ' + NULLIF(COUNTY, ''), '')
			+ Coalesce(', ' + NULLIF(POST_CODE, ''), '')
			+ Coalesce(', ' + NULLIF(DESCRIPTION, ''), '')
			, 1, 1, '')) as 'locationName'
	from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID
								LEFT JOIN MD_MULTI_NAMES MN ON MN.ID = ADDRESS.COUNTRY  and MN.LANGUAGE = 1
	where CONFIG_NAME <> 'Primary')


--select top 100
select --distinct
	concat('BB',ccc.CONTACT) as 'contact-externalId'
	, concat('BB',ccc.CLIENT) as 'contact-companyId'
	, replace(pg.FIRST_NAME,'?','') as 'contact-firstName'
	, case when ( replace(pg.LAST_NAME,'?','') = '' or pg.LAST_NAME is null) then 'No Lastname' else replace(pg.LAST_NAME,'?','') end as 'contact-lastName'
	, pg.MIDDLE_NAME as 'contact-middleName'
	--, ltrim(replace(replace(replace(replace(replace(replace(pe.EMAIL_ADD,'�',''),'?',''),'  ',''),char(9),''),'/',','),'mailto:','')) as 'contact-email'
    --, iif(maildup.ID in (select ID from mail5),concat(maildup.rn,'_',maildup.email),mail5.email) as 'contact-email'
	, replace(coalesce(we.EMAIL_ADD,he.EMAIL_ADD),' ','') as 'contact-email1'
	, ce.email as 'contact-email'
	, coalesce(telwork.TEL_NUMBER,telmobile.TEL_NUMBER,telhome.TEL_NUMBER) as 'contact-phone'
	, pg.JOB_TITLE as 'contact-jobTitle'
	, pg.LINKEDIN as 'contact-linkedin'
	, owner.owmerEmail as 'contact-owners'
	, doc.docs AS 'contact-document'
    , Stuff(Coalesce('Contact External ID: BB' + cast(ccc.CONTACT as nvarchar(max)) + char(10), '')
			+ Coalesce('Salutation: ' + NULLIF(pg.SALUTATION, '') + char(10), '')
			+ Coalesce('Work Email: ' + NULLIF(we.EMAIL_ADD, '') + char(10), '')
			+ Coalesce('Home Email: ' + NULLIF(he.EMAIL_ADD, '') + char(10), '')
            --+ Coalesce('Full Name: ' + NULLIF(pg.chifullname, '') + char(10), '')
            --+ Coalesce('Chinese Name: ' + NULLIF(cast(pg.chinesename as nvarchar(max)), '') + char(10), '')
            + Coalesce('Mobile Phone: ' + NULLIF(cast(telmobile.TEL_NUMBER as nvarchar(max)), '') + char(10), '')
            + Coalesce('Home Phone: ' + NULLIF(cast(telhome.TEL_NUMBER as nvarchar(max)), '') + char(10), '')
            + Coalesce('Work Phone: ' + NULLIF(cast(telwork.TEL_NUMBER as nvarchar(max)), '') + char(10), '')
			+ Coalesce('Address Line 1: ' + NULLIF(replace(replace(replace(replace(address.locationName,',,',','),', ,',', '),'  ',' '),' ,',','), '') + char(10), '')
			+ Coalesce('Address Line 2: ' + NULLIF(replace(replace(replace(replace(address2.locationName,',,',','),', ,',', '),'  ',' '),' ,',','), '') + char(10), '')
            --+ Coalesce('Address: ' + NULLIF(cast(address.STREET1 as nvarchar(max)), '') + char(10), '')
            --+ Coalesce('City: ' + NULLIF(cast(address.TOWN as nvarchar(max)), '') + char(10), '')
            --+ Coalesce('State: ' + NULLIF(cast(address.county as nvarchar(max)), '') + char(10), '')
            --+ Coalesce('Country: ' + NULLIF(cast(address.COUNTRY as nvarchar(max)), '') + char(10), '')
           -- + Coalesce('Zip Code: ' + NULLIF(cast(address.POST_CODE as nvarchar(max)), '') + char(10), '')
            + Coalesce('Contact Owner: ' + NULLIF(cast(owner.NAME as nvarchar(max)), '') + char(10), '')
			+ Coalesce('PA NAME: ' + NULLIF(pcg.PA_NAME, '') + char(10), '')
			+ Coalesce('PA TELEPHONE: ' + NULLIF(pcg.PA_TELE, '') + char(10), '')
			+ Coalesce('PA EMAIL: ' + NULLIF(pcg.PA_EMAIL, '') + char(10), '')
            , 1, 0, '') as 'contact-note'
	--, replace(doc.DOC_ID,'.txt','.rtf') as 'contact-document'
	--, left(replace(d.Notes,'&#x0D;',''),32000) as 'contact-comments'

 --select count(distinct cc.CONTACT) --14892 rows
from PROP_X_CLIENT_CON cc
left join contact ccc on cc.CONTACT = ccc.CONTACT
left join (select REFERENCE,FIRST_NAME,LAST_NAME, MIDDLE_NAME, JOB_TITLE, LINKEDIN, SALUTATION from PROP_PERSON_GEN) pg on ccc.CONTACT = pg.REFERENCE
left join mail5 ON ccc.CONTACT = mail5.id -- PRIMARY-EMAIL
left join maildup ON ccc.CONTACT = maildup.id -- DUPLICATED-EMAIL
left join oe2 ON ccc.CONTACT = oe2.id -- Other-EMAIL
left join oe3 ON ccc.CONTACT = oe3.id -- Other-EMAIL
left join oe4 ON ccc.CONTACT = oe4.id -- Other-EMAIL
left join tel ON ccc.CONTACT = tel.REFERENCE
left join owner ON ccc.CONTACT = owner.PROP_PERSON_GEN_REFERENCE
--left join doc on ccc.CONTACT = doc.OWNER_ID
left join address on ccc.CONTACT = address.REFERENCE
left join address2 on ccc.CONTACT = address2.REFERENCE
left join (select REFERENCE,TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Home') telhome on ccc.CONTACT = telhome.REFERENCE --candidate-homePhone
left join (select REFERENCE,TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Mobile') telmobile on ccc.CONTACT = telmobile.REFERENCE --candidate-mobile
left join (select REFERENCE,TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Work') telwork on ccc.CONTACT = telwork.REFERENCE --candidate-phone & candidate-workPhone
left join WorkEmail we on ccc.CONTACT = we.REFERENCE
left join homeEmail he on ccc.CONTACT = he.REFERENCE
left join ContactEmail ce on ccc.CONTACT = ce.CONTACT
left join doc on ccc.CONTACT = doc.OWNER_ID
left join PROP_CAND_GEN pcg on ccc.CONTACT = pcg.REFERENCE
--where PA_NAME is not null-- and PA_NAME <>''
--where ccc.CONTACT = 116679990546
UNION ALL
select 'BB9999999','BB9999999','Default','Contact','','','','','','','','','This is default contact from Data Import'

/*
--select EMPLOYEE.REFERENCE,EMPLOYEE.NAME,EMPLOYEE.USER_REF
select PERSON_GEN.REFERENCE,PERSON_GEN.FIRST_NAME,PERSON_GEN.LAST_NAME,EMPLOYEE.NAME,mail.EMAIL_ADD
--,mail.EMAIL_ADD 
from PROP_PERSON_GEN PERSON_GEN 
INNER JOIN PROP_OWN_CONS CONS ON PERSON_GEN.REFERENCE = CONS.REFERENCE INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT 
left join (SELECT REFERENCE, EMAIL_ADD = STUFF((SELECT DISTINCT ', ' + EMAIL_ADD FROM PROP_EMAIL b WHERE b.EMAIL_ADD like '%@%' and b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM PROP_EMAIL a GROUP BY REFERENCE) mail ON PERSON_GEN.REFERENCE = mail.REFERENCE
where CONFIG_NAME = 'Permanent'
and PERSON_GEN.FIRST_NAME like '%Christian%' and PERSON_GEN.LAST_NAME like '%Kwan%'
and mail.EMAIL_ADD is not null
*/
