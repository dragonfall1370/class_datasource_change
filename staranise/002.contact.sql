
with
  contact0 (CLIENT,CONTACT,rn) as (SELECT CLIENT,CONTACT,ROW_NUMBER() OVER(PARTITION BY cg.CONTACT ORDER BY cg.BISUNIQUEID DESC) AS rn FROM PROP_X_CLIENT_CON cg)
, contact as (select CLIENT,CONTACT from contact0 where rn = 1)


-- EMAIL
, mail1 (ID,email) as (select REFERENCE, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(EMAIL_LINK
        ,'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),CHAR(9),' ') as mail from PROP_EMAIL where EMAIL_LINK like '%_@_%.__%' ) -- from bullhorn1.Candidate
, mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
/*, mail5 (ID,email1,email2) as (
		select pe.ID, email as email1, we.email2 as email2 from mail4 pe
		left join (select ID, email as email2 from mail4 where rn = 2) we on we.ID = pe.ID
		where pe.rn = 1 ) */
, mail5 as (select ID, email from mail4 where rn = 1)
, maildup0 (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM mail4)
, maildup (ID,email,rn) as (select distinct ID,email,rn from maildup0 where rn > 1)
, oe2 as (select ID, email from mail4 where rn = 2)
, oe3 as (select ID, email from mail4 where rn = 3)
, oe4 as (select ID, email from mail4 where rn = 4)
--select distinct ID,email from maildup where rn > 2 --20313


, tel(REFERENCE, TEL_NUMBER) as (SELECT REFERENCE, STUFF((SELECT DISTINCT ',' + ltrim(rtrim(replace(TEL_NUMBER,',',' '))) from PROP_TELEPHONE WHERE TEL_NUMBER != '' and TEL_NUMBER is not null and TEL_NUMBER <> '' and REFERENCE = a.REFERENCE FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS URLList FROM PROP_TELEPHONE as a GROUP BY a.REFERENCE)
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

, dob (BISUNIQUEID,REFERENCE,DT_OF_BIRTH,rn) as (SELECT BISUNIQUEID,REFERENCE,DT_OF_BIRTH,ROW_NUMBER() OVER(PARTITION BY cg.REFERENCE ORDER BY cg.BISUNIQUEID DESC) AS rn FROM PROP_CAND_GEN cg)

, owner as ( select PERSON_GEN.REFERENCE as PROP_PERSON_GEN_REFERENCE,PERSON_GEN.FIRST_NAME,PERSON_GEN.LAST_NAME,EMPLOYEE.REFERENCE as PROP_EMPLOYEE_GEN_REFERENCE,EMPLOYEE.NAME,mail.EMAIL_LINK,EMPLOYEE.USER_REF from PROP_PERSON_GEN PERSON_GEN 
	INNER JOIN PROP_OWN_CONS CONS ON PERSON_GEN.REFERENCE = CONS.REFERENCE INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT 
	left join (SELECT REFERENCE, EMAIL_LINK = STUFF((SELECT DISTINCT ', ' + EMAIL_LINK FROM PROP_EMAIL b WHERE b.EMAIL_LINK like '%@%' and b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM PROP_EMAIL a GROUP BY REFERENCE) mail ON EMPLOYEE.REFERENCE = mail.REFERENCE
	where CONFIG_NAME = 'Permanent'
	--and PERSON_GEN.FIRST_NAME like '%Christian%' and PERSON_GEN.LAST_NAME like '%Kwan%'
	)
--select * from owner


--select top 100
select distinct
	ccc.CONTACT as 'contact-externalId'
	, ccc.CLIENT as 'contact-companyId'
	, replace(pg.FIRST_NAME,'?','') as 'contact-firstName'
	, case when ( replace(pg.LAST_NAME,'?','') = '' or pg.LAST_NAME is null) then 'No Lastname' else replace(pg.LAST_NAME,'?','') end as 'contact-lastName'
	, pg.MIDDLE_NAME as 'contact-middleName'
	--, ltrim(replace(replace(replace(replace(replace(replace(pe.EMAIL_LINK,'�',''),'?',''),'  ',''),char(9),''),'/',','),'mailto:','')) as 'contact-email'
	--, case when (pe.rn > 1 and pe.String is not null) then concat('DUPLICATED_',pe.rn,'_',pe.String) else pe.String end as 'contact-email'
	--, case when (mail5.email != '' and mail5.email is not null and maildup.rn) then concat('DUPLICATED_',maildup.rn,'_',maildup.email) else mail5.email end as 'contact-email)'
        , iif(maildup.ID in (select ID from mail5),concat('DUPLICATED_',maildup.rn,'_',maildup.email),mail5.email) as 'contact-email'
	, tel.TEL_NUMBER as 'contact-phone'
	, pg.JOB_TITLE as 'contact-jobTitle'
	, pg.LINKED_IN as 'contact-linkedin'
	, owner.EMAIL_LINK as 'contact-owners'
        , Stuff(  Coalesce('Other email: ' + NULLIF(concat(oe2.email,' ',oe3.email,' ',oe4.email), '') + char(10), '')
                + Coalesce('Full Name: ' + NULLIF(pg.chifullname, '') + char(10), '')
                + Coalesce('Chinese Name: ' + NULLIF(cast(pg.chinesename as nvarchar(max)), '') + char(10), '')
                + Coalesce('Mobile Phone: ' + NULLIF(cast(telmobile.TEL_NUMBER as nvarchar(max)), '') + char(10), '')
                + Coalesce('Home Phone: ' + NULLIF(cast(telhome.TEL_NUMBER as nvarchar(max)), '') + char(10), '')
                + Coalesce('Work Phone: ' + NULLIF(cast(telwork.TEL_NUMBER as nvarchar(max)), '') + char(10), '')
                + Coalesce('Address: ' + NULLIF(cast(address.STREET1 as nvarchar(max)), '') + char(10), '')
                + Coalesce('City: ' + NULLIF(cast(address.TOWN as nvarchar(max)), '') + char(10), '')
                + Coalesce('State: ' + NULLIF(cast(address.state as nvarchar(max)), '') + char(10), '')
                + Coalesce('Country: ' + NULLIF(cast(address.COUNTRY as nvarchar(max)), '') + char(10), '')
                + Coalesce('Zip Code: ' + NULLIF(cast(address.POST_CODE as nvarchar(max)), '') + char(10), '')
                + Coalesce('Contact Owner: ' + NULLIF(cast(owner.NAME as nvarchar(max)), '') + char(10), '')
                , 1, 0, '') as 'contact-note'

	, replace(doc.DOC_ID,'.txt','.rtf') as 'contact-document'
	--, left(replace(d.Notes,'&#x0D;',''),32000) as 'contact-comments'

 --select count(distinct cc.CONTACT) --14892 rows
from PROP_X_CLIENT_CON cc
left join contact ccc on cc.CONTACT = ccc.CONTACT
left join (select REFERENCE,FIRST_NAME,LAST_NAME, MIDDLE_NAME, JOB_TITLE, LINKED_IN,chifullname,chinesename from PROP_PERSON_GEN) pg on ccc.CONTACT = pg.REFERENCE
left join mail5 ON ccc.CONTACT = mail5.id -- PRIMARY-EMAIL
left join maildup ON ccc.CONTACT = maildup.id -- DUPLICATED-EMAIL
left join oe2 ON ccc.CONTACT = oe2.id -- Other-EMAIL
left join oe3 ON ccc.CONTACT = oe3.id -- Other-EMAIL
left join oe4 ON ccc.CONTACT = oe4.id -- Other-EMAIL
left join tel ON ccc.CONTACT = tel.REFERENCE
left join owner ON ccc.CONTACT = owner.PROP_PERSON_GEN_REFERENCE
--left join doc on ccc.CONTACT = doc.OWNER_ID
left join (select REFERENCE,CONFIG_NAME,STREET1,TOWN,STATE,COUNTRY,POST_CODE from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID where CONFIG_NAME = 'Primary') address on ccc.CONTACT = address.REFERENCE
left join (select REFERENCE,TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Home') telhome on ccc.CONTACT = telhome.REFERENCE --candidate-homePhone
left join (select REFERENCE,TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Mobile') telmobile on ccc.CONTACT = telmobile.REFERENCE --candidate-mobile
left join (select REFERENCE,TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Work') telwork on ccc.CONTACT = telwork.REFERENCE --candidate-phone & candidate-workPhone
--left join photo on pg.REFERENCE = photo.OWNER_ID
--and mail5.email is not null
--and pg.chinesename is not null
--and ccc.CONTACT in (396970,397884,397965,398036,406635,409577,410217,414232,415647,420063,436059,528026,550535,558455,567093,588173,588743,591415,592958,596988,599869,626195,629811,631637,659586,661339,676728,718815,728977,753998,760676,801076,814991)
--and pedup.rn = 2
--and ccc.CONTACT in (394855,394857,394858,394859,394860,394941,422556,440045,520613,538555,556969,596437,603166,672412,682563,715196,724260,767526,775203,808647,816694,838398)
--and pg.FIRST_NAME like '%Christopher%' and pg.LAST_NAME like '%Tang%'
--and pg.FIRST_NAME like '%Susanna%' and pg.LAST_NAME like '%Poon%'
--and mail.EMAIL_LINK like '%chris.tang@staranise.com.hk%'
--and pe.string like '%)%'
--and own.EMPLOYEE_NAME is not null
--and ccc.CLIENT = 395108
--and mail.EMAIL_LINK like '%david.fleming@venetian.com.mo%'
order by ccc.CONTACT desc

/*
--select EMPLOYEE.REFERENCE,EMPLOYEE.NAME,EMPLOYEE.USER_REF
select PERSON_GEN.REFERENCE,PERSON_GEN.FIRST_NAME,PERSON_GEN.LAST_NAME,EMPLOYEE.NAME,mail.EMAIL_LINK
--,mail.EMAIL_LINK 
from PROP_PERSON_GEN PERSON_GEN 
INNER JOIN PROP_OWN_CONS CONS ON PERSON_GEN.REFERENCE = CONS.REFERENCE INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT 
left join (SELECT REFERENCE, EMAIL_LINK = STUFF((SELECT DISTINCT ', ' + EMAIL_LINK FROM PROP_EMAIL b WHERE b.EMAIL_LINK like '%@%' and b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM PROP_EMAIL a GROUP BY REFERENCE) mail ON PERSON_GEN.REFERENCE = mail.REFERENCE
where CONFIG_NAME = 'Permanent'
and PERSON_GEN.FIRST_NAME like '%Christian%' and PERSON_GEN.LAST_NAME like '%Kwan%'
and mail.EMAIL_LINK is not null
*/
