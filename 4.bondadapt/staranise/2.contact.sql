
with
contact (BISUNIQUEID,CLIENT,CONTACT,rn) as (SELECT BISUNIQUEID,CLIENT,CONTACT,ROW_NUMBER() OVER(PARTITION BY cg.CONTACT ORDER BY cg.BISUNIQUEID DESC) AS rn FROM PROP_X_CLIENT_CON cg)
--select count(*) from contact where rn = 1

--, mail as (SELECT REFERENCE, EMAIL_LINK = STUFF((SELECT DISTINCT ', ' + EMAIL_LINK FROM PROP_EMAIL b WHERE b.EMAIL_LINK like '%@%' and b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM PROP_EMAIL a GROUP BY REFERENCE)
--select * from mail2 where EMAIL_LINK like '%(%'
--, email as (SELECT REFERENCE,EMAIL_LINK,ROW_NUMBER() OVER(PARTITION BY ltrim(pe.REFERENCE) ORDER BY pe.REFERENCE DESC) AS rn FROM PROP_EMAIL pe WHERE EMAIL_LINK like '%@%' and EMAIL_LINK != '' and EMAIL_LINK IS NOT NULL )
--select * from email where EMAIL_LINK = ''
------------
-- MAIL
------------
, mail1 (userID,email) as (select REFERENCE, ltrim(rtrim(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(EMAIL_LINK,'/',''),'<',''),'>',''),'(',''),')',''),':',' '),'.@',''),'+',' '),CHAR(9),' '),'      ',''),'•',' '),'''',' '),'&',' '),';',' '),'@@','@'))) as email from PROP_EMAIL where EMAIL_LINK is not null and EMAIL_LINK != '')
--select *,len(email) from mail1 where userid in (556358,564244,696401,806078,760088,743297,811988,407286) and email != '' and email != 'null' --,617025,615922,623956,625575,623014,599240,616626,623443,556358,564244,696401,711436,711367,623852,598451,632180,410107,495050,806078,760088,743297)
--SELECT userid, concat('<M>',REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>'),'</M>') AS Data FROM mail1 where email != '' and email is not null

, mail2 (userid,String) as (
	SELECT userid, Split.a.value('.', 'VARCHAR(max)') AS String
	FROM  (SELECT userid, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1 where email != '' and email != 'null') AS A 
	--FROM  (SELECT userid, cast(concat('<M>',REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>'),'</M>') as XML) AS Data FROM mail1 where email != '' and email is not null) AS A 
	CROSS APPLY Data.nodes ('/M') AS Split(a))
--select * from mail2 where userid in (396970,397884,397965,398036,406635,409577,410217,414232,415647,420063,436059,528026,550535,558455,567093,588173,588743,591415,592958,596988,599869,626195,629811,631637,659586,661339,676728,718815,728977,753998,760676,801076,814991)
, mail2a (userID,String,rn) as (SELECT userID,String,ROW_NUMBER() OVER(PARTITION BY userID ORDER BY userID DESC) AS rn FROM mail2 WHERE String like '%_@_%_.__%')
--select * from mail2a where userid in (396970,397884,397965,398036,406635,409577,410217,414232,415647,420063,436059,528026,550535,558455,567093,588173,588743,591415,592958,596988,599869,626195,629811,631637,659586,661339,676728,718815,728977,753998,760676,801076,814991)
, mail2b (userID,String,rn) as (SELECT userID,String,ROW_NUMBER() OVER(PARTITION BY String ORDER BY userID DESC) AS rn FROM mail2 WHERE String like '%_@_%_.__%')

, mail3 (userID,String) as (SELECT userid, STUFF((SELECT DISTINCT ',' + iif(right(string,1) = '.',LEFT(String, len(String) -1),String) from mail2a WHERE String like '%_@_%_.__%' and rn > 1 and userid = a.USERID FOR XML PATH ('')), 1, 1, '')  AS URLList FROM mail2a AS a GROUP BY a.USERID)
--select * from mail3 where right(string,1) = '.'
--SELECT userID,String,ROW_NUMBER() OVER(PARTITION BY String ORDER BY userID DESC) AS rn FROM mail3 in (617025,615922,623956,625575,623014,599240,616626,623443,556358,564244,696401,711436,711367,623852,598451,632180,410107,495050,806078,760088,743297)

--, mail4 (userID,String,rn) as (SELECT userID,String,ROW_NUMBER() OVER(PARTITION BY String ORDER BY userID DESC) AS rn FROM mail3)
--select * from mail4 where String is not null and String like '%nextlink%' order by String
--select * from mail4 where userid in (396970,397884,397965,398036,406635,409577,410217,414232,415647,420063,436059,528026,550535,558455,567093,588173,588743,591415,592958,596988,599869,626195,629811,631637,659586,661339,676728,718815,728977,753998,760676,801076,814991)

, tel(REFERENCE, TEL_NUMBER) as (SELECT REFERENCE, STUFF((SELECT DISTINCT ', ' + TEL_NUMBER from  PROP_TELEPHONE WHERE TEL_NUMBER != '' and TEL_NUMBER is not null and REFERENCE = a.REFERENCE FOR XML PATH ('')), 1, 1, '')  AS URLList FROM PROP_TELEPHONE as a GROUP BY a.REFERENCE)
--select * from tel

/*, doc_note (OWNER_ID, DOC_ID, NOTE) as (SELECT D.OWNER_ID, D.DOC_ID, DC.NOTE from DOCUMENTS D left join DOCUMENT_CONTENT DC on DC.DOC_ID = d.DOC_ID WHERE DOC_CATEGORY = 6532841 AND FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip'))
--select top 100 * from doc_note where OWNER_ID = 394903
, doc(OWNER_ID, NOTE) as (SELECT OWNER_ID, STUFF((SELECT char(10) + 'NOTE: ' + NOTE from doc_note WHERE NOTE != '' and NOTE is not null and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 0, '')  AS doc FROM doc_note as a GROUP BY a.OWNER_ID)
--select top 50 * from doc where OWNER_ID = 394903
*/
, dob (BISUNIQUEID,REFERENCE,DT_OF_BIRTH,rn) as (SELECT BISUNIQUEID,REFERENCE,DT_OF_BIRTH,ROW_NUMBER() OVER(PARTITION BY cg.REFERENCE ORDER BY cg.BISUNIQUEID DESC) AS rn FROM PROP_CAND_GEN cg)

, owner as ( select PERSON_GEN.REFERENCE as PROP_PERSON_GEN_REFERENCE,PERSON_GEN.FIRST_NAME,PERSON_GEN.LAST_NAME,EMPLOYEE.REFERENCE as PROP_EMPLOYEE_GEN_REFERENCE,EMPLOYEE.NAME,mail.EMAIL_LINK,EMPLOYEE.USER_REF from PROP_PERSON_GEN PERSON_GEN 
	INNER JOIN PROP_OWN_CONS CONS ON PERSON_GEN.REFERENCE = CONS.REFERENCE INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT 
	left join (SELECT REFERENCE, EMAIL_LINK = STUFF((SELECT DISTINCT ', ' + EMAIL_LINK FROM PROP_EMAIL b WHERE b.EMAIL_LINK like '%@%' and b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM PROP_EMAIL a GROUP BY REFERENCE) mail ON EMPLOYEE.REFERENCE = mail.REFERENCE
	where CONFIG_NAME = 'Permanent'
	--and PERSON_GEN.FIRST_NAME like '%Christian%' and PERSON_GEN.LAST_NAME like '%Kwan%'
	)
--select * from owner


--select top 3 
select distinct 
	ccc.CONTACT as 'contact-externalId'
	, ccc.CLIENT as 'contact-companyId'
	, replace(pg.FIRST_NAME,'?','') as 'contact-firstName'
	, case when ( replace(pg.LAST_NAME,'?','') = '' or pg.LAST_NAME is null) then 'No Lastname' else replace(pg.LAST_NAME,'?','') end as 'contact-lastName'
	, pg.MIDDLE_NAME as 'contact-middleName'
	--, ltrim(replace(replace(replace(replace(replace(replace(pe.EMAIL_LINK,'�',''),'?',''),'  ',''),char(9),''),'/',','),'mailto:','')) as 'contact-email'
	--, case when (pe.rn > 1 and pe.String is not null) then concat('DUPLICATED_',pe.rn,'_',pe.String) else pe.String end as 'contact-email'
	, case when (pe.String != '' and pe.String is not null and pedup.rn = 2) then concat('DUPLICATED_',pedup.rn,'_',pedup.String) else iif(right(pe.string,1) = '.',LEFT(pe.String, len(pe.String) -1),pe.String) end as 'contact-email'
	, tel.TEL_NUMBER as 'contact-phone'
	, pg.JOB_TITLE as 'contact-jobTitle'
	, pg.LINKED_IN as 'contact-linkedin'
	, owner.EMAIL_LINK as 'contact-owners'
	, concat (
		 case when (oe.String != '' and oe.String is not null) then concat('Other email: ',iif(right(oe.string,1) = '.',LEFT(oe.String, len(oe.String) -1),oe.String),char(10)) else '' end
		, iif(pg.chifullname = '' or pg.chifullname is null,'', concat('Full Name: ',pg.chifullname,char(10)))
		, iif(pg.chinesename = '' or pg.chinesename is null,'', concat('Chinese Name: ',pg.chinesename,char(10)))
		, iif(telmobile.TEL_NUMBER = '' or telmobile.TEL_NUMBER is null,'', concat('Mobile Phone: ',telmobile.TEL_NUMBER,char(10)))
		, iif(telhome.TEL_NUMBER = '' or telhome.TEL_NUMBER is null,'', concat('Home Phone: ',telhome.TEL_NUMBER,char(10)))
		, iif(telwork.TEL_NUMBER = '' or telwork.TEL_NUMBER is null,'', concat('Work Phone: ',telwork.TEL_NUMBER,char(10)))
		, iif(address.STREET1 = '' or address.STREET1 is null,'', concat('Address: ',address.STREET1,char(10)))
		, iif(address.TOWN = '' or address.TOWN is null,'', concat('City: ',address.TOWN,char(10)))
		, iif(address.state = '' or address.state is null,'', concat('State: ',address.state,char(10)))
		, iif(address.COUNTRY is null,'', concat('Country: ',address.COUNTRY,char(10)))
		, iif(address.POST_CODE = '' or address.POST_CODE is null,'', concat('Zip Code: ',address.POST_CODE,char(10)))
		, iif(owner.NAME = '' or owner.NAME is null,'',concat('Contact Owner: ',owner.NAME,char(10)))
		) as 'contact-Note'
		
		/*, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
			left(doc.NOTE,32765)
			,'&#x00;',''),'&#x01;',''),'&#x02;',''),'&#x03;',''),'&#x04;',''),'&#x05;',''),'&#x06;',''),'&#x07;',''),'&#x08;',''),'&#x09;','')
			,'&#x0A;',''),'&#x0B;',''),'&#x0C;',''),'&#x0D;',''),'&#x0E;',''),'&#x0F;','')
			,'&#x10;',''),'&#x11;',''),'&#x12;',''),'&#x13;',''),'&#x14;',''),'&#x15;',''),'&#x16;',''),'&#x17;',''),'&#x18;',''),'&#x19;','')
			,'&#x1a;',''),'&#x1b;',''),'&#x1c;',''),'&#x1D;',''),'&#x1E;',''),'&#x1f;',''),'&#x7f;',''),'?C','C')
			,'amp;',''),'?(','(')
		) as 'contact-Note' */
	--, replace(doc.DOC_ID,'.txt','.rtf') as 'contact-document'
	--, left(replace(d.Notes,'&#x0D;',''),32000) as 'contact-comments'
--select count(*) --13846 rows
--select * 
from PROP_X_CLIENT_CON cc
left join contact ccc on cc.CONTACT = ccc.CONTACT
left join PROP_PERSON_GEN pg on ccc.CONTACT = pg.REFERENCE
--left join (SELECT REFERENCE, EMAIL_LINK = STUFF((SELECT DISTINCT ', ' + EMAIL_LINK FROM PROP_EMAIL b WHERE b.EMAIL_LINK like '%@%' and b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM PROP_EMAIL a GROUP BY REFERENCE) mail ON ccc.CONTACT = mail.REFERENCE
--left join (select * from email where rn = 1) pe ON ccc.CONTACT = pe.REFERENCE -- PRIMARY-EMAIL
left join (select * from mail2a where rn = 1) pe ON ccc.CONTACT = pe.userid -- PRIMARY-EMAIL
left join (select * from mail2b where rn = 2) pedup ON ccc.CONTACT = pedup.userid -- DUPLICATED-EMAIL
left join mail3 oe ON ccc.CONTACT = oe.userid -- Other-EMAIL
left join tel ON ccc.CONTACT = tel.REFERENCE
left join owner ON ccc.CONTACT = owner.PROP_PERSON_GEN_REFERENCE
--left join doc on ccc.CONTACT = doc.OWNER_ID
left join (select REFERENCE,CONFIG_NAME,STREET1,TOWN,STATE,COUNTRY,POST_CODE from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID where CONFIG_NAME = 'Primary') address on ccc.CONTACT = address.REFERENCE
left join (select REFERENCE,TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Home') telhome on ccc.CONTACT = telhome.REFERENCE --candidate-homePhone
left join (select REFERENCE,TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Mobile') telmobile on ccc.CONTACT = telmobile.REFERENCE --candidate-mobile
left join (select REFERENCE,TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Work') telwork on ccc.CONTACT = telwork.REFERENCE --candidate-phone & candidate-workPhone
--left join photo on pg.REFERENCE = photo.OWNER_ID
where ccc.rn = 1
--and pg.chinesename is not null
--and ccc.CONTACT in (396970,397884,397965,398036,406635,409577,410217,414232,415647,420063,436059,528026,550535,558455,567093,588173,588743,591415,592958,596988,599869,626195,629811,631637,659586,661339,676728,718815,728977,753998,760676,801076,814991)
--and pedup.rn = 2
--and ccc.CONTACT in (394855,394857,394858,394859,394860,394941,422556,440045,520613,538555,556969,596437,603166,672412,682563,715196,724260,767526,775203,808647,816694,838398)
--and pg.FIRST_NAME like '%Christopher%' and pg.LAST_NAME like '%Tang%'
and pg.FIRST_NAME like '%Susanna%' and pg.LAST_NAME like '%Poon%'
--and mail.EMAIL_LINK like '%chris.tang@staranise.com.hk%'
--and pe.string like '%)%'
--and own.EMPLOYEE_NAME is not null
--and ccc.CLIENT = 395108
--and mail.EMAIL_LINK like '%david.fleming@venetian.com.mo%'

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

with tmp as (SELECT REFERENCE, EMAIL_LINK = STUFF((SELECT DISTINCT ', ' + EMAIL_LINK FROM PROP_EMAIL b WHERE b.EMAIL_LINK like '%@%' and b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM PROP_EMAIL a GROUP BY REFERENCE)
select REFERENCE,email_link from tmp where email_link like '%christian.kwan@staranise.com.hk, kwan.christian@gmail.com%' --518610


-- PERSON_GEN.REFERENCE = <<PROP_PERSON_GEN.REFERENCE>>
*/

/*
with
contact (BISUNIQUEID,CLIENT,CONTACT,rn) as (SELECT BISUNIQUEID,CLIENT,CONTACT,ROW_NUMBER() OVER(PARTITION BY cg.CONTACT ORDER BY cg.BISUNIQUEID DESC) AS rn FROM PROP_X_CLIENT_CON cg)
,email as (SELECT REFERENCE,EMAIL_LINK,ROW_NUMBER() OVER(PARTITION BY ltrim(pe.REFERENCE) ORDER BY pe.REFERENCE DESC) AS rn FROM PROP_EMAIL pe WHERE EMAIL_LINK like '%@%' and EMAIL_LINK != '' and EMAIL_LINK IS NOT NULL )

select ccc.CONTACT
	 ,ltrim(replace(replace(replace(replace(replace(replace(pe.EMAIL_LINK,'�',''),'?',''),'  ',''),char(9),''),'/',','),'mailto:','')) as 'PRIMARY-EMAIL'
	 ,ltrim(replace(replace(replace(replace(replace(replace(pe2.EMAIL_LINK,'�',''),'?',''),'  ',''),char(9),''),'/',','),'mailto:','')) as 'SECONDARY-EMAIL'
from PROP_X_CLIENT_CON cc
left join contact ccc on cc.CONTACT = ccc.CONTACT
left join PROP_PERSON_GEN pg on ccc.CONTACT = pg.REFERENCE
left join (select * from email where rn = 1) pe ON ccc.CONTACT = pe.REFERENCE -- PRIMARY-EMAIL
left join (select * from email where rn > 1) pe2 ON ccc.CONTACT = pe2.REFERENCE -- SECONDARY-EMAIL
*/