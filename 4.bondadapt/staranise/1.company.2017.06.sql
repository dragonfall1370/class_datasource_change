
with
 tel(REFERENCE, TEL_NUMBER) as (SELECT REFERENCE, STUFF((SELECT DISTINCT ', ' + TEL_NUMBER from PROP_TELEPHONE WHERE TEL_NUMBER != '' and REFERENCE = a.REFERENCE FOR XML PATH ('')), 1, 2, '')  AS URLList FROM PROP_TELEPHONE as a GROUP BY a.REFERENCE)
--select * from tel

--, doc(OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY in (31185,7022996) AND FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip') and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
, doc(OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY in (7022996,31190) and FILE_EXTENSION != 'txt' and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
--select * from doc where doc.DOC_ID is not null
--select count(*) from doc where doc.DOC_ID is not null

--COMPANY - PSA Documents:	SELECT count(*) FROM DOCUMENTS where DOC_CATEGORY = 7022996
--Client Description:			SELECT count(*) FROM DOCUMENTS WHERE DOC_CATEGORY = 7023000 AND OWNER_ID = <<PROP_CLIENT_GEN.REFERENCE>> -- 4373 rows
--Client Overview:			SELECT count(*) FROM DOCUMENTS WHERE DOC_CATEGORY = 6532843 AND OWNER_ID = <<PROP_CLIENT_GEN.REFERENCE>> -- 4845 rows
--Client Visit Notes:			SELECT count(*) FROM DOCUMENTS WHERE DOC_CATEGORY = 7023004 AND OWNER_ID = <<PROP_CLIENT_GEN.REFERENCE>> -- 4406 rows
--Client Email:				SELECT count(*) FROM DOCUMENTS WHERE DOC_CATEGORY = 31190 AND OWNER_ID = <<PROP_CLIENT_GEN.REFERENCE>> -- 52329 rows

, owner as (
	select CLIENT_GEN.REFERENCE as CLIENT_GEN_REFERENCE, CLIENT_GEN.NAME as CLIENT_GEN_NAME, EMPLOYEE.REFERENCE as EMPLOYEE_REFERENCE, EMPLOYEE.NAME as EMPLOYEE_NAME, CONS.REFERENCE as CONS_REFERENCE, mail.EMAIL_LINK
	from PROP_CLIENT_GEN CLIENT_GEN INNER JOIN PROP_OWN_CONS CONS ON CLIENT_GEN.REFERENCE = CONS.REFERENCE INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT
	left join (SELECT REFERENCE, EMAIL_LINK = STUFF((SELECT DISTINCT ', ' + EMAIL_LINK FROM PROP_EMAIL b WHERE b.EMAIL_LINK like '%@%' and b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM PROP_EMAIL a GROUP BY REFERENCE) mail ON EMPLOYEE.REFERENCE = mail.REFERENCE
	where CONFIG_NAME = 'Permanent' 
--and CONS.REFERENCE = 10629 AND PERSON_GEN.REFERENCE = <<PROP_CLIENT_GEN.REFERENCE>>
)
--select * from owner

select top 200
--select
	cg.REFERENCE as 'company-externalId'
	, ho.NAME as 'company-headoffice'
	, case 
		when cg.NAME in (select ltrim(cg.NAME) from PROP_CLIENT_GEN cg group by ltrim(cg.name) having count(*) > 1) then concat (dup.name,' ',dup.rn)
		when (cg.NAME = '' or cg.name is null) then 'No Company Name'
		else ltrim(replace(cg.NAME,'?','')) 
		end as 'company-name'
	, sb.TEL_NUMBER as 'company-switchboard'
--, cg.STATUS as '(STATUS)'
	, address.STREET1 as 'company-locationAddress'
	, address.TOWN as 'company-locationCity'
	, address.state as 'company-locationState'
	, address.POST_CODE as 'company-locationZipCode'
	--, cnt.DESCRIPTION as '(company-locationCountry)'
	, CASE WHEN (cnt.DESCRIPTION = '' OR cnt.DESCRIPTION = 'NULL') THEN '' ELSE tc.ABBREVIATION END as 'company-locationCountry'
--, cg.LOCATION as '(company-locationName)'
	, ltrim(concat(address.TOWN
		,case when (address.STATE = '' OR address.STATE is NULL) THEN '' ELSE concat(' ',address.STATE) END
		,case when (cnt.DESCRIPTION = '' OR cnt.DESCRIPTION is NULL) THEN '' ELSE concat(' ',cnt.DESCRIPTION) END
		)) as 'company-locationName'
	, tel.TEL_NUMBER as 'company-phone'
	, fax.TEL_NUMBER as 'company-fax'
	, left(cg.WEB_ADD,100) as 'company-website'
	, replace(doc.DOC_ID,'.txt','.rtf') as 'company-document'
	, owner.EMAIL_LINK as 'company-owners'
	, concat(
		iif(owner.EMPLOYEE_NAME = '' OR owner.EMPLOYEE_NAME = 'NULL','',concat('Company Owner: ',owner.EMPLOYEE_NAME,char(10)))
		, iif(address.STREET2 = '' or address.STREET2 is NULL,'',concat('Address Line 2: ',address.STREET2,char(10)))
		, iif(clientsource.source = '' or clientsource.source is NULL,'',concat('Client Source: ',clientsource.source,char(10)))
		, iif(currency.currency = '' or currency.currency is NULL,'',concat('Currency: ',currency.currency,char(10)))
		, iif(sal.SAL_AGREED = '' or sal.SAL_AGREED is NULL,'',concat('SAL T&C: ',sal.SAL_AGREED,char(10)))
		, iif(guarantee.GUARANTEE = '' or guarantee.GUARANTEE is NULL,'',concat('Guarantee Period: ',guarantee.GUARANTEE,char(10)))
		, iif(psa.PSA = '' or psa.PSA is NULL,'',concat('PSA Rate: ',psa.PSA))
		) as 'company-note'
	--, mail.EMAIL_LINK as '(company-owners-email)'
-- select count(*)
from PROP_CLIENT_GEN cg --4093
--left join ENTITY_TABLE e ON cg.REFERENCE = cg.REFERENCE
left join tel ON CG.REFERENCE = tel.REFERENCE
left join (select REFERENCE,CONFIG_NAME,STREET1,STREET2,TOWN,STATE,COUNTRY,POST_CODE from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID where CONFIG_NAME = 'Primary') address ON cg.REFERENCE = address.REFERENCE
left join (select REFERENCE,CONFIG_NAME,TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Fax') fax ON cg.REFERENCE = fax.REFERENCE
left join (select REFERENCE, DESCRIPTION from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID INNER JOIN MD_MULTI_NAMES MN ON MN.ID = ADDRESS.COUNTRY where CONFIG_NAME = 'Primary') cnt ON cg.REFERENCE = cnt.REFERENCE
left join (select upper(Country) as COUNTRY, ABBREVIATION from tmp_currency) tc ON cnt.DESCRIPTION = tc.COUNTRY
left join (SELECT CLIENT_ID,name,ROW_NUMBER() OVER(PARTITION BY ltrim(cg.name) ORDER BY cg.CLIENT_ID DESC) AS rn FROM PROP_CLIENT_GEN cg) dup on cg.CLIENT_ID = dup.CLIENT_ID
left join (SELECT REFERENCE, EMAIL_LINK = STUFF((SELECT DISTINCT ', ' + EMAIL_LINK FROM PROP_EMAIL b WHERE b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM PROP_EMAIL a GROUP BY REFERENCE) mail ON cg.REFERENCE = mail.REFERENCE

--left join PROP_X_CLIENT_CON client ON cg.REFERENCE = client.CONTACT
left join owner ON cg.REFERENCE = owner.CLIENT_GEN_REFERENCE --PERSON_GEN.REFERENCE = <<PROP_CLIENT_GEN.REFERENCE>>

left join doc on cg.REFERENCE = doc.OWNER_ID
left join (SELECT X_CLIENT.CLIENT,LE.NAME FROM PROP_X_LE_CLIENTS X_CLIENT INNER JOIN PROP_LE_GEN LE ON LE.REFERENCE = X_CLIENT.LE) ho on CG.REFERENCE = ho.CLIENT
left join (SELECT REFERENCE,TEL_NUMBER FROM PROP_TELEPHONE WHERE OCC_ID =2034418) sb on CG.REFERENCE = sb.REFERENCE
left join (SELECT CG.REFERENCE,MN.DESCRIPTION as source FROM PROP_CLIENT_GEN CG INNER JOIN MD_MULTI_NAMES MN ON MN.ID = CG.SOURCE) clientsource on cg.REFERENCE = clientsource.REFERENCE
left join (SELECT CG.REFERENCE,MN.DESCRIPTION as currency FROM PROP_CLIENT_GEN CG INNER JOIN MD_MULTI_NAMES MN ON MN.ID = CG.CURRENCY) currency on cg.REFERENCE = currency.REFERENCE
left join (SELECT REFERENCE,SAL_AGREED FROM PROP_CLIENT_TC) sal on cg.REFERENCE = sal.REFERENCE
left join (SELECT CLIENT_TC.REFERENCE,MN.DESCRIPTION as GUARANTEE FROM PROP_CLIENT_TC CLIENT_TC INNER JOIN MD_MULTI_NAMES MN ON MN.ID = CLIENT_TC.GUARANTEE) guarantee on cg.REFERENCE = guarantee.REFERENCE
left join (SELECT REFERENCE,PSA FROM PROP_CLIENT_TC) psa on cg.REFERENCE = psa.REFERENCE

--where ho.NAME is not null
--doc.DOC_ID is not null
--where cg.name like '%samsung%'
--where cg.reference in (395179,395559,406888,407392,423835,430384,517925,523592,534160,571388,572621,595701,626088,664881) --long website
--where own.NAME is not null
order by cg.REFERENCE

--select * from PROP_CLIENT_GEN CLIENT_GEN INNER JOIN PROP_OWN_CONS CONS ON CLIENT_GEN.REFERENCE = CONS.REFERENCE INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT where CONFIG_NAME = 'Permanent'

/*
--select top 100 * from 	PROP_CLIENT_GEN
--select top 100 * from 	PROP_ADDRESS

select cc.name from bullhorn1.BH_ClientCorporation CC
where CC.name not like '%Imported Contacts%' and CC.status not like '%Archive%'
--where cc.name like '%Deloitte%' or cc.name like '%Manhattan Chamber of Commerce%'
group by cc.name having count(*) > 1
order by name
*/


