-- ALTER DATABASE [initi8_280619] SET COMPATIBILITY_LEVEL = 130

with
  tel(REFERENCE, TEL_NUMBER) as (SELECT REFERENCE, STUFF((SELECT DISTINCT ',' + replace(TEL_NUMBER,',',' ') from PROP_TELEPHONE WHERE TEL_NUMBER != '' and REFERENCE = a.REFERENCE FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS URLList FROM PROP_TELEPHONE as a GROUP BY a.REFERENCE)
--select * from tel

--, doc(OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY in (31185,7022996) AND FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip') and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
--, doc(OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY in (7022996,31190) and FILE_EXTENSION != 'txt' and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
--, doc(OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ',' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE OWNER_ID = a.OWNER_ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
, doc(OWNER_ID, DOC_ID) as ( SELECT OWNER_ID, STRING_AGG( concat(cast(DOC_ID as varchar(max)),'.',FILE_EXTENSION) ,',') WITHIN GROUP (ORDER BY DOC_ID) DOC FROM DOCUMENTS where DOC_CATEGORY <> 6532843 GROUP BY OWNER_ID)
--select top 100 * from doc1 where doc.DOC_ID is null
--select count(*) from doc where doc.DOC_ID is not null
--select cg.REFERENCE, cg.name, replace(doc.DOC_ID,'.txt','.rtf') as 'company-document' from PROP_CLIENT_GEN cg left join doc on cg.REFERENCE = doc.OWNER_ID where doc.OWNER_ID is not null

--COMPANY - PSA Documents:	SELECT count(*) FROM DOCUMENTS where DOC_CATEGORY = 7022996
--Client Description:			SELECT count(*) FROM DOCUMENTS WHERE DOC_CATEGORY = 7023000 AND OWNER_ID = <<PROP_CLIENT_GEN.REFERENCE>> -- 4373 rows
--Client Overview:			SELECT count(*) FROM DOCUMENTS WHERE DOC_CATEGORY = 6532843 AND OWNER_ID = <<PROP_CLIENT_GEN.REFERENCE>> -- 4845 rows
--Client Visit Notes:			SELECT count(*) FROM DOCUMENTS WHERE DOC_CATEGORY = 7023004 AND OWNER_ID = <<PROP_CLIENT_GEN.REFERENCE>> -- 4406 rows
--Client Email:				SELECT count(*) FROM DOCUMENTS WHERE DOC_CATEGORY = 31190 AND OWNER_ID = <<PROP_CLIENT_GEN.REFERENCE>> -- 52329 rows

, owner as (
	select CLIENT_GEN.REFERENCE as CLIENT_GEN_REFERENCE, CLIENT_GEN.NAME as CLIENT_GEN_NAME, EMPLOYEE.REFERENCE as EMPLOYEE_REFERENCE, EMPLOYEE.NAME as EMPLOYEE_NAME, CONS.REFERENCE as CONS_REFERENCE, tmp_email.EMAIL as EMAIL_ADD --, mail.EMAIL_ADD
	-- select distinct EMPLOYEE.REFERENCE, EMPLOYEE.NAME
	from PROP_CLIENT_GEN CLIENT_GEN 
	INNER JOIN PROP_OWN_CONS CONS ON CLIENT_GEN.REFERENCE = CONS.REFERENCE 
	INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID 
	INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT
	--left join (SELECT REFERENCE, EMAIL_ADD = STUFF((SELECT DISTINCT ', ' + EMAIL_ADD FROM PROP_EMAIL b WHERE b.EMAIL_ADD like '%@%' and b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM PROP_EMAIL a GROUP BY REFERENCE) mail ON EMPLOYEE.REFERENCE = mail.REFERENCE
	left join tmp_email ON EMPLOYEE.REFERENCE = tmp_email.REFERENCE
	where CONFIG_NAME = 'Permanent' 
       --and CONS.REFERENCE = 10629 AND PERSON_GEN.REFERENCE = <<PROP_CLIENT_GEN.REFERENCE>>
)
--select * from owner


, n0 (owner_id, content, CREATED_DATE) as (SELECT owner_id, ltrim(replace(replace( [dbo].[udf_StripHTML](convert(varchar(max),convert(varbinary(max),DOCUMENT))) ,'Â',''),'ï»¿','')) as 'content', CREATED_DATE FROM DOCUMENTS where DOC_CATEGORY = 6532843)
--select owner_id from n0 group by owner_id having count(*) > 1
, n1 (owner_id, content) as (
       SELECT owner_id
              , STRING_AGG( coalesce( concat('CREATED DATE: ',CREATED_DATE,char(10)) + nullif(content, '') + char(10), '') ,char(10) ) WITHIN GROUP (ORDER BY CREATED_DATE desc) content       
       FROM n0 where content <> '' GROUP BY owner_id
       )
--select top 10 * from n1 where OWNER_ID in (44563,101104,438706,440299,476705,484467,497083,508127,668791)

, eg5_contact (reference, name, rn) as (
       select distinct oc.reference, eg5.name 
               , ROW_NUMBER() OVER(PARTITION BY oc.reference ORDER BY oc.BISUNIQUEID asc) AS rn
       from PROP_OWN_CONS oc left join PROP_EMPLOYEE_GEN eg5 on eg5.user_ref = oc.consultant 
       where eg5.user_ref is not null
       --and oc.reference = 90522
       )


select --top 10
	cg.REFERENCE as 'company-externalId', cg.client_id
	--, ho.NAME as 'company-headoffice'
	/*, case 
		when cg.NAME in (select ltrim(cg.NAME) from PROP_CLIENT_GEN cg group by ltrim(cg.name) having count(*) > 1) then concat (dup.name,' ',dup.rn)
		when (cg.NAME = '' or cg.name is null) then 'No Company Name'
		else ltrim(replace(cg.NAME,'?','')) 
		end as 'company-name'*/
       , iif(dup.rn > 1,concat(dup.name,' ',dup.rn), iif(dup.name in (null,''),'No CompanyName',dup.name)) as 'company-name'
	--, sb.TEL_NUMBER as 'company-switchboard'
	, tel.TEL_NUMBER as 'company-phone'
	--, fax.TEL_NUMBER as 'company-fax'
	, left(cg.WEB_ADD,100) as 'company-website'	
       --, cg.STATUS as '(STATUS)'
	, ltrim(Stuff( Coalesce(' ' + NULLIF(address.STREET1, ''), '') + Coalesce(', ' + NULLIF(address.STREET2, ''), '') , 1, 1, '') ) as 'company-locationAddress'
	, address.TOWN as 'company-locationCity'
	, address.COUNTY as 'company-locationState'
	, address.POST_CODE as 'company-locationZipCode'
	--, cnt.DESCRIPTION as '(company-locationCountry)'
        , case
		when cnt.DESCRIPTION like 'AFGHANI%' then 'AF'
		when cnt.DESCRIPTION like 'AUSTRAL%' then 'AU'
		when cnt.DESCRIPTION like 'AUSTRIA%' then 'AT'
		when cnt.DESCRIPTION like 'BELGIUM%' then 'BE'
		when cnt.DESCRIPTION like 'DENMARK%' then 'DK'
		when cnt.DESCRIPTION like 'ESTONIA%' then 'EE'
		when cnt.DESCRIPTION like 'FRANCE%' then 'FR'
		when cnt.DESCRIPTION like 'GERMANY%' then 'DE'
		when cnt.DESCRIPTION like 'INDIA%' then 'IN'
		when cnt.DESCRIPTION like 'IRELAND%' then 'IE'
		when cnt.DESCRIPTION like 'ITALY%' then 'IT'
		when cnt.DESCRIPTION like 'LEBANON%' then 'LB'
		when cnt.DESCRIPTION like 'LIECHTE%' then 'LI'
		when cnt.DESCRIPTION like 'LUXEMBO%' then 'LU'
		when cnt.DESCRIPTION like 'NETHERL%' then 'NL'
		when cnt.DESCRIPTION like 'NEW ZEALAND' then 'NZ'
		when cnt.DESCRIPTION like 'NORWAY%' then 'NO'
		when cnt.DESCRIPTION like 'POLAND%' then 'PL'
		when cnt.DESCRIPTION like 'PORTUGA%' then 'PT'
		when cnt.DESCRIPTION like 'SPAIN%' then 'ES'
		when cnt.DESCRIPTION like 'SWEDEN%' then 'SE'
		when cnt.DESCRIPTION like 'SWITZER%' then 'CH'
		when cnt.DESCRIPTION like '%UNITED%ARAB%' then 'AE'
		when cnt.DESCRIPTION like '%UAE%' then 'AE'
		when cnt.DESCRIPTION like '%U.A.E%' then 'AE'
		when cnt.DESCRIPTION like '%UNITED%KINGDOM%' then 'GB'
		when cnt.DESCRIPTION like '%UNITED%STATES%' then 'US'
		when cnt.DESCRIPTION like '%US%' then 'US'
		else '' end as 'company-locationCountry'
       --, cg.LOCATION as '(company-locationName)'
	, ltrim(Stuff(     Coalesce(' ' + NULLIF(address.TOWN, ''), '')
                        + Coalesce(', ' + NULLIF(address.COUNTY, ''), '')
                        + Coalesce(', ' + NULLIF(cnt.DESCRIPTION, ''), '')
                , 1, 1, '')) as 'company-locationName'

	, replace(doc.DOC_ID,'.txt','.rtf') as 'company-document'
	, et.createddate as "insert_timestamp"
	, owner.EMAIL_ADD as 'company-owners'
       , Stuff(  
                Coalesce('ID: ' + NULLIF(convert(nvarchar(max),cg.client_id), '') + char(10), '')
              --Coalesce('Company Owner: ' + NULLIF(owner.EMPLOYEE_NAME, '') + char(10), '')
              --+ Coalesce('Address Line 2: ' + NULLIF(address.STREET2, '') + char(10), '')
              --+ Coalesce('Client Source: ' + NULLIF(cast(clientsource.source as nvarchar(max)), '') + char(10), '')
              --+ Coalesce('Currency: ' + NULLIF(cast(currency.currency as nvarchar(max)), '') + char(10), '')
              --+ Coalesce('SAL T&C: ' + NULLIF(cast(sal.SAL_AGREED as nvarchar(max)), '') + char(10), '')
              --+ Coalesce('Guarantee Period: ' + NULLIF(cast(guarantee.GUARANTEE as nvarchar(max)), '') + char(10), '')
              --+ Coalesce('PSA Rate: ' + NULLIF(cast(psa.PSA as nvarchar(max)), '') + char(10), '')
              
              + Coalesce('Status: ' + NULLIF(convert(nvarchar(max),status.DESCRIPTION), '') + char(10), '')
              + Coalesce('Grading: ' + NULLIF(convert(nvarchar(max),grading.DESCRIPTION), '') + char(10), '')
              --+ Coalesce('GENERAL NOTES: ' + NULLIF(convert(nvarchar(max),cg.), '') + char(10), '')
              --+ Coalesce('Last 5 Journal Entries > Date: ' + NULLIF(convert(nvarchar(max),cg.), '') + char(10), '')
              --+ Coalesce('Last 5 Journal Entries > Workflow Task: ' + NULLIF(convert(nvarchar(max),cg.), '') + char(10), '')
              --+ Coalesce('Last 5 Journal Entries > Notes: ' + NULLIF(convert(nvarchar(max),cg.), '') + char(10), '')
              --+ Coalesce('Last 5 Journal Entries > Recruiter: ' + NULLIF(convert(nvarchar(max),cg.), '') + char(10), '')
              + Coalesce('Client Type: ' + NULLIF(convert(nvarchar(max),client_type.DESCRIPTION), '') + char(10), '')
              --+ Coalesce('Creation Date: ' + NULLIF(convert(nvarchar(max),et.createddate), '') + char(10), '')
              + Coalesce('Created By: ' + NULLIF(convert(nvarchar(max),et.created_by_name), '') + char(10), '')
              + Coalesce('Source: ' + NULLIF( case when cg.SOURCE = 0 then '' else convert(varchar(200),cg.SOURCE) end, '') + char(10), '')
              + Coalesce('Last Contacted: ' + NULLIF(convert(nvarchar(max),convert(date,ch.last_cont_dt)), '') + char(10), '')
              + Coalesce('By: ' + NULLIF(convert(nvarchar(max),eg.name), '') + char(10), '')
              + Coalesce('Last Visited: ' + NULLIF(convert(nvarchar(max),convert(date,ch.last_vis_dt)), '') + char(10), '')
              + Coalesce('By: ' + NULLIF(convert(nvarchar(max),eg3.name), '') + char(10), '')
              + Coalesce('Last Updated: ' + NULLIF(convert(nvarchar(max),convert(date,et.updateddate)), '') + char(10), '')
              + Coalesce('By: ' + NULLIF(convert(nvarchar(max),et.updated_by_name), '') + char(10), '')
              + Coalesce('Last Perm Job: ' + NULLIF(convert(nvarchar(max),convert(date,ch.last_p_dt)), '') + char(10), '')
              + Coalesce('Managed By: ' + NULLIF(convert(nvarchar(max),eg2.name), '') + char(10), '')
              + Coalesce('Last Contract Job: ' + NULLIF(convert(nvarchar(max),convert(date,ch.last_c_dt)), '') + char(10), '')
              + Coalesce('Managed By: ' + NULLIF(convert(nvarchar(max),eg4.name), '') + char(10), '')
              + Coalesce('Contract: ' + NULLIF(convert(nvarchar(max),eg5.name), '') + char(10), '')
              --+ Coalesce('Perm: ' + NULLIF(convert(nvarchar(max),owner.EMPLOYEE_NAME), '') + char(10), '')
              --+ Coalesce('Team > Contract: ' + NULLIF(convert(nvarchar(max),cg.), '') + char(10), '')
              --+ Coalesce('Team > Perm: ' + NULLIF(convert(nvarchar(max),cg.), '') + char(10), '')
              --+ Coalesce( char(10) + 'General Notes: ' + char(10) + NULLIF(convert(nvarchar(max),n1.content), '') + char(10), '')
              + Coalesce( char(10) + 'General Notes: ' + char(10) + NULLIF( [dbo].[udf_StripHTML](n1.content), '') + char(10), '')
              --+ Coalesce('TOB Sent: ' + NULLIF(convert(nvarchar(max),cg.), '') + char(10), '')             
                , 1, 0, '') as 'company-note'       
        --, mail.EMAIL_ADD as '(company-owners-email)'
-- select count(*) -- select top 10 * -- select distinct cg.SOURCE --cg.grading
from PROP_CLIENT_GEN cg --where cg.REFERENCE is not null and REFERENCE in (49183) --4502
left join (SELECT REFERENCE, ltrim(rtrim(name)) as name, ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(name)) ORDER BY REFERENCE ASC) AS rn FROM PROP_CLIENT_GEN where REFERENCE is not null) dup on dup.REFERENCE = cg.REFERENCE
left join (select distinct REFERENCE, DESCRIPTION from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID INNER JOIN MD_MULTI_NAMES MN ON MN.ID = ADDRESS.COUNTRY where CONFIG_NAME = 'Primary' ) cnt ON cg.REFERENCE = cnt.REFERENCE
left join (select REFERENCE,CONFIG_NAME,STREET1,STREET2,TOWN,COUNTY,COUNTRY,POST_CODE from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID where CONFIG_NAME = 'Primary' ) address ON cg.REFERENCE = address.REFERENCE
--left join (select REFERENCE,CONFIG_NAME,TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Fax') fax ON cg.REFERENCE = fax.REFERENCE
--left join (select upper(Country) as COUNTRY, ABBREVIATION from tmp_currency) tc ON cnt.DESCRIPTION = tc.COUNTRY
--left join (SELECT CLIENT_ID,name,ROW_NUMBER() OVER(PARTITION BY ltrim(cg.name) ORDER BY cg.CLIENT_ID DESC) AS rn FROM PROP_CLIENT_GEN cg) dup on cg.CLIENT_ID = dup.CLIENT_ID
left join (SELECT REFERENCE, EMAIL_ADD = STUFF((SELECT DISTINCT ', ' + EMAIL_ADD FROM PROP_EMAIL b WHERE b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM PROP_EMAIL a GROUP BY REFERENCE) mail ON cg.REFERENCE = mail.REFERENCE
left join (select cg.REFERENCE, cg.grading, mn.DESCRIPTION from PROP_CLIENT_GEN cg INNER JOIN MD_MULTI_NAMES MN ON MN.ID = cg.grading where MN.ID is not null and LANGUAGE = 10010) grading on grading.REFERENCE = cg.REFERENCE
--left join PROP_X_CLIENT_CON client ON cg.REFERENCE = client.CONTACT
left join owner ON cg.REFERENCE = owner.CLIENT_GEN_REFERENCE --PERSON_GEN.REFERENCE = <<PROP_CLIENT_GEN.REFERENCE>>
left join tel ON CG.REFERENCE = tel.REFERENCE
left join doc on cg.REFERENCE = doc.OWNER_ID
--left join (SELECT X_CLIENT.CLIENT,LE.NAME FROM PROP_X_LE_CLIENTS X_CLIENT INNER JOIN PROP_LE_GEN LE ON LE.REFERENCE = X_CLIENT.LE) ho on CG.REFERENCE = ho.CLIENT
--left join (SELECT REFERENCE,TEL_NUMBER FROM PROP_TELEPHONE WHERE OCC_ID =2034418) sb on CG.REFERENCE = sb.REFERENCE
--left join (SELECT CG.REFERENCE,MN.DESCRIPTION as source FROM PROP_CLIENT_GEN CG INNER JOIN MD_MULTI_NAMES MN ON MN.ID = CG.SOURCE) clientsource on cg.REFERENCE = clientsource.REFERENCE
--left join (SELECT CG.REFERENCE,MN.DESCRIPTION as currency FROM PROP_CLIENT_GEN CG INNER JOIN MD_MULTI_NAMES MN ON MN.ID = CG.CURRENCY) currency on cg.REFERENCE = currency.REFERENCE
--left join ENTITY_TABLE e ON cg.REFERENCE = cg.REFERENCE
--left join (SELECT REFERENCE,SAL_AGREED FROM PROP_CLIENT_TC) sal on cg.REFERENCE = sal.REFERENCE
--left join (SELECT CLIENT_TC.REFERENCE,MN.DESCRIPTION as GUARANTEE FROM PROP_CLIENT_TC CLIENT_TC INNER JOIN MD_MULTI_NAMES MN ON MN.ID = CLIENT_TC.GUARANTEE) guarantee on cg.REFERENCE = guarantee.REFERENCE
--left join (SELECT REFERENCE,PSA FROM PROP_CLIENT_TC) psa on cg.REFERENCE = psa.REFERENCE
left join (select cg.client_id, cg.name, cg.REFERENCE, mn.DESCRIPTION from PROP_CLIENT_GEN cg INNER JOIN MD_MULTI_NAMES MN ON MN.ID = cg.status where MN.ID is not null and LANGUAGE = 10010) status on status.REFERENCE = cg.REFERENCE
left join (select cg.client_id, cg.name, cg.REFERENCE, mn.DESCRIPTION from PROP_CLIENT_GEN cg INNER JOIN MD_MULTI_NAMES MN ON MN.ID = cg.client_type where MN.ID is not null and LANGUAGE = 10010) client_type on client_type.REFERENCE = cg.REFERENCE
left join PROP_CLIENT_HIST ch on ch.reference = cg.reference
left join PROP_EMPLOYEE_GEN eg on eg.user_ref = ch.last_cont_by
left join PROP_EMPLOYEE_GEN eg2 on eg2.user_ref = ch.last_p_by
left join PROP_EMPLOYEE_GEN eg3 on eg3.user_ref = ch.last_vis_by
left join PROP_EMPLOYEE_GEN eg4 on eg4.user_ref = ch.last_c_by
--left join (select distinct oc.reference, eg5.name from PROP_OWN_CONS oc left join PROP_EMPLOYEE_GEN eg5 on eg5.user_ref = oc.consultant where eg5.user_ref is not null) eg5 on eg5.reference = cg.reference
left join (select * from eg5_contact where rn = 1) eg5 on eg5.reference = cg.reference
left join n1 on n1.owner_id = cg.reference
left join (
       select et.ENTITY_ID, et.createddate, et.created_by, eg1.name as created_by_name, et.UPDATEDDATE, et.UPDATED_BY, eg2.name as UPDATED_BY_NAME--, et.*
       from ENTITY_TABLE et 
       left join PROP_EMPLOYEE_GEN eg1 on eg1.user_ref = et.created_by
       left join PROP_EMPLOYEE_GEN eg2 on eg2.user_ref = et.UPDATED_BY
       --where et.ENTITY_ID = 51094
       ) et on et.ENTITY_ID = cg.reference
where cg.REFERENCE is not null --AND cg.REFERENCE in (90522)
--and n1.OWNER_ID in (44563,101104,438706,440299,476705,484467,497083,508127,668791)
--and CG.client_id = 1096903 -- 
--where ho.NAME is not null
--doc.DOC_ID is not null
--and dup.name like '%test client%'
--where cg.reference in (395092) --(395179,395559,406888,407392,423835,430384,517925,523592,534160,571388,572621,595701,626088,664881) --long website
--where own.NAME is not null
--order by cg.REFERENCE desc

--select * from PROP_CLIENT_GEN CLIENT_GEN INNER JOIN PROP_OWN_CONS CONS ON CLIENT_GEN.REFERENCE = CONS.REFERENCE INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT where CONFIG_NAME = 'Permanent'

/*
select top 10 * from PROP_CLIENT_GEN cg where cg.REFERENCE = 116681494331 or cg.name = 'Argos'
select REFERENCE from PROP_CLIENT_GEN cg group by REFERENCE having count(*) > 1

--select top 100 * from 	PROP_CLIENT_GEN
--select top 100 * from 	PROP_ADDRESS
select top 100 * FROM PROP_EMAIL WHERE EMAILADDRESS IS NOT NULL
select cc.name from bullhorn1.BH_ClientCorporation CC
where CC.name not like '%Imported Contacts%' and CC.status not like '%Archive%'
--where cc.name like '%Deloitte%' or cc.name like '%Manhattan Chamber of Commerce%'
group by cc.name having count(*) > 1
order by name
*/

