
with
  tel as (
  SELECT REFERENCE, STUFF((SELECT DISTINCT ',' + replace(TEL_NUMBER,',',' ') 
					from PROP_TELEPHONE 
					WHERE TEL_NUMBER != '' and REFERENCE = a.REFERENCE FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS TEL_NUMBER
  FROM PROP_TELEPHONE as a GROUP BY a.REFERENCE)
--select * from tel

, doc as (
	SELECT OWNER_ID, STUFF((SELECT DISTINCT ',' + cast(DOC_ID as varchar(max)) + '_' 
						+ replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(DOC_NAME,'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':',''),char(10),''),char(13),'')
						+ '.' + replace(coalesce(nullif(FILE_EXTENSION,''),PREVIEW_TYPE),'txt','rtf')
						from DOCUMENTS 
						WHERE OWNER_ID = a.OWNER_ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS docs
FROM DOCUMENTS as a
--WHERE DOC_ID = 101883385                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
GROUP BY a.OWNER_ID)

, tempOwners as (
	select CLIENT_GEN.REFERENCE as CLIENT_GEN_REFERENCE, CLIENT_GEN.NAME as CLIENT_GEN_NAME, EMPLOYEE.REFERENCE as EMPLOYEE_REFERENCE, EMPLOYEE.NAME as EMPLOYEE_NAME, CONS.REFERENCE as CONS_REFERENCE, mail.EMAIL_ADD
	from PROP_CLIENT_GEN CLIENT_GEN INNER JOIN PROP_OWN_CONS CONS ON CLIENT_GEN.REFERENCE = CONS.REFERENCE INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT
	left join (SELECT REFERENCE, EMAIL_ADD = STUFF((SELECT DISTINCT ', ' + EMAIL_ADD FROM PROP_EMAIL b WHERE b.EMAIL_ADD like '%@%' and b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM PROP_EMAIL a GROUP BY REFERENCE) mail ON EMPLOYEE.REFERENCE = mail.REFERENCE
	where CONFIG_NAME = 'Permanent' 
--and CONS.REFERENCE = 10629 AND PERSON_GEN.REFERENCE = <<PROP_CLIENT_GEN.REFERENCE>>
)

, owner as(
	select *, case 
	when employee_name like '%Alistair Illstone%' then 'alistairillstone@brosterbuchanan.com'
	when employee_name like '%Andrew Broster%' then 'andrewbroster@brosterbuchanan.com'
	when employee_name like '%Antony Clish%' then 'antonyclish@brosterbuchanan.com'
	when employee_name like '%Antony Marchant%' then 'antonymarchant@brosterbuchanan.com'
	when employee_name like '%Bruce Hopkin%' then 'brucehopkin@brosterbuchanan.com'
	when employee_name like '%Charles Ford%' then 'charlesford@brosterbuchanan.com'
	when employee_name like '%Chloe Hawkins%' then 'rachelpike@brosterbuchanan.com'
	when employee_name like '%Chris Batters%' then 'chrisbatters@brosterbuchanan.com'
	when employee_name like '%Christian Fell%' then 'christianfell@brosterbuchanan.com'
	when employee_name like '%Dominic Cassidy%' then 'dominiccassidy@brosterbuchanan.com'
	when employee_name like '%Gemma Ingram%' then 'gemmaingram@brosterbuchanan.com'
	when employee_name like '%Hilary Marshall%' then 'hilarymarshall@brosterbuchanan.com'
	when employee_name like '%Kevin Moran%' then 'kevinmoran@brosterbuchanan.com'
	when employee_name like '%Joel Shewell%' then 'kevinmoran@brosterbuchanan.com'
	when employee_name like '%Lenna Thompson%' then 'lennathompson@brosterbuchanan.com'
	when employee_name like '%Lucy Tavender%' then 'lucytavender@brosterbuchanan.com'
	when employee_name like '%Marie Brocklehurst%' then 'dominiccassidy@brosterbuchanan.com'
	when employee_name like '%Nancy Storey%' then 'nancystorey@brosterbuchanan.com'
	when employee_name like '%Nick Parry%' then 'charlesford@brosterbuchanan.com'
	when employee_name like '%Patrick Smith%' then 'patricksmith@brosterbuchanan.com'
	when employee_name like '%Rachel Payne%' then 'rachelpayne@brosterbuchanan.com'
	when employee_name like '%Rachel Pike%' then 'rachelpike@brosterbuchanan.com'
	when employee_name like '%Sean Hynan%' then 'rachelpike@brosterbuchanan.com'
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

, ho as (SELECT X_CLIENT.CLIENT,LE.NAME 
	FROM PROP_X_LE_CLIENTS X_CLIENT INNER JOIN PROP_LE_GEN LE ON LE.REFERENCE = X_CLIENT.LE)

, clientsource as (SELECT CG.REFERENCE,MN.DESCRIPTION as source 
	FROM PROP_CLIENT_GEN CG INNER JOIN MD_MULTI_NAMES MN ON MN.ID = CG.SOURCE
	where LANGUAGE = 1)

, currency as (SELECT CG.REFERENCE,MN.DESCRIPTION as currency 
	FROM PROP_CLIENT_GEN CG INNER JOIN MD_MULTI_NAMES MN ON MN.ID = CG.CURRENCY
	where LANGUAGE = 1)

, dup as (SELECT REFERENCE,name,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(cg.name)) ORDER BY cg.REFERENCE DESC) AS rn 
	FROM PROP_CLIENT_GEN cg where cg.status is not null)

--select * from owner

--select top 200
select
	concat('BB',cg.REFERENCE) as 'company-externalId'
	, cg.NAME as '(Original Name)'
	, ho.NAME as 'company-headQuarter'
	, iif(cg.REFERENCE in (select REFERENCE from dup where dup.rn > 1)
		, iif(dup.name = '' or dup.name is null,concat('No Company Name - ',dup.REFERENCE),concat(ltrim(rtrim(dup.name)),' ',dup.rn))
		, iif(cg.name = '' or cg.name is null,concat('No Company Name - ',cg.REFERENCE),ltrim(rtrim((cg.Name))))) as 'company-name'
	--, case 
	--	when cg.NAME in (select ltrim(cg.NAME) from PROP_CLIENT_GEN cg group by ltrim(cg.name) having count(*) > 1) then concat (dup.name,' ',dup.rn)
	--	when (cg.NAME = '' or cg.name is null) then 'No Company Name'
	--	else ltrim(replace(cg.NAME,'?','')) 
	--	end as 'company-name'
	--, cg.STATUS as '(STATUS)'
	, replace(replace(replace(replace(address.locationName,',,',','),', ,',', '),'  ',' '),' ,',',') as 'company-locationAddress'
	, replace(replace(replace(replace(address.locationName,',,',','),', ,',', '),'  ',' '),' ,',',') as 'company-locationName'
	--, address.STREET1 as 'company-locationAddress'
	, address.TOWN as 'company-locationCity'
	, address.COUNTY as 'company-locationState'
	, address.POST_CODE as 'company-locationZipCode'
	, case
        when address.DESCRIPTION like '%AUSTRALIA%' then 'AU'
		when address.DESCRIPTION like '%GERMANY%' then 'DE'
		when address.DESCRIPTION like '%IRELAND%' then 'IE'
		when address.DESCRIPTION like '%ITALY%' then 'IT'
		when address.DESCRIPTION like '%UNITED KINGDOM%' then 'GB'
		when address.DESCRIPTION like '%UNITED STATES%' then 'US'
		else 'GB' end as 'company-locationCountry'
--, cg.LOCATION as '(company-locationName)'
	--, ltrim(Stuff( Coalesce(NULLIF(address.TOWN, ''), '')
 --                       + Coalesce(' ' + NULLIF(address.COUNTY, ''), '')
 --                       + Coalesce(' ' + NULLIF(cnt.DESCRIPTION, ''), '')
 --               , 1, 0, '')) as 'company-locationName'
	, tel.TEL_NUMBER as 'company-phone'
	, sb.TEL_NUMBER as 'company-switchboard'
	, fax.TEL_NUMBER as 'company-fax'
	, left(cg.WEB_ADD,100) as 'company-website'
	--, replace(doc.DOC_ID,'.txt','.rtf') as 'company-document'
	, doc.docs AS 'company-document'
	, owner.owmerEmail as 'company-owners'
        , Stuff(  Coalesce('Company External ID: BB'+ cast(cg.REFERENCE as nvarchar(max)) + char(10),'')
				+ Coalesce('Company Owner: ' + NULLIF(owner.EMPLOYEE_NAME, '') + char(10), '')
                + Coalesce('Address Line 2: ' + NULLIF(replace(replace(replace(replace(address2.locationName,',,',','),', ,',', '),'  ',' '),' ,',','), '') + char(10), '')
               -- + Coalesce('Client Source: ' + NULLIF(cast(clientsource.source as nvarchar(max)), '') + char(10), '')
               -- + Coalesce('Currency: ' + NULLIF(cast(currency.currency as nvarchar(max)), '') + char(10), '')
			    + Coalesce('Status: ' + NULLIF(mn.Description, ''), '')
                , 1, 0, '') as 'company-note'        
       -- , mail.EMAIL_ADD as '(company-owners-email)'
from PROP_CLIENT_GEN cg --4423
left join tel ON CG.REFERENCE = tel.REFERENCE
left join address ON cg.REFERENCE = address.REFERENCE
left join address2 ON cg.REFERENCE = address2.REFERENCE
left join (select REFERENCE,CONFIG_NAME,TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Fax') fax ON cg.REFERENCE = fax.REFERENCE
left join dup on cg.REFERENCE = dup.REFERENCE
left join (SELECT REFERENCE, EMAIL_ADD = STUFF((SELECT DISTINCT ', ' + EMAIL_ADD FROM PROP_EMAIL b WHERE b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM PROP_EMAIL a GROUP BY REFERENCE) mail ON cg.REFERENCE = mail.REFERENCE
left join owner ON cg.REFERENCE = owner.CLIENT_GEN_REFERENCE --PERSON_GEN.REFERENCE = <<PROP_CLIENT_GEN.REFERENCE>>
left join doc on cg.REFERENCE = doc.OWNER_ID
left join ho on CG.REFERENCE = ho.CLIENT
left join (SELECT REFERENCE,TEL_NUMBER FROM PROP_TELEPHONE WHERE OCC_ID =2034418) sb on CG.REFERENCE = sb.REFERENCE
left join clientsource on cg.REFERENCE = clientsource.REFERENCE
left join currency on cg.REFERENCE = currency.REFERENCE
left join MD_MULTI_NAMES MN ON MN.ID = cg.STATUS
where cg.status is not null and mn.LANGUAGE = 1--and cg.name like '%Whitbread%'
UNION ALL
select 'BB9999999','','','Default Company','','','','','','','','','','','','','This is Default Company from Data Import'


