-------------
--PART 1: MAIN SCRIPT
-------------
with
--DUPLICATION REGCONITION
dup as (SELECT ID, replace(NAME,'%','') as NAME, ROW_NUMBER() OVER(PARTITION BY replace(NAME,'%','') ORDER BY ID ASC) AS rn 
FROM Company)

/* Support in SQL Server 2016
--TERM OF BUSINESS -- not included before Sirius confirm
, CompTermBusiness as (SELECT
     CLIENT__C,
     STUFF(
         (SELECT ',' + NAME
          from  TermBusiness
          WHERE CLIENT__C = a.CLIENT__C
		  order by CREATEDDATE desc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS CompTermBusiness
FROM TermBusiness as a
GROUP BY a.CLIENT__C)

--CV FLOATED -- not included before Sirius confirm
, CompCVFloated as (SELECT
     COMPANY__C,
     STUFF(
         (SELECT ',' + NAME
          from  CVFloated
          WHERE COMPANY__C = a.COMPANY__C
		  order by CREATEDDATE desc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS CompCVFloated
FROM CVFloated as a
where COMPANY__C is not NULL
GROUP BY a.COMPANY__C)
*/

--TERM OF BUSINESS --added as company note
, CompTermBusiness as (select CLIENT__C
	, STRING_AGG(concat(NAME,' - ',CREATEDDATE), ', ') as CompTermBusiness
	from TermBusiness
	group by CLIENT__C)

, TermBusinessFinal as (select tb.CLIENT__C
	, STRING_AGG(a.NAME, ',') as TermBusinessFinal
	from (select distinct CLIENT__C from TermBusiness) tb
	left join Attachments a on a.PARENTID = tb.CLIENT__C
	group by tb.CLIENT__C)

--COMPANY ATTACHMENTS /* Support from SQL Server 2017 */
, CompAttachments as (select PARENTID
	, STRING_AGG(NAME, ', ') as CompAttachments
	from Attachments
	where NAME like '.%doc' or NAME like '%.pdf' or NAME like '%.rtf' or NAME like '%.xls%' or NAME like '%.html'
	group by PARENTID)

--MAIN SCRIPT
select c.ID as 'company-externalId'
, iif(c.ID in (select ID from dup where dup.rn > 1)
	, iif(dup.NAME = '' or dup.NAME is NULL,concat('Company name -',dup.ID),concat(dup.NAME,'-DUPLICATE-',dup.ID))
	, iif(c.NAME = '' or c.NAME is null,concat('Company name -',dup.ID),replace(c.NAME,'%',''))) as 'company-name'
--, c.NAME as CompanyOriginal
, ltrim(rtrim(c.PHONE)) as 'company-phone'
, left(ltrim(rtrim(c.WEBSITE)),100) as 'company-website'
, nullif(ltrim(stuff((coalesce(' ' + nullif(rtrim(ltrim(c.BILLINGSTREET)),''),'') + coalesce(', ' + nullif(rtrim(ltrim(c.BILLINGSTATE)),''),'') 
	+ coalesce(', ' + nullif(rtrim(ltrim(c.BILLINGCITY)),''),'') + coalesce(', ' + nullif(rtrim(ltrim(c.BILLINGPOSTALCODE)),''),'') 
	+ coalesce(', ' + nullif(rtrim(ltrim(c.BILLINGCOUNTRY)),''),'')),1,1,'')),'') as 'company-locationName'
, ltrim(stuff((coalesce(' ' + nullif(rtrim(ltrim(c.BILLINGSTREET)),''),'') + coalesce(', ' + nullif(rtrim(ltrim(c.BILLINGSTATE)),''),'') 
	+ coalesce(', ' + nullif(rtrim(ltrim(c.BILLINGCITY)),''),'') + coalesce(', ' + nullif(rtrim(ltrim(c.BILLINGPOSTALCODE)),''),'') 
	+ coalesce(', ' + nullif(rtrim(ltrim(c.BILLINGCOUNTRY)),''),'')),1,1,'')) as 'company-locationAddress'  --Remove Billing Address prefix in PROD script
, c.BILLINGCITY as 'company-locationCity'
, c.BILLINGSTATE as 'company-locationState'
, c.BILLINGPOSTALCODE as 'company-locationZipCode'
, case
	when c.BILLINGCOUNTRY in ('USA','United States') then 'US'
	when c.BILLINGCOUNTRY in ('New Zealand','NZ') then 'NZ'
	when c.BILLINGCOUNTRY = 'Hong Kong' then 'HK'
	when c.BILLINGCOUNTRY = 'Malaysia' then 'MY'
	when c.BILLINGCOUNTRY in ('England','United Kingdom') then 'GB'
	when c.BILLINGCOUNTRY in ('Sinapore','Singapore') then 'SG'
	when c.BILLINGCOUNTRY = 'Aruba' then 'AW'
	when c.BILLINGCOUNTRY = 'Bahamas' then 'BS'
	when c.BILLINGCOUNTRY in ('AUSTRALIA','Sydney','Australua') then 'AU'
	when c.BILLINGCOUNTRY = 'India' then 'IN'
	when c.BILLINGCOUNTRY = 'Austria' then 'AT'
	when c.BILLINGCOUNTRY = 'Canada' then 'CA'
	when c.BILLINGCOUNTRY = 'Ireland' then 'IE'
	when c.BILLINGCOUNTRY = 'japan' then 'JP'
	when c.BILLINGCOUNTRY = 'Papua New Guinea' then 'PG'
	when c.BILLINGCOUNTRY = 'Cambodia' then 'KH'
	when c.BILLINGCOUNTRY = 'South Africa' then 'ZA'
	else NULL end as 'company-locationCountry'
, concat(coalesce(ltrim(su.EMAIL),''), coalesce(',' + ltrim(su2.EMAIL),'')) as 'company-owners'
--, concat(case when ctb.CompTermBusiness = '' or ctb.CompTermBusiness is NULL then ''
--	when ctb.CompTermBusiness <> '' and (ccf.CompCVFloated = '' or ccf.CompCVFloated is NULL) then ctb.CompTermBusiness
--	else concat(ctb.CompTermBusiness,', ') end,
--	case when ccf.CompCVFloated = '' or ccf.CompCVFloated is NULL then ''
--	else ccf.CompCVFloated end) as 'company-document'
, stuff((coalesce(',' + nullif(ltrim(ca.CompAttachments),''),'') + coalesce(',' + nullif(ltrim(tbf.TermBusinessFinal),''),'')
	), 1, 1,'') as 'company-document'
, concat('Company External ID: ', c.ID, char(10)
	, coalesce('Parent ID: ' + c.PARENTID + char(10),'')
	, coalesce('Description: ' + c.DESCRIPTION + char(10),'')
	, coalesce('Last activity date: ' + convert(varchar(20),c.LASTACTIVITYDATE,120) + char(10),'')
	, coalesce('Division: ' + c.DIVISION__C + char(10),'')
	, coalesce('Business Number - ABN: ' + c.ABN_ACN__C + char(10),'')
	, coalesce('Company Industry: ' + c.INDUSTRY_SECTORS__C + char(10),'')
	, coalesce('Awards: ' + c.MODERN_AWARD__C + char(10),'')
	, coalesce('Special Term Conditions: ' + c.SPECIAL_TERMS_CONDITIONS__C + char(10),'')
	, coalesce('Workplace OHS Assessment Completed: ' + c.WORKPLACE_OHS_ASSESSMENT_COMPLETED__C + char(10),'')
	, coalesce('Workplace OHS Date Completed: ' + convert(varchar(20),c.WORKPLACE_OHS_DATE_COMPLETED__C,120) + char(10),'')
	, coalesce('Company ID Hidden: ' + c.COMPANY_ID_HIDDEN__C + char(10),'')
	, coalesce('Terms of Business: ' + nullif(ctb.CompTermBusiness,''),'')
	--when ctb.CompTermBusiness <> '' and (ccf.CompCVFloated = '' or ccf.CompCVFloated is NULL) then ctb.CompTermBusiness
	--else concat(ctb.CompTermBusiness,', ') end,
	--case when ccf.CompCVFloated = '' or ccf.CompCVFloated is NULL then ''
	--else ccf.CompCVFloated end),'')
) as 'company-note'
from Company c
left join dup on dup.ID = c.ID
left join SiriusUsers su on su.ID = c.OWNERID
left join SiriusUsers su2 on su2.ID = c.ACCOUNT_MANAGER__C
left join CompTermBusiness ctb on ctb.CLIENT__C = c.ID
--left join CompCVFloated ccf on ccf.COMPANY__C = c.ID
left join CompAttachments ca on ca.PARENTID = c.ID
left join TermBusinessFinal tbf on tbf.CLIENT__C = c.ID
--where c.ID in ('0019000000pCrDgAAK' , '0019000000pCrsDAAS')

UNION ALL

select 'SP999999999','Default company','','','','','','','','','','','This is default company from data import'

-------------
--PART 2: INJECT COMPANY ACTIVITIES
-------------
/* Query from DB: PAYMENT HISTORY */
select ph.ACCOUNT__C as CompanyExtID
	, -10 as Sirius_user_account_id
	, Concat(concat('Payment name: ',ph.NAME),char(10)
		 + concat('Created date: ', ph.CREATEDDATE,char(10))
		 + concat('Created by: ',su.NAME,' | ',su.EMAIL),char(10)
		 + concat('Note payment info: ',ph.NOTES_PAYMENT_INFO__C)
		 ) as Sirius_comments
	, CONVERT(varchar(20), ph.CREATEDDATE, 120) as Sirius_insert_timestamp
	, 'comment' as Sirius_category
	, 'company' as Sirius_type
	from PaymentHistory ph
	left join SiriusUSers su on su.ID = ph.CREATEDBYID

/* Running process */

-----

/* Query from DB: COMPANY TASKS -- added from 7-Feb-2018 */
select t.ACCOUNTID as CompanyExtID
	, -10 as Sirius_user_account_id
	, Concat(concat('Subject: ', t.SUBJECT),char(10)
		 + concat('Status: ', t.STATUS,char(10))
		 + concat('Owner by: ', su.EMAIL,' - ',su.NAME,char(10))
		 + concat('Short description: ', t.SHORT_DESCRIPTION__C,char(10))
		 + concat('*** Completed date: ', convert(varchar(20),t.COMPLETED_DATE__C,120))
		 ) as Sirius_company_activity
	, getdate() as Sirius_insert_timestamp
	, 'comment' as Sirius_category
	, 'company' as Sirius_type
	from Tasks t
	left join SiriusUsers su on su.ID = t.OWNERID
	where t.ACCOUNTID is not NULL

/* Running process */

-------------
--PART 3: CUSTOM FIELDS
-------------

/*1. Owner includes OWNERID | ACCOUNT_MANAGER__C */

/* 2. Company CF: Division */

--GET ALL CF TYPE & FIELD_ID & FIELD_VALUE from Custom field - company
--VERSION 1
select a.form_id, cf.type, cffv.field_id, a.translate, cffv.field_value, cfl.translate, cfl.language_code
from configurable_form_field_value cffv
left join configurable_form_language cfl on cffv.title_language_code = cfl.language_code
left join (select cff.form_id, cff.id, cff.field_type, cfl.language_code, cfl.translate
	from configurable_form_field cff
	left join configurable_form_language cfl on cff.label_language_code = cfl.language_code) a on a.id = cffv.field_id
left join configurable_form cf on cf.id = a.form_id
order by cffv.field_id, cffv.title_language_code

--VERSION 2
select cff.form_id, cf.type, cff.id, cfl2.translate, cffv.field_value, cfl.translate, cfl.language_code
from configurable_form_field cff
left join configurable_form_language cfl2 on cff.label_language_code = cfl2.language_code
left join configurable_form_field_value cffv on cff.id = cffv.field_id
left join configurable_form_language cfl on cffv.title_language_code = cfl.language_code
left join configurable_form cf on cf.id = cff.form_id
order by cffv.field_id, cffv.title_language_code, cff.id

--Table Input
select 'add_comp_info' as Sirius_additional_type
, ID as Sirius_CompExtID
, ?? as Siriusform_id
, ?? as Siriusfield_id
, DIVISION__C as Sirius_Division
, getdate() as Sirius_insert_timestamp
from Company

--Mapping
-->> Sirius_CompExtID >> external_id (lookup) >> id (select id from company)
-->> additional_type | form_id | field_id | field_value = Sirius_Division | insert_timestamp

/* 3. Business number */
select business_number from company --from Vincere

--Table Input
select ID as Sirius_CompExtID
, concat('ABN: ',ABN_ACN__C) as Sirius_BusinessNo
from Company

--Mapping
-->> Sirius_BusinessNo >> business_number
-->> Sirius_CompExtID >> external_id (lookup) >> id (select id from company)

--Insert/Update process

/* 4. Industry sector */
--Table Input
select ID as Sirius_CompExtID
, INDUSTRY_SECTORS__C as Sirius_CompanyIndustry
, getdate() as Sirius_insert_timestamp
from Company
where INDUSTRY_SECTORS__C is not NULL

UNION ALL

select ID
, INDUSTRY_SECTORS__C as Sirius_CompanyIndustry
, getdate() as Sirius_insert_timestamp
from CompanyDelta
where INDUSTRY_SECTORS__C is not NULL

--total: 13747 rows

--Lookup
---Sirius_CompExtID --> VCCompExtID
---Sirius_CompanyIndustry --> VCIndustryID

--Mapping (select * from company_industry)
---industry_id = VCIndustryID
---company_id = VCCompExtID
---insert_timestamp = Glocap_insert_timestamp