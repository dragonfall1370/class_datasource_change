--Sirius Industry
---Table input
select VCIndustry as SiriusIndustry
, getdate() as Sirius_insert_timestamp 
from SiriusIndustry


--Sirius FE
---Table Input
select distinct VCFE as SiriusFE --40 rows
, getdate() as Sirius_insert_timestamp 
from SiriusFE

--Sirius SFE
---Table Input


--Mapping


--COMPANY INDUSTRY
---Table input: 
select ID, INDUSTRY_SECTORS__C
from Company

---Mapping:



--CONTACT INDUSTRY
---Table input:
select ID, INDUSTRY_SECTORS__C
from Contact

---Mapping:


--CONTACT FE / SFE >> Sirius_contact_v2.sql
-->>Table input
with FunctionalExp as (select ID
, concat_ws(';',ACCOUNTING_FINANCE__C
, DEVELOPMENT_QUALIFICATION__C
, INFRASTRUCTURE_QUALIFICATION__C
, BI_DATA_CRM_QUALIFICATION__C
, PROJECT_SERVICES_QUALIFICATION__C
, SUPPORT__C
, INDUSTRIOUS__C
, SSM__C
, COMPANIES_PACKAGES__C
, DIGITAL_QUALIFICATION__C) as FExp
from Contact)

select ID as Sirius_ContactExtID
, value as Sirius_FunExp
, getdate() as Sirius_insert_timestamp
	from FunctionalExp
	CROSS APPLY STRING_SPLIT(FExp, ';')
	where FExp <> ''
	
-->>Mapping
--contact_id
--functional_expertise_id
--insert_timestamp
--sub_functional_expertise_id


---->> UPDATE CONTACT SFE (csv file received on 01 Mar 2018)
with SubFunctionalExp as (select ID
	, concat_ws(';',ACCOUNTING_FINANCE__C
	, A_F_SUB_COMMUNITY__C
	, DEVELOPMENT_QUALIFICATION__C
	, DEVELOPMENT_SKILLSET__C
	, INFRASTRUCTURE_QUALIFICATION__C
	, INFRA_SKILLSET__C
	, BI_DATA_CRM_QUALIFICATION__C
	, BI_DATA_CRM_SKILLSET__C
	, PROJECT_SERVICES_QUALIFICATION__C
	, PROJ_SERVICES_SKILLSET__C
	, SUPPORT__C
	, SUPPORT_SUB_COMMUNITIES__C
	, INDUSTRIOUS__C
	, INDUSTRIOUS_SUB_COMMUNITIES__C
	, SSM__C
	, SSM_SUB_COMMUNITIES__C
	, COMPANIES_PACKAGES__C
	, DIGITAL_QUALIFICATION__C
	, DIGITAL_SKILLS__C) as SubFE
	from ContactFE
)

, FinalSFE as (select ID as Sirius_ContactExtID
	, value as Sirius_SubFE
	from SubFunctionalExp
	CROSS APPLY STRING_SPLIT(SubFE, ';')
	where SubFE <> '')

select Sirius_ContactExtID
, Sirius_SubFE
, VCSFE
, VCFE
, getdate() as Sirius_insert_timestamp
from FinalSFE
left join SiriusFE on SiriusFE.SiriusFE = FinalSFE.Sirius_SubFE --166966
--where VCSFE is not NULL --151393
--where VCSFE is NULL --15573


--->> UPDATE CONTACT FE (csv file updated on 01 Mar 2018)
with FunctionalExp as (select ID
, concat_ws(';',ACCOUNTING_FINANCE__C
, A_F_SUB_COMMUNITY__C
, DEVELOPMENT_QUALIFICATION__C
, DEVELOPMENT_SKILLSET__C
, INFRASTRUCTURE_QUALIFICATION__C
, INFRA_SKILLSET__C
, BI_DATA_CRM_QUALIFICATION__C
, BI_DATA_CRM_SKILLSET__C
, PROJECT_SERVICES_QUALIFICATION__C
, PROJ_SERVICES_SKILLSET__C
, SUPPORT__C
, SUPPORT_SUB_COMMUNITIES__C
, INDUSTRIOUS__C
, INDUSTRIOUS_SUB_COMMUNITIES__C
, SSM__C
, SSM_SUB_COMMUNITIES__C
, COMPANIES_PACKAGES__C
, DIGITAL_QUALIFICATION__C
, DIGITAL_SKILLS__C) as FExp
from ContactFE
)

, FinalFE as (select ID as Sirius_ContactExtID
, value as Sirius_FunExp
	from FunctionalExp
	CROSS APPLY STRING_SPLIT(FExp, ';')
	where FExp <> '')

select Sirius_ContactExtID
--distinct Sirius_ContactExtID
, Sirius_FunExp
, VCFE
, getdate() as Sirius_insert_timestamp
from FinalFE
left join SiriusFE on SiriusFE.SiriusFE = FinalFE.Sirius_FunExp
where VCFE is not NULL --151435 | distinct 6939