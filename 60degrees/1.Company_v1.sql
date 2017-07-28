
with tmp1 as (select id
, case when (addr2 = '' OR addr2 is NULL) THEN '' ELSE concat('Address 2: ', addr2) END as addr2
, case when (created_date = '' OR created_date is NULL) THEN '' ELSE concat('Created Date: ', created_date) END as created_date
, case when (cast(overview as varchar(max)) = '' OR overview is NULL) THEN '' ELSE concat('Overview: ', addr2) END as overview
, case when (industry = '' OR industry is NULL) THEN '' ELSE concat('Industry: ', industry) END as industry
from company)
--select * from tmp1

, tmp2 (id, company_note) as (select id, concat(addr2, char(10), created_date, char(10), overview, char(10), industry)
from tmp1)
--select ltrim(company_note) from tmp2

, tmp3 as (select CB.id
, CB.comp_id
, A.name
, CB.name as branch_name
, case when (CB.addr2 = '' OR CB.addr2 is NULL) THEN '' ELSE concat('Address 2: ', CB.addr2) END as addr2
, case when (CB.createdate = '' OR CB.createdate is NULL) THEN '' ELSE concat('Created Date: ', CB.createdate) END as createdate
from company_branch CB
left join company A on A.id = CB.comp_id)
--select * from tmp3

, tmp4 as (select id
, concat(createdate, char(10), 'Parent company: ', name, char(10), 'Parent CompanyID: ', char(10), addr2) as branch_name_note
from tmp3)

select 
  A.id as 'company-externalId'
, A.name as 'company-name'
, A.contactphone1 as 'company-phone'
, A.addr1 as 'company-locationName'
, A.addr1 as 'company-locationAddress'
, A.city as 'company-locationCity'
, A.state as 'company-locationState'
, A.zip as 'company-locationZipCode'
, CC.Code as 'company-locationCountry'
, A.rep as 'companyownersID'
, B.user_email as 'company-owners'
, A.website as 'company-website'
, C.company_note as 'company-note'
from company A
left join users B on A.rep = B.ID
left join tmp2 C on A.id = C.id
left join CountryCode CC on A.country = CC.Name

UNION

select CB.id as 'company-externalId'
, concat(A.name,' - ',CB.name) as 'company-name'
, CB.phone as 'company-phone'
, CB.addr1 as 'company-locationName'
, CB.addr1 as 'company-locationAddress'
, CB.city as 'company-locationCity'
, CB.state as 'company-locationState'
, CB.zip as 'company-locationZipCode'
, CC.Code as 'company-locationCountry'
, '' as 'companyownersID'
, '' as 'company-owners'
, '' as 'company-website'
, tmp4.branch_name_note as 'company-note'
from company_branch CB
left join tmp4 on CB.id = tmp4.id
left join company A on CB.comp_id = A.id
left join CountryCode CC on CB.country = CC.Name
where CB.name <> 'Main'
