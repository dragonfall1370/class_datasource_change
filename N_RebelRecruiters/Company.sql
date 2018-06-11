---DUPLICATION REGCONITION
with loc1 as (
	select client_ref, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(client_add, ''), '')
			+ Coalesce(', ' + NULLIF(client_add2, ''), '')
			+ Coalesce(', ' + NULLIF(client_town, ''), '')
			+ Coalesce(', ' + NULLIF(client_county, ''), '')
				+ Coalesce(', ' + NULLIF(client_pcode, ''), '')
			+ Coalesce(', ' + NULLIF('United Kingdom (Great Britain)', ''), '')
			, 1, 1, '')) as 'locationName'
	from client)

, loc2 as (
select c.client_ref, l.description
from client c left join clientlocation cl on c.client_ref = cl.client_ref
left join location l on cl.loc_ref = l.loc_ref
where cl.loc_ref <> 0)

, dup as (SELECT client_ref, client_name, ROW_NUMBER() OVER(PARTITION BY client_name ORDER BY client_ref ASC) AS rn 
FROM client)

, owners as (select consult_ref, consult_name
	, case consult_name 
		when 'Support' then 'zed@rebelrecruiters.co.uk'
		when 'Azar Hussain' then 'azar@rebelrecruiters.co.uk'
		when 'Mica Bell' then 'mica@rebelrecruiters.co.uk'
		when 'Faisal Faik' then 'fess@rebelrecruiters.co.uk'
		when 'Yas Mahtab' then 'yas@rebelrecruiters.co.uk'
		when 'Loukia Poutziouris' then 'loukia@rebelrecruiters.co.uk'
		when 'Hamzah Ikram' then 'hamzah@rebelrecruiters.co.uk'
		when 'Julija Lipnickaja' then 'julija@rebelrecruiters.co.uk'
		when 'Ben Williamson' then 'ben@rebelrecruiters.co.uk'
		when 'Hayley McGowan' then 'hayley@rebelrecruiters.co.uk'
		else '' end as consult_email
from consultant where consult_inits <> '')

----select * from dup
---Main Script---
select
  concat('REBEL',c.client_ref) as 'company-externalId'
, C.client_name as '(OriginalName)'
, iif(C.client_ref in (select client_ref from dup where dup.rn > 1)
	, iif(dup.client_name = '' or dup.client_name is NULL,concat('No Company Name - ',dup.client_ref),concat(dup.client_name,' - ',dup.rn))
	, iif(C.client_name = '' or C.client_name is null,concat('No Company Name - ',C.client_ref),C.client_name)) as 'company-name'
, como.consult_email as 'company-owners'
, replace(replace(coalesce(loc1.locationName, loc2.description),',,',','),', ,',',') as 'company-locationName'
, coalesce(loc1.locationName, loc2.description) as 'company-locationAddress'
, c.client_town as 'company-locationCity'
, c.client_county as 'company-locationState'
, c.client_pcode as 'company-locationZipCode'
, 'GB' as 'company-locationCountry'
, client_switchboard as 'company-switchBoard'
, coalesce(cd.client_ref + '_' + cd.doc_ref + '.' + doc_ext,'') as 'company-document'
, left(Concat(
			'Company External ID: REBEL', C.client_ref,char(10)
			, iif(CHARINDEX(':',client_datereg)<>0,concat(char(10), 'Registered Date (dd/mm/yyyy): ', substring(client_datereg,6,2),'/',substring(client_datereg,9,2),'/',left(client_datereg,4),char(10)),concat(char(10), 'Registered Date: ',client_datereg,char(10)))
			--, iif(c.client_datereg = '' or c.client_datereg is NULL,'',Concat(char(10), 'Registered Date: ', c.client_datereg, char(10)))
			, iif(como.consult_name = '' or como.consult_name is NULL,'',Concat(char(10), 'Consultant name: ', como.consult_name, char(10)))
			, iif(c.client_rate = '' or c.client_rate is NULL,'',Concat(char(10), 'Rate: ', c.client_rate, char(10)))
			, iif(loc2.description = '' or loc2.description is NULL,'',Concat(char(10), 'Location: ', loc2.description, char(10)))
			, iif(c.client_notes = '' or c.client_notes is NULL,'',Concat(char(10),'Notes: ',char(10),c.client_notes))),32000)
			as 'company-note'
FROM client as c
			left join dup on c.client_ref = dup.client_ref
			left join loc1 on c.client_ref = loc1.client_ref
			left join loc2 on c.client_ref = loc2.client_ref
			left join owners como on c.client_consult = como.consult_ref
			left join clientdocs cd on c.client_ref = cd.client_ref
--			where dup.rn>1
--where c.client_ref = 2
UNION ALL
select 'REBEL9999999','','Default Company','','','','','','','','','','This is Default Company from Data Import'

