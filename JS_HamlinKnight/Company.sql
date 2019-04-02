--select convert(datetime, CONVERT(float,date_column))

with main_address as (select * from address where main_address = 'Y')
, main_organisation as (select *,ROW_NUMBER() over (partition by name order by name) as rn from organisation)
, linkedin as (select * from linksite where parent_object_name = 'organisation')
--, company_document as (select linkfile_ref,parent_object_name,parent_object_ref,file_extension,
--case when reverse(file_extension) = left(reverse(displayname),3) 
----then displayname end as a
--then replace(displayname,concat('.',file_extension),'') else displayname end as displayname
--from linkfile where parent_object_name = 'organisation')
--,
--FileName as (
--SELECT parent_object_ref as 'ClientID',
--    STUFF((SELECT DISTINCT ', ' + concat(displayname,'.',file_extension)
--           FROM company_document a 
--           WHERE parent_object_ref = parent_object_ref
--          FOR XML PATH('')), 1, 2, '') as 'Company_File'
--FROM company_document b
--GROUP BY parent_object_ref )
--,FileName as (select parent_object_ref as 'ClientID', string_agg(concat(displayname,'.',file_extension),',') as 'Company_File' from company_document group by parent_object_ref)

,test3 as (select *, row_number() over (partition by organisation_ref order by organisation_ref) as rn3 from organisation)
,owners2 as (select a.displayname,a.organisation_ref,a.responsible_user, b.email_address from test3 a left join person b on a.responsible_user = b.person_ref)

select 
a.organisation_ref as 'company-externalId',
iif(a.rn = 1,a.name,concat(a.rn,'-',a.name)) as 'company-name',
iif(a.web_site_url is null or a.web_site_url = '','',iif(a.web_site_url = 'Website...' or a.web_site_url = 'http://','',a.web_site_url)) as 'company-website',
iif(e.email_address is null or e.email_address ='','',e.email_address) as 'company-owners',
concat(iif(b.address_line_1 is null or b.address_line_1 = '','',b.address_line_1), 
nullif(concat(', ',b.post_town),', '),
nullif(concat(', ',b.county_state),', '),
nullif(concat(', ',b.zipcode),', ')
) as 'company-address',
iif(b.address_line_1 = '' or b.address_line_1 is null,'',b.address_line_1) as 'company-locationName',
iif(b.county_state = '' or b.county_state is null,'',b.county_state) as 'company-locationState',
iif(b.post_town = '' or b.post_town is null,'',b.post_town) as 'company-locationCity',
iif(b.zipcode is null or b.zipcode ='','',b.zipcode) as 'company-locationZipCode',
case when c.en_short_name = b.country then c.alpha_2_code
when b.country = 'United Kingdom' then 'GB'
else '' end
as 'company-locationCountry',
iif(b.telephone_number is null or b.telephone_number ='','',dbo.udf_GetNumeric(b.telephone_number)) as 'company-phone',
iif(d.website_url is null or d.website_url ='','',d.website_url) as 'company-linkedin',
concat(nullif(concat('Parent Company: ',a.parent_organ_ref,(char(13)+char(10))),concat('Parent Company: ',(char(13)+char(10)))),
nullif(concat('Client Type: ',a.type),'Client Type: '))
as 'company-note'
--,iif(f.Company_File = '' or f.Company_File is null,'',f.Company_File) as 'company-document'

from main_organisation a
left join main_address b on a.organisation_ref = b.organisation_ref
left join countries c on b.country = c.en_short_name
left join linkedin d on a.organisation_ref = d.parent_object_ref
left join owners2 e on a.responsible_user = e.organisation_ref
--left join FileName f on a.organisation_ref = f.ClientID






