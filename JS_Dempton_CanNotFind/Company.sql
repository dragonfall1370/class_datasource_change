with test as (select distinct enterby, b.email from company a left join  MhUsers b on a.enterby = b.username)

, companydup as (select id, compname, ROW_NUMBER() over (partition by compname order by compname) as 'row_num' from company)

, parentcompany as (
select a.Company2ID ,b.compname
from CompanyRelationships a
left join company b on a.Company2ID = b.id)

select
a.id as 'company-externalId',
iif(d.row_num = 1,d.compname,concat(d.row_num, '-' ,d.compname)) as 'company-name',
iif(c.email is null or c.email = '','',c.email) as 'company-owner',
concat(
nullif(a.address1 + ', ',', '),
nullif(a.city + ', ',', '),
nullif(a.state + ', ',', '),
nullif(a.zip,''))
as 'company-locationName',
concat(
nullif(a.address1 + ', ',', '),
nullif(a.city + ', ',', '),
nullif(a.state + ', ',', '),
nullif(a.zip,''))
as 'company-Address',
concat(nullif(concat('(',a.phone_area,') '),'(0)'),a.phone) as 'company-phone',
concat(nullif(concat('(',a.phone2_area,') '),'(0)'),a.phone2) as 'company-phone2',
iif(a.web is null or a.web = '','',a.web) as 'company-website',
iif(a.city is null or a.city = '','',a.city) as 'company-locationCity',
iif(a.state is null or a.state = '','',a.state) as 'company-locationState',
iif(a.zip is null or a.zip = '','',a.zip) as 'company-locationZipCode',
case when a.locale = 'CA' then 'CA'
when a.locale = 'cananda' then 'CA'
when a.locale = 'Canada' then 'CA'
when a.locale = 'USA' then 'US'
when a.locale = 'Australia' then 'AU'
else '' end as 'company-locationCountry',
iif(a.geo is null or a.geo = '','',a.geo) as 'company-locationDistrict',
concat(
nullif(concat('Parent Company: ', e.compname,(char(13)+char(10))),concat('Parent Company: ',(char(13)+char(10)))),
nullif(concat('Company Status: ',iif(a.active = 1,'Active','Unactive')),'Company Status: '), (char(13)+char(10)),
nullif(concat('Sic: ',a.sic),'Sic: '), (char(13)+char(10)),
nullif(concat('Pipeline Status: ',b.companystatusname),'Pipeline Status: '), (char(13)+char(10)),
nullif(concat('Description: ',a.descript),'Description: '))
as 'company-Note'

from company a
left join CompanyStatus b on a.CompanyStatus_ID = b.CompanyStatus_ID
left join test c on a.enterby = c.enterby
left join companydup d on a.id = d.id
left join parentcompany e on a.id = e.Company2ID
