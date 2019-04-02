with test as (select cli_no,replace(cli_reg_date,left(cli_reg_date,4),left(cli_reg_date,4)+'-') as cli_reg_date from client where cli_reg_date <> 0),
test2 as (select cli_no,replace(cli_updated,left(cli_updated,4),left(cli_updated,4)+'-') as cli_updated from client where cli_updated <> 0)

,test3 as (select
a.cli_no															as 'company-externalId',
iif(cli_name = '','No Company Name',cli_name)						as 'company',
concat(cli_establish, nullif(concat(', ',cli_street),', '), nullif(concat(', ',cli_district),', '))		as 'company-locationAddress',
concat(cli_establish, nullif(concat(', ',cli_street),', '), nullif(concat(', ',cli_district),', '))		as 'company-locationName',
cli_town														as 'company-locationCity',
cli_county														as 'company-locationState',
cli_postcode													as 'company-locationZipCode',
case when cli_country = 'Netherlands' then 'NL'
when cli_country = 'United States of Ame' then 'US'
when cli_country = 'Germany' then 'DE'
when cli_country = 'Guernsey' then 'GG'
when cli_country = 'United Kingdom' then 'GB'
when cli_country = 'Austria' then 'AT'
when cli_country = 'United Arab Emirates' then 'AE'
when cli_country = 'Ireland' then 'IE'
when cli_country = 'Latvia' then 'LV'
when cli_country = 'Denmark' then 'DK'
else '' end														as 'company-country',
[dbo].[udf_GetNumeric](cli_tel)									as 'company-phone',
cli_www															as 'company-website',
concat(
'External ID: ', a.cli_no, (char(13)+char(10)),
nullif(concat('Region: ', cli_region,(char(13)+char(10))),concat('Region: ',(char(13)+char(10)))),
nullif(concat('Reg Date: ', replace(b.cli_reg_date,left(b.cli_reg_date,7),concat(left(b.cli_reg_date,7),'-')),(char(13)+char(10))),concat('Reg Date: ',(char(13)+char(10)))),
nullif(concat('Updated: ', replace(c.cli_updated,left(c.cli_updated,7),concat(left(c.cli_updated,7),'-'))),concat('Updated: ',(char(13)+char(10)))),
nullif(concat('Source: ', cli_source,(char(13)+char(10))),concat('Source: ',(char(13)+char(10))))
)																as 'company-notes'
,ROW_NUMBER() over (partition by a.cli_name order by a.cli_name) as rn

from client a left join test b on a.cli_no = b.cli_no
left join test2 c on a.cli_no = c.cli_no
where cli_name <> '')

select iif(rn=1,company,concat(rn,' - ',company)) as 'company-name',* from test3