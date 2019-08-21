with 
document as (select code,string_agg(cast(trim(filename) as varchar(max)),',') as filename

from attachment where table1 = 'CL' group by code)
,test as (select 
code as 'company-externalId',
client as 'company',
address as 'company-address',
address as 'company-locationname',
replace(tel,'/','-') as 'company-phone',
fax as 'company-fax',
url as 'company-website',
invoiceadd as 'billing-address',
concat('External ID: ',code,(char(13)+char(10)),
nullif(concat('Invoice Contact: ',invoicecon,(char(13)+char(10))),concat('Invoice Contact: ',(char(13)+char(10)))),
nullif(concat('Invoice Contact Phone Number: ',invoiceco2,(char(13)+char(10))),concat('Invoice Contact Phone Number: ',(char(13)+char(10)))),
nullif(concat('Invoice Contact Fax Number: ',invoiceco3,(char(13)+char(10))),concat('Invoice Contact Fax Number: ',(char(13)+char(10)))),
nullif(concat('Note: ',notes),'Note: '))
as 'company-notes',
ROW_NUMBER() over (partition by client order by client) as rn
from Client)

select iif(rn=1,company,concat(rn,'-',company)) as 'company-name',a.*,iif(b.filename is null or b.filename = '','',b.filename) as 'company-document' from test a
left join document b on a.[company-externalId] = b.code