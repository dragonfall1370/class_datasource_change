with 
--document as (select code,string_agg(cast(trim(filename) as varchar(max)),',') as filename

--from attachment where table1 = 'CL' group by code),

companylocation as (select * from CMLFCLIENTLOC where locaddress <> '' and location = 'main')

,test as (select
code as 'company-externalId',
iif(client is null or client = '','',client) as 'company-name',
iif(address = '' or address is null,'',address) as 'company-address',
iif(postcode is null or postcode = '','',postcode) as 'company-postal',
iif(tel is null or tel = '','',replace(tel,'/','-')) as 'company-phone',
iif(url is null or url = '','',url) as 'company-website',
concat('External ID: ',code,(char(13)+char(10)),
nullif(concat('Note: ',notes),'Note: ')) as 'company-notes',
row_number() over (partition by client order by client) as rn
from Client)

select a.*
,iif(b.locaddress is null or b.locaddress = '','',b.locaddress) as 'company-address'
,iif(b.locaddress is null or b.locaddress = '','',b.locaddress) as 'company-location'
,iif(rn=1,[company-name],concat(rn,'-',[company-name])) as 'company-name2' from test a
left join companylocation b on a.[company-externalId] = trim(b.clientcode)