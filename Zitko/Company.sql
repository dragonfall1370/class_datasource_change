with filedup as (select Id,iif(Attachment.Name is null or Attachment.Name = '','',Attachment.Name) as filename,ParentId,Name from Attachment)

,filename as (select *,ROW_NUMBER() over ( partition by filename order by filename) as 'row_num' from filedup),

filename2 as (select id, iif(row_num = 1,filename,concat(row_num,'-',filename)) as filename,ParentId,Name from filename),

companyname as ( select id,name, row_number() over(partition by name order by name) as name_num from Account),

countrycode as (select id, BillingCountry, iif(a.billingcountry = b.en_short_name,b.alpha_2_code,a.billingcountry) as countrycode from Account a left join countries b on a.BillingCountry = b.en_short_name)

,test as (select Account.Id as 'company-externalId',
iif(c.name = d.name,'',iif(c.name_num = 1,c.name,concat(c.name_num,' - ',c.name))) as 'company-name',
iif(BillingStreet is null or BillingStreet ='','',BillingStreet) as 'company-locationAddress',
concat(

nullif(concat(BillingStreet,', '),', '),
nullif(concat(BillingCity, ', '),', '),
nullif(concat(BillingState, ', '),', '),
nullif(concat(account.BillingCountry, ', '),', '), 
nullif(BillingPostalCode,'')

) as 'company-locationName',
case
when account.BillingCountry = 'England' then 'GB'
when account.BillingCountry = 'FR' then 'FR'
when account.BillingCountry = 'GB' then 'GB'
when account.BillingCountry = 'Kingdom of Bahrain' then 'BH'
when account.BillingCountry = 'Korea' then 'KR'
when account.BillingCountry = 'Repubic of Korea (South)' then 'KR'
when account.BillingCountry = 'Russia' then 'RU'
when account.BillingCountry = 'Taiwan' then 'TW'
when account.BillingCountry = 'The Netherlands' then 'NL'
when account.BillingCountry = 'U.K.' then 'GB'
when account.BillingCountry = 'U.S' then 'US'
when account.BillingCountry = 'UK' then 'GB'
when account.BillingCountry = 'United Kingdom' then 'GB'
when account.BillingCountry = 'United Kingdom,' then 'GB'
when account.BillingCountry = 'US' then 'US'
when account.BillingCountry = 'USA' then 'US'
else account.BillingCountry end
as 'companycountry',
iif(BillingState is null or BillingState = '','',BillingState) as 'company-locationState',
iif(BillingCity is null or BillingCity = '','',BillingCity) as 'company-locationCity',
iif(BillingPostalCode is null or BillingPostalCode = '','',BillingPostalCode) as 'company-locationZipCode',
iif(Account.Phone is null or Account.Phone = '','',iif(account.phone like '[0-9]%/%','',account.phone)) as 'company-phone',
iif(Website is null or Website ='','',Website) as 'company-website',
[User].email as 'company-owners',
iif(filename2.Name is null or filename2.Name = '','',filename2.Name) 'company-Document',
concat('External ID: ',Account.id, (char(13)+char(10)),
'Type: ',Type, (char(13)+char(10)),
nullif(concat('Description: ', (char(13)+char(10)),Account.Description),concat('Description: ', (char(13)+char(10))))) as 'company-Note',
row_number() over (partition by Account.Id order by Account.ID) as 'row_num'

from Account
left join [User] on [User].Id = Account.OwnerId
left join filename2 on Account.Id = filename2.ParentId
left join companyname c on Account.Id = c.id
left join companyposgres d on Account.Name = d.name
)

select * 
from test a 
where row_num = 1 and [company-name] <> ''