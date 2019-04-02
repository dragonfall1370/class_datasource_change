with 
document1 as (select table1,code,reverse(left(REVERSE(filename), charindex('\', REVERSE(filename)) - 1)) as filename from attachment)

,document2 as (select code,string_agg(cast(trim(filename) as varchar(max)),',') as filename

from document1 where table1 = 'CN' group by code)
,test as (select 
a.code as 'candidate-externalId',
iif(title in ('MR','MRS','MS','MISS','DR'),upper(replace(title,'.','')),'') as 'candidate-title',
iif(forename is null or forename = '','No First Name',forename) as 'candidate-firstname',
iif(surname = '' or surname is null,'No Last Name',surname) as 'candidate-lastname',
middlename as 'candidate-middlename',
salutation as 'preferred-name',
address as 'candidate-address',
postcode as 'candidate-postcode',
cast(telhome as nvarchar(max)) as 'candidate-homePhone',
cast(telwork as nvarchar(max)) as 'candidate-workPhone',
dbo.udf_GetNumeric(telother) as 'candidate-phone',
email as 'candidate-email',
dob as 'candidate-dob',
url as 'candidate-linkedin',
iif(b.filename is null or b.filename ='','',b.filename) as 'candidate-document',

Concat( 'External ID: ',a.code,
nullif(concat('Salutation: ',salutation,(char(13)+char(10))),concat('Salutation: ',(char(13)+char(10)))),
'Status: ',
case when status = 1 then 'Working for us'
when status = 2 then 'Interview Pending'
when status = 3 then 'Interested Position'
when status = 4 then 'Active Looking'
when status = 5 then 'Considering Opportunities'
when status = 6 then 'On Contract'
when status = 7 then 'Found Own Job'
when status = 8 then 'Not Looking'
when status = 9 then 'Other'
when status = 10 then 'Placed by us'
end,
(char(13)+char(10)),
nullif(concat('Employer: ',lastempl,(char(13)+char(10))),concat('Employer: ',(char(13)+char(10)))),
nullif(concat('Job: ',lastpost,(char(13)+char(10))), concat('Job: ',(char(13)+char(10)))),
nullif(concat('Salary: ',lastsal,(char(13)+char(10))),concat('Salary: ',(char(13)+char(10))))

)as 'candidate-note',

ROW_NUMBER() over (partition by email order by email) as rn

--owningcons as 'candidate-owners'


from candidate a
left join document2 b on a.code = b.code)

select iif([candidate-email] is null or [candidate-email] ='','',iif(rn=1,[candidate-email],concat(rn,'-',[candidate-email]))) as email,* from test
