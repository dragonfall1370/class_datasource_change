with 
--document1 as (select table1,code,reverse(left(REVERSE(filename), charindex('\', REVERSE(filename)) - 1)) as filename from attachment)

--,document2 as (select code,string_agg(cast(trim(filename) as varchar(max)),',') as filename

--from document1 where table1 = 'CN' group by code),

test5 as (select candcode,
concat('Employer: ',employer,(char(10)+char(13)),
'Job Title: ',jobtitle,(char(10)+char(13)),
'Start Date: ',convert(datetime, CONVERT(float,startdate)),(char(10)+char(13)),
'End Date: ',convert(datetime, CONVERT(float,enddate)),(char(10)+char(13))
) as workhistory
from candhistory where STARTDATE > 0)

,test6 as (select candcode,string_agg(workhistory,concat(',',(char(10)+char(13)))) as workhistory from test5 group by candcode)

,test as (select 
a.code as 'candidate-externalId',
iif(title in ('MR','MRS','MS','MISS','DR'),upper(replace(title,'.','')),'') as 'candidate-title',
iif(forename is null or forename = '','No First Name',forename) as 'candidate-firstname',
iif(surname = '' or surname is null,'No Last Name',surname) as 'candidate-lastname',
isnull(salutation,'') as 'preferred-name',
isnull(address,'') as 'candidate-address',
isnull(postcode,'') as 'candidate-postcode',
isnull(cast(telhome as nvarchar(max)),'') as 'candidate-homePhone',
isnull(cast(telwork as nvarchar(max)),'') as 'candidate-workPhone',
isnull(dbo.udf_GetNumeric(telother),'') as 'candidate-phone',
isnull(email,'') as 'candidate-email',
isnull(convert(datetime, CONVERT(float,replace(DOB,'-338633',''))),'') as 'candidate-dob',
isnull(gender,'') as gender,
expsalc as 'candidate-desiredSalary',

isnull(c.workhistory,'') as 'candidate-workHistory',
Concat( 'External ID: ',a.code, (char(13)+char(10)),
nullif(concat('Alert: ',alert,(char(13)+char(10))), concat('Alert: ',(char(13)+char(10)))),
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
nullif(concat('Comment: ',(char(13)+char(10)),comments,(char(13)+char(10))),concat('Comment: ',(char(13)+char(10)),(char(13)+char(10))))
)as 'candidate-note',

ROW_NUMBER() over (partition by email order by email) as rn

,d.s37 as 'candidate-owners'


from candidate a
--left join document2 b on a.code = b.code
left join test6 c on a.code = c.candcode
left join secure d on a.OWNINGCONS = d.INITIALS)

select iif([candidate-email] is null or [candidate-email] ='','',iif(rn=1,[candidate-email],concat(rn,'-',[candidate-email]))) as email,a.*,iif(b.S37 is null,'',b.s37) as true_owner from test a 
left join secure b on trim(a.[candidate-owners]) = b.initials
