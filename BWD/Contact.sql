with test as (select peo_no, cli_no from peo_career where cli_no <> 0 and emp_to = 0 and emp_from <> 0)
,test2 as (select
a.peo_no as 'contact-externalId',
a.cli_no as 'contact-companyId',
iif(peo_forename = '','No Last Name',peo_forename) as 'contact-lastName',
iif(peo_surname = '','No First Name',peo_surname) as 'contact-firstName',
peo_known as 'preffered_name',
peo_cnt_email as 'email',
peo_email as 'personal_email',
peo_work_tel as 'contact-phone',
peo_cur_emp_job_title as 'current_job_title'
,concat(
'Address: ',
nullif(peo_cnt_establish,''), nullif(concat(', ',peo_cnt_street),', '), nullif(concat(', ',peo_cnt_district),', '), nullif(concat(', ',peo_cnt_town),', ')    
,nullif(concat(', ',peo_cnt_county),', '),nullif(concat(', ',peo_cnt_country),', '),nullif(concat(', ',peo_cnt_postcode),concat(', ',(char(13)+char(10))))
,(char(13)+char(10))
,nullif(concat('Title: ',peo_title,(char(13)+char(10))),concat('Other Phone: ',(char(13)+char(10))))
,nullif(concat('Preferred Name: ',peo_known,(char(13)+char(10))),concat('Other Phone: ',(char(13)+char(10))))
,nullif(concat('Other Phone: ',peo_cnt_other_no,(char(13)+char(10))),concat('Other Phone: ',(char(13)+char(10))))
,nullif(concat('Direct Phone: ',peo_cnt_direct_tel,(char(13)+char(10))),concat('Direct Phone: ',(char(13)+char(10))))
)as 'notes'
,ROW_NUMBER() over (partition by peo_cnt_email order by peo_cnt_email) as rn
from test a left join people b
on a.peo_no = b.peo_no)

select iif(email = '','',iif(rn=1,email,concat(rn,'-',email))) as 'contact-email',* from test2
