with test as (select peo_no as 'candidate-externalId'
,case when peo_title = 'Mrs' then 'MRS'
when peo_title = 'Miss' then 'MISS'
when peo_title = 'Dr' then 'DR'
when peo_title = 'Sir' then 'MR'
when peo_title = 'Ms' then 'MS'
when peo_title = 'Mr' then 'MR'
else '' end as 'candidate-title'
,iif(peo_forename = '' or peo_forename is null,'No First Name',peo_forename) as 'candidate-firstName'
,iif(peo_surname = '' or peo_surname is null,'No Last Name',peo_surname) as 'candidate-Lastname'
,peo_known as 'preferred_name'
,replace(concat( nullif(concat(peo_establish, ', '),', ')
,nullif(peo_street,' ')
,nullif(concat(', ',peo_district),', ')
),', ,',', ') as 'candidate-address'
,peo_town as 'candidate-city'
,peo_county as 'candidate-State'
,iif(a.peo_country = b.en_short_name,b.alpha_2_code,
case when a.peo_country = 'United States of Ame' then 'US'
when a.peo_country = 'United Kingdom' then 'GB' 
when a.peo_country = 'USSR' then '' 
when a.peo_country = 'Abu Dhabi' then 'AE' 
when a.peo_country = 'Dubai' then 'AE'
else ''
end) as 'candidate-Country'
,peo_postcode as 'candidate-zipCode'
,peo_con as 'candidate-owners'
,peo_home_tel as 'candidate-homePhone'
,peo_work_tel as 'candidate-workPhone'
,peo_other_tel as 'candidate-phone'
,peo_email as 'email'
,peo_cur_emp_cli_name as 'candidate-company1'
,peo_cur_emp_job_title as 'candidate-jobTitle1'
,concat(
nullif(concat('Region: ',peo_region,(char(13)+char(10))),concat('Region: ',(char(13)+char(10))))
,nullif(concat('Reg Date: ',peo_reg_date,(char(13)+char(10))),concat('Reg Date: ',(char(13)+char(10))))
,nullif(concat('Status: ',peo_status,(char(13)+char(10))),concat('Status: ',(char(13)+char(10))))
,nullif(concat('Status Date: ',peo_status_date,(char(13)+char(10))),concat('Status Date: ',(char(13)+char(10))))
,nullif(concat('Link: ',peo_www,(char(13)+char(10))),concat('Link: ',(char(13)+char(10))))
,nullif(concat('Updated: ',peo_updated,(char(13)+char(10))),concat('Updated: ',(char(13)+char(10))))
,nullif(concat('CV Updated: ',peo_cv_updated,(char(13)+char(10))),concat('CV Updated: ',(char(13)+char(10))))
) as 'candidate-note'
--,peo_career.emp_job_title
,ROW_NUMBER() over (partition by peo_email order by peo_email) as rn
from people a
left join countries b on a.peo_country = b.en_short_name)

select iif(email = '' or email is null,'',iif(rn=1,email,concat(rn,'-',email))) as 'candidate-email',* from test