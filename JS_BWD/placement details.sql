
select 
job_no as 'application-positionExternalId',
peo_no as 'application-candidateExternalId',
iif(action_date = 0,'',
cast(replace(replace(action_date,left(action_date,4),concat(left(action_date,4),'-')),left(replace(action_date,left(action_date,4),concat(left(action_date,4),'-')),7),
concat(left(replace(action_date,left(action_date,4),concat(left(action_date,4),'-')),7),'-')) as datetime)) as 'position-startDate'
, 1 as use_quick_fee_forecast
, 1 as invoice_valid
, 1 as invoice_renewal_index
, 1 as invoice_renewal_flow_status
, 2 as invoice_status
, 3 as offer_draft_offer
, 1 as offer_valid
, 1 as offer_position_type
,concat('Action Con: ',action_con,(char(13)+char(10)),
nullif(concat('Start Date: ',
replace(replace(start_date,left(start_date,4),concat(left(start_date,4),'-')),left(replace(start_date,left(start_date,4),concat(left(start_date,4),'-')),7),
concat(left(replace(start_date,left(start_date,4),concat(left(start_date,4),'-')),7),'-')),char(13)+char(10)),concat('Start Date: ',(char(13)+char(10))))
,(char(13)+char(10))
,nullif(concat('Salary: ',salary,(char(13)+char(10))),concat('Salary: ',(char(13)+char(10))))
,nullif(concat('Job Fee Percentage: ',job_fee_percentage,(char(13)+char(10))),concat('Job Fee Percentage: ',(char(13)+char(10))))
,nullif(concat('Job Fee: ',job_fee,(char(13)+char(10))),concat('Job Fee: ',(char(13)+char(10))))

) as note
from placed


