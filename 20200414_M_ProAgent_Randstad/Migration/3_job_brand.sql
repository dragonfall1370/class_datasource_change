--Job Brand
select j.[PANO ] as job_ext_id
, j.[JOB担当者ユーザID]
, PAUserName
, PAUserMailAddress
, case when PAUserMailAddress is not NULL then JobBrand
	else 'Professionals' end as pa_brand --added rule on 20200207
, 'BRAND' as group_type
, current_timestamp as insert_timestamp
from csv_job j
left join UserBrand u on u.PAUserID = j.[JOB担当者ユーザID]
where 1=1
--and PAUserMailAddress is not NULL --removed on 20200224
and j.[雇用区分] not like '%障がい者採用正社員%'
and j.[雇用区分] not like '%障がい者採用契約社員%'
and j.[雇用区分] not like '%障がい者採用紹介予定派遣%'
--140385 rows