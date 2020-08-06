--Update job owners for updated user mapping
select j.[PANO ] as job_ext_id
, case 
	when j.JOB担当者ユーザID = 'FPC51' then 29707 --Tetsuya.Uenuma@randstad.co.jp
	when j.JOB担当者ユーザID = 'FPC63' then 29030 --candidate.div@randstad.co.jp
	when j.JOB担当者ユーザID = 'FPC70' then 28990 --kasumi.konishi@randstad.co.jp
	end as job_owner
, current_timestamp as insert_timestamp
from csv_job j
left join UserMapping u on u.UserID = j.JOB担当者ユーザID
where j.[雇用区分] not like '%障がい者採用正社員%'
and j.[雇用区分] not like '%障がい者採用契約社員%'
and j.[雇用区分] not like '%障がい者採用紹介予定派遣%'
and j.JOB担当者ユーザID in ('FPC51', 'FPC63', 'FPC70') --35664