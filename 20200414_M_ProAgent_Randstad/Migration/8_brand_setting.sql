--Company Brand
select c.[PANO ] as com_ext_id
, c.[企業担当ユーザID]
, PAUserName
, PAUserMailAddress
, CompanyBrand
, current_timestamp as insert_timestamp
from csv_recf c
left join UserBrand u on u.PAUserID = c.[企業担当ユーザID]
where PAUserMailAddress is not NULL


--Contact Brand
insert into team_group_contact (contact_id, team_group_id, insert_timestamp)
(select id as contact_id
, 9999 as team_group_id
, current_timestamp as insert_timestamp
from contact
where external_id like 'REC-%'
and deleted_timestamp is NULL

UNION ALL
select id as contact_id
, 9999 as team_group_id
, current_timestamp as insert_timestamp
from contact
where external_id like 'REC-%'
and deleted_timestamp is NULL

UNION ALL
select id as contact_id
, 9999 as team_group_id
, current_timestamp as insert_timestamp
from contact
where external_id like 'REC-%'
and deleted_timestamp is NULL

UNION ALL
select id as contact_id
, 9999 as team_group_id
, current_timestamp as insert_timestamp
from contact
where external_id like 'REC-%'
and deleted_timestamp is NULL)


--Candidate Brand
select c.[PANO ] as cand_ext_id
, c.[人材担当ユーザID]
, PAUserName
, PAUserMailAddress
, CandidateBrand
, current_timestamp as insert_timestamp
from csv_can c
left join UserBrand u on u.PAUserID = c.[人材担当ユーザID]
where 1=1
and PAUserMailAddress is not NULL
and c.[チェック項目] not like '%チャレンジド人材%'


--Job Brand
select j.[PANO ] as job_ext_id
, j.[JOB担当者ユーザID]
, PAUserName
, PAUserMailAddress
, JobBrand
, current_timestamp as insert_timestamp
from csv_job j
left join UserBrand u on u.PAUserID = j.[JOB担当者ユーザID]
where 1=1
and PAUserMailAddress is not NULL
and j.[雇用区分] not like '%障がい者採用正社員%'
and j.[雇用区分] not like '%障がい者採用契約社員%'
and j.[雇用区分] not like '%障がい者採用紹介予定派遣%'
--140385 rows