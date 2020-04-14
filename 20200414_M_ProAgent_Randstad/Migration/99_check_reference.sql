--COMPANY
select [PANO ]
, [会社名]
, [フリガナ]
, *
from csv_recf
where 1=1 --30109 rows
and [PANO ] = 'CPY023020'
--and [PANO ] = 'CPY003867' --Company ID
--and [企業NO ] = 'CPY003867'
--and [企業担当] = '足利支店'
--and 企業担当ユーザID = 'FPC205'

select *
from csv_recf_history --30109

--BILLING COMPANY ADDRESS
select *
from csv_recf_claim --30109 rows

--CONTACT
select [企業 PANO ]
, [企業 企業名]
, *
from csv_rec
where 1=1 --35669 rows
and [企業 PANO ] = 'CPY023020'
--and [企業 PANO ] = 'CPY003867'
--and [企業 企業名] = 'ＲＣＡ'
--and [企業 PANO ] = 'CPY003867'

--JOB
select 採用担当者ID as ContactID
, [企業 PANO ] as CompanyID
, [PANO ] as JobID
, *
from csv_job --132528 rows
where [企業 PANO ] = 'CPY023020'

--CANDIDATE
select [キャンディデイト PANO ], count(*)
from csv_can_history
group by [キャンディデイト PANO ]
having count(*) > 1 --none

select *
from csv_can --162981

--APPLICATION
select *
from csv_status --783802 rows

select top 100 *
from csv_status_my_can --606800

select *
from CAN_resume

select *
from csv_contract