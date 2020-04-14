--Company job not found in company
select *
from csv_recf
where [PANO ] in ('CPY008238','CPY018374','CPY018793','CPY018925','CPY019144','CPY019195','CPY019280','CPY019886','CPY020458','CPY019666') 


--Job with company not found
select *
from csv_job
where [企業 PANO ] not in (select [PANO ] from csv_recf) --29 rows


--Total 6 records
select distinct [企業 PANO ], [採用担当者ID]
		, concat('DEF', [企業 PANO ]) as default_contactID
		from csv_job
		where concat('REC-',採用担当者ID) not in (select 採用担当者ID from csv_rec)
		
--Contact not found in company > contact: 'REC-250897', 'REC-250825', 'REC-91406', 'REC-105368', 'REC-78654', 'REC-252424'

-->> Check job - contact - company rules <<--
--Contact not existing in Company, Default contact will be mapped
with jobcontact as (select distinct [企業 PANO ], [採用担当者ID]
		, concat('DEF', [企業 PANO ]) as default_contactID
		--, [PANO ]
		from csv_job
		where concat('REC-',採用担当者ID) not in (select 採用担当者ID from csv_rec))

, dup as (select [PANO ], [ポジション名]
		, row_number() over(partition by trim(lower([ポジション名])) order by [PANO ] desc) as rn
		from csv_job)

--MAIN SCRIPT contact-job
select j.[PANO ] as [position-externalId]
, j.[企業 PANO ] as original_com
, j.[採用担当者ID] as original_contact
, case when j.[企業 PANO ] not in (select [PANO ] from csv_recf) then 'REC-999999999' --default contact
	when jc.採用担当者ID is not NULL then default_contactID --default contact in each company
	else concat('REC-', j.[採用担当者ID]) end as [position-contactId]
from csv_job j
left join csv_Job_Situation js on j.[PANO ] = js.JobPANo
left join dup on dup.[PANO ] = j.[PANO ]
left join jobcontact jc on jc.[企業 PANO ] = j.[企業 PANO ]
where j.[PANO ] = 'JOB005643'

--Candidate Status
select [PANO] as cand
, string_agg(concat_ws(char(10)
	, coalesce('[JOBNO#] ' + nullif([PANO2], ''), NULL)
	, coalesce('[Open (紹介中)] ' + nullif([Open (紹介中)], ''), NULL)
	, coalesce('[Placement (決定)] ' + nullif([Placement (決定)], ''), NULL)
	, coalesce('[Close (紹介終了)] ' + nullif([Close (紹介終了)], ''), NULL)
	, coalesce('[Other (その他)] ' + nullif([Other (その他)], ''), NULL)
	), concat(char(10),char(13))) 
	within group (order by coalesce([Open (紹介中)], [Placement (決定)], [Close (紹介終了)], [Other (その他)]) desc) as status_my_can
from csv_status_my_can
where [PANO] is not NULL
and coalesce(nullif([PANO2],''), nullif([Open (紹介中)],''), nullif([Placement (決定)],''), nullif([Close (紹介終了)],''), nullif([Other (その他)],'')) is not NULL
group by [PANO]

--
select distinct [転職状況]
from csv_can

--
select distinct [状況メモ] --status note
from csv_status_my_can

--
select [PANO], count(*)
from csv_status_my_can
group by [PANO]
having count(*) > 1 --64226 rows

--Candidate source
select distinct [登録経路]
from csv_can --45 candidate sources | mapping required

--
select top 1000 *
from csv_status_my_can

--Check item
select distinct [PANO ]
, value as check_item
from csv_can
cross apply string_split([チェック項目], char(10))
where coalesce(nullif([チェック項目],''), NULL) is not NULL
and value like '%AG人材%' --only take AG Personnel

--status note
select [PANO] as cand
, string_agg(concat_ws(char(10)
	, coalesce('[JOBNO#] ' + nullif([PANO2], ''), NULL)
	, coalesce('[状況メモ] ' + char(10) + nullif([状況メモ], ''), NULL)
	), concat(char(10),char(13)))
	within group (order by [PANO2] desc) as status_note
from csv_status_my_can
where [PANO] is not NULL
and coalesce(nullif([状況メモ],''), NULL) is not NULL
group by [PANO]

--Cognitive pathway
select distinct [PANO ] as cand_ext_id
, [認知経路]
from csv_can --64 values
where [認知経路] <> ''

--candidate picture
select can_id, count(*)
from CAN_picture
group by can_id
having count(*) > 1

select can_id
, [file]
, right([file], len([file]) - len('can/picture/')) as photo
from CAN_picture

--gender
select distinct 性別
from csv_can --2 values