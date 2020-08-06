--JOB打診依頼 | JOB consultation request
with jobapp as (select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, 'Shortlisted - Pending' as app_status
, [JOB打診依頼実施日] as action_date
, NULL as rejected_date
, 'JOB打診依頼' as sub_status
, 1 as order_id
, 'JOB打診依頼' as pa_ja_status
, 'JOB consultation request' as pa_en_status
, 'JOB打診依頼実施日' as csv_field
from csv_status where [JOB打診依頼実施日] <> ''

UNION ALL
--候補案件 | Candidate job
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, 'Shortlisted - Pending' as app_status, [候補案件実施日], NULL as rejected_date
, '候補案件' as sub_status
, 3 as order_id
, '候補案件' as pa_ja_status
, 'Candidate job' as pa_en_status
, '候補案件実施日' as csv_field
from csv_status where [候補案件実施日] <> ''

UNION ALL
--JOB打診 | JOB consultation
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, 'Shortlisted - Pending' as app_status, [JOB打診実施日], NULL as rejected_date
, 'JOB打診' as sub_status
, 4 as order_id
, 'JOB打診' as pa_ja_status
, 'JOB consultation' as pa_en_status
, 'JOB打診実施日' as csv_field
from csv_status where [JOB打診実施日] <> ''

UNION ALL
--JOB打診NG | JOB consultation NG
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, 'Shortlisted - Rejected' as app_status, [JOB打診NG実施日], [JOB打診NG実施日] as rejected_date
, 'Rejected - Candidate' as sub_status
, 5 as order_id
, 'JOB打診NG' as pa_ja_status
, 'JOB consultation NG' as pa_en_status
, 'JOB打診NG実施日' as csv_field
from csv_status
where [JOB打診NG実施日] <> ''

UNION ALL
--応募準備中 | Preparing for application
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, 'Shortlisted - Pending' as app_status, [応募準備中実施日], NULL as rejected_date
, 'Pitched - Waiting' as sub_status
, 6 as order_id
, '応募準備中' as pa_ja_status
, 'Preparing for application' as pa_en_status
, '応募準備中実施日' as csv_field
from csv_status where [応募準備中実施日] <> ''

UNION ALL
--応募承諾 | Application acceptance
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, 'Shortlisted - Pending' as app_status, [応募承諾実施日], NULL as rejected_date
, '応募承諾' as sub_status
, 7 as order_id
, '応募承諾' as pa_ja_status
, 'Application acceptance' as pa_en_status
, '応募承諾実施日' as csv_field
from csv_status where [応募承諾実施日] <> ''

UNION ALL
--推薦NG | Recommendation NG
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, 'Shortlisted - Rejected' as app_status, [推薦NG実施日], [推薦NG実施日] as rejected_date
, 'Rejected - Consultant' as sub_status
, 9 as order_id
, '推薦NG' as pa_ja_status
, 'Recommendation NG' as pa_en_status
, '推薦NG実施日' as csv_field
from csv_status where [推薦NG実施日] <> ''

UNION ALL
--応募辞退 | Decline application
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, 'Shortlisted - Rejected' as app_status, [応募辞退実施日], [応募辞退実施日] as rejected_date
, 'Rejected - Candidate' as sub_status
, 10 as order_id
, '応募辞退' as pa_ja_status
, 'Decline application' as pa_en_status
, '応募辞退実施日' as csv_field
from csv_status where [応募辞退実施日] <> ''

UNION ALL
--書類推薦 | Document recommendation
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, 'Sent - Pending' as app_status, [書類推薦実施日], NULL as rejected_date
, NULL as sub_status
, 11 as order_id
, '書類推薦' as pa_ja_status
, 'Document recommendation' as pa_en_status
, '書類推薦実施日' as csv_field
from csv_status where [書類推薦実施日] <> ''

UNION ALL
--筆記試験 | Written exam
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, 'Sent - Pending' as app_status, [筆記試験実施日], NULL as rejected_date
, '筆記試験' as sub_status
, 12 as order_id
, '筆記試験' as pa_ja_status
, 'Written exam' as pa_en_status
, '筆記試験実施日' as csv_field
from csv_status where [筆記試験実施日] <> ''

UNION ALL
--筆記試験NG | Written exam NG
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, 'Sent - Rejected' as app_status, [筆記試験ＮＧ実施日], [筆記試験ＮＧ実施日] as rejected_date
, 'Rejected - Client' as sub_status
, 13 as order_id
, '筆記試験NG' as pa_ja_status
, 'Written exam NG' as pa_en_status
, '筆記試験ＮＧ実施日' as csv_field
from csv_status where [筆記試験ＮＧ実施日] <> ''

UNION ALL
--書類OK | Document OK
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, 'Sent - Pending' as app_status, [書類OK実施日], NULL as rejected_date
, '書類OK' as sub_status
, 14 as order_id
, '書類OK' as pa_ja_status
, 'Document OK' as pa_en_status
, '書類OK実施日' as csv_field
from csv_status where [書類OK実施日] <> ''

UNION ALL
--書類NG | Document NG
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, 'Sent - Rejected' as app_status, [書類NG実施日], [書類NG実施日] as rejected_date
, 'Rejected - Client' as sub_status
, 15 as order_id
, '書類NG' as pa_ja_status
, 'Document NG' as pa_en_status
, '書類NG実施日' as csv_field
from csv_status where [書類NG実施日] <> ''

UNION ALL
--面接（一次）| Interview (primary)
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, '1st Interview - Pending' as app_status, [面接（一次）実施日], NULL as rejected_date
, NULL as sub_status
, 16 as order_id
, '面接（一次）' as pa_ja_status
, 'Interview (primary)' as pa_en_status
, '面接（一次）実施日' as csv_field
from csv_status where [面接（一次）実施日] <> ''

UNION ALL
--面接（一次）OK | Interview (primary) OK
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, '1st Interview - Pending' as app_status, [面接（一次）OK実施日], NULL as rejected_date
, '面接OK' as sub_status
, 17 as order_id
, '面接（一次）OK' as pa_ja_status
, 'Interview (primary) OK' as pa_en_status
, '面接（一次）OK実施日' as csv_field
from csv_status where [面接（一次）OK実施日] <> ''

UNION ALL
--面接（二次）| Interview (secondary)
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, '2nd+ Interview - Pending' as app_status, [面接（二次）実施日], NULL as rejected_date
, NULL as sub_status
, 18 as order_id
, '面接（二次）' as pa_ja_status
, 'Interview (secondary)' as pa_en_status
, '面接（二次）実施日' as csv_field
from csv_status where [面接（二次）実施日] <> ''

UNION ALL
--面接（二次）OK | Interview (secondary) OK
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, '2nd+ Interview - Pending' as app_status, [面接（二次）OK実施日], NULL as rejected_date
, '面接OK' as sub_status
, 19 as order_id
, '面接（二次）OK' as pa_ja_status
, 'Interview (secondary) OK' as pa_en_status
, '面接（二次）OK実施日' as csv_field
from csv_status where [面接（二次）OK実施日] <> ''

UNION ALL
--面接（三次以降） | Interview (3rd and later)
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, '2nd+ Interview - Pending' as app_status, [面接（三次以降）実施日], NULL as rejected_date
, NULL as sub_status
, 20 as order_id
, '面接（三次以降）' as pa_ja_status
, 'Interview (3rd and later)' as pa_en_status
, '面接（三次以降）実施日' as csv_field
from csv_status where [面接（三次以降）実施日] <> ''

UNION ALL
--面接（三次以降）OK | Interview (3rd and later) OK
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, '2nd+ Interview - Pending' as app_status, [面接（三次以降）OK実施日], NULL as rejected_date
, '面接OK' as sub_status
, 21 as order_id
, '面接（三次以降）OK' as pa_ja_status
, 'Interview (3rd and later) OK' as pa_en_status
, '面接（三次以降）OK実施日' as csv_field
from csv_status where [面接（三次以降）OK実施日] <> ''

UNION ALL
--面接（最終）| Interview (final)
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, '2nd+ Interview - Pending' as app_status, [面接（最終）実施日], NULL as rejected_date
, NULL as sub_status
, 22 as order_id
, '面接（最終）' as pa_ja_status
, 'Interview (final)' as pa_en_status
, '面接（最終）実施日' as csv_field
from csv_status where [面接（最終）実施日] <> ''

UNION ALL
--面接NG | Interview NG
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, case when coalesce(nullif([面接（二次）実施日],'')
					, nullif([面接（二次）OK実施日], '')
					, nullif([面接（三次以降）実施日], '')
					, nullif([面接（三次以降）OK実施日], '')
					, nullif([面接（最終）実施日], ''), NULL) is NULL then '1st Interview - Rejected'
	else '2nd+ Interview - Rejected' end as app_status
, [面接NG実施日]
, [面接NG実施日] as rejected_date
, NULL as sub_status
, 23 as order_id
, '面接NG' as pa_ja_status
, 'Interview NG' as pa_en_status
, '面接NG実施日' as csv_field
/* --Audit rules
, [面接（二次）実施日]
, [面接（二次）OK実施日]
, [面接（三次以降）実施日]
, [面接（三次以降）OK実施日]
, [面接（最終）実施日] */
from csv_status where [面接NG実施日] <> ''

UNION ALL
--内定 | Offer
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, 'Offered - received' as app_status, [内定実施日], NULL as rejected_date
, NULL as sub_status
, 24 as order_id
, '内定' as pa_ja_status
, 'Offer' as pa_en_status
, '内定実施日' as csv_field
from csv_status where [内定実施日] <> ''

UNION ALL
--入社承諾 | Acceptance to join
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, 'Placed - Starting' as app_status, [入社承諾実施日], NULL as rejected_date
, NULL as sub_status
, 25 as order_id
, '入社承諾' as pa_ja_status
, 'Acceptance to join' as pa_en_status
, '入社承諾実施日' as csv_field
from csv_status where [入社承諾実施日] <> ''

UNION ALL
--入社予定 | Plan to join
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, 'Placed - Starting' as app_status, [入社予定実施日], NULL as rejected_date
, NULL as sub_status
, 26 as order_id
, '入社予定' as pa_ja_status
, 'Plan to join' as pa_en_status
, '入社予定実施日' as csv_field
from csv_status where [入社予定実施日] <> ''

UNION ALL
--入社 | Join
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, 'Placed - Active' as app_status, [入社実施日], NULL as rejected_date
, NULL as sub_status
, 27 as order_id
, '入社' as pa_ja_status
, 'Join' as pa_en_status
, '入社実施日' as csv_field
from csv_status where [入社実施日] <> ''

UNION ALL
--本人辞退 | Decline
select [キャンディデイト PANO ] as cand_ext_id
, [JOB PANO ] as job_ext_id
, 'Shortlisted - Rejected' as app_status, [本人辞退実施日], [本人辞退実施日] as rejected_date --corrected on 07/07/2020
, 'Rejected - Candidate' as sub_status
, 28 as order_id
, '本人辞退' as pa_ja_status
, 'Decline' as pa_en_status
, '本人辞退実施日' as csv_field
from csv_status where [本人辞退実施日] <> '')

--NEW CONDITIONS ADDED FROM 02/2020
, cand_filter as (select [PANO ] as cand_ext_id
	from csv_can
	where [チェック項目] not like '%チャレンジド人材%')
	
, job_filter as (select [PANO ] as job_ext_id
	from csv_job
	where [雇用区分] not like '%障がい者採用正社員%'
	and [雇用区分] not like '%障がい者採用契約社員%'
	and [雇用区分] not like '%障がい者採用紹介予定派遣%')

--MAIN SCRIPT
select cand_ext_id
, job_ext_id
, app_status
, convert(date, action_date, 120) as action_date
, convert(date, rejected_date, 120) as rejected_date
, sub_status
, order_id
, pa_ja_status
, pa_en_status
, csv_field
--into pa_final_jobapp --temp table for job app
from jobapp -- rows
where 1=1
and cand_ext_id in (select cand_ext_id from cand_filter)
and job_ext_id in (select job_ext_id from job_filter)
--and cand_ext_id = 'CDT001017' and job_ext_id = 'JOB000720'