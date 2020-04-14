--JOB MIGRATION PRIORITY
select [PANO ]
, convert(date, [更新日], 120)
, dateadd(month, -6, convert(date, [更新日], 120)) 
, [募集状況]
, [HP公開]
, [HP公開承認フラグ]
, [雇用区分]
from csv_job
where convert(date, [更新日], 120) >= dateadd(month, -6, getdate()) --28977
or ([募集状況] in ('Open', 'Other') and [HP公開] = '公開' and [HP公開承認フラグ] = '承認' and ([雇用区分] like '%正社員%' or [雇用区分] like '%契約社員%'))