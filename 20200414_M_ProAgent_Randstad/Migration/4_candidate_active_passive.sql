--Inject | 転職状況 | "Active / Passive / Stage (Highest)"
/* Availability: 1-Do not call 2-Unavailable 3-Available */
select [PANO ] as cand_ext_id
--distinct 転職状況
, case when 転職状況 in ('List(リスト)', 'Entry (エントリー)', 'Contact(連絡・面談)', 'FirstInterview (初回面談)', 'Open(転職活動中)', 'Placement (決定)') then 1
	when 転職状況 in ('Close(転職活動は行なっていない)', 'Other (その他)') then 0
	else NULL end as active_passive
, case when 転職状況 in ('List(リスト)', 'Entry (エントリー)', 'Contact(連絡・面談)', 'FirstInterview (初回面談)', 'Open(転職活動中)', 'Placement (決定)') then 3
	when 転職状況 in ('Close(転職活動は行なっていない)') then 2
	when 転職状況 in ('Other (その他)') then 1
	else NULL end as highest_stage
from csv_can