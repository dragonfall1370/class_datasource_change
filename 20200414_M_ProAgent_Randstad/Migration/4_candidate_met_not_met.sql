--#Inject Met/Not Met
with met_notmet as (select distinct [キャンディデイト PANO ] as cand_ext_id
	, [面談実施実施日] --面談実施実施日
	, case when nullif(面談実施実施日, '') is not NULL then 1 --Met
		else 2 end as met_notmet --Not met
	, row_number() over(partition by [キャンディデイト PANO ] --updated 20200219
						order by case when 面談実施実施日 is NULL or 面談実施実施日 = '' then 1 else 2 end desc) as rn
	from csv_can_history
)

/* Audit if having more than 1
select [キャンディデイト PANO ], count(*)
from csv_can_history
group by [キャンディデイト PANO ]
having count(*) > 1
*/

select * 
from met_notmet
where rn = 1