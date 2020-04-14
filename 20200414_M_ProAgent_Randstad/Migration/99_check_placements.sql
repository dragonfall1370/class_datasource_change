--Check placements if existing more than 1
select [キャンディデイト PANO ]
, [JOB PANO ]
, count(*)
from csv_contract
group by [キャンディデイト PANO ], [JOB PANO ]
having count(*) > 1

