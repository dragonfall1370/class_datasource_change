--Compensation: Current / Desired Salary
select [PANO ] as cand_ext_id
, convert(float, coalesce(nullif(現在の年収,''),'0')) * 10000 as current_salary
, convert(float, coalesce(nullif(希望の年収,''),'0')) * 10000 as desired_salary
from csv_can
where coalesce(nullif(現在の年収, ''), nullif(希望の年収, '')) is not NULL
order by [PANO ]

--Inject | Annual income memo | Other Benefits
select [PANO ]
, 年収メモ as other_benefit
from csv_can
where coalesce(nullif(年収メモ, ''), NULL) is not NULL
order by [PANO ]