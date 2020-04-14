select [氏名], len([氏名])
, [PANO ]
, case when len([氏名]) <= 2 then trim(left([氏名], 1))
	when charindex(' ', [氏名]) > 0 then trim(left([氏名], charindex(' ', [氏名]) - 1)) 
	else trim(left([氏名], 2)) end as first_name
, case when len([氏名]) <= 2 then trim(right([氏名], 1))
	when charindex(' ', trim([氏名])) > 0 then trim(right([氏名], len([氏名]) - charindex(' ', [氏名])))
	else trim(right([氏名], len([氏名]) - len(left([氏名], 2)))) end as last_name
, [フリガナ]
, case when len([フリガナ]) <= 2 then trim(left([フリガナ], 1))
	when charindex(' ', [フリガナ]) > 0 then trim(left([フリガナ], charindex(' ', [フリガナ]) - 1)) 
	else trim(left([フリガナ], 2)) end as first_name_kana
, case when len([フリガナ]) <= 2 then trim(right([フリガナ], 1))
	when charindex(' ', trim([フリガナ])) > 0 then trim(right([フリガナ], len([フリガナ]) - charindex(' ', [フリガナ])))
	else trim(right([フリガナ], len([フリガナ]) - len(left([フリガナ], 2)))) end as last_name_kana
from csv_can
--where len([氏名]) = 40
order by [PANO ]

select top 7000 *
from csv_can
order by [PANO ]

select distinct len([氏名])
from csv_can
--笹岡 典史

select [採用担当者], len(採用担当者)
from csv_rec