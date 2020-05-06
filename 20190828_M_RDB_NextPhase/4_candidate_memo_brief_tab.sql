--Candidate new brief tab
select [PANO ] as cand_ext_id
, 'Memo' as title
, trim(メモ) as memo
, current_timestamp as insert_timestamp
from csv_can --max char 10946