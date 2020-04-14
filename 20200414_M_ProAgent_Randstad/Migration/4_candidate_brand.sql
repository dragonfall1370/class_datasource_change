--Candidate Brand
select c.[PANO ] as cand_ext_id
, c.[人材担当ユーザID]
, PAUserName
, PAUserMailAddress
, case when PAUserMailAddress is not NULL then CandidateBrand
	else 'CA' end as pa_brand --added rule on 20200207
, 'BRAND' as group_type
, current_timestamp as insert_timestamp
from csv_can c
left join UserBrand u on u.PAUserID = c.[人材担当ユーザID]
where 1=1
--and PAUserMailAddress is not NULL --removed on 20200224
and c.[チェック項目] not like '%チャレンジド人材%'