--Company Brand
select c.[PANO ] as com_ext_id
, c.[企業担当ユーザID]
, PAUserName
, PAUserMailAddress
, case when PAUserMailAddress is not NULL then CompanyBrand
	else 'Professionals' end as pa_brand --added rule on 20200207
, 'BRAND' as group_type
, current_timestamp as insert_timestamp
from csv_recf c
left join UserBrand u on u.PAUserID = c.[企業担当ユーザID]