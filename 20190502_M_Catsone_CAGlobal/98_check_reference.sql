--COMPARE from CAND combined all
select concat('CG',candidates_id) as CandidateExtID
, 'add_cand_info' as Additional_type
, cf_value as Custom_value --ID Number
, 1005 as Form_id
, 1020 as Field_id
, getdate() as Insert_timestamp
from candidates_custom_fields_value
where cf_value <> ''
and cf_id = 153638
order by candidates_id --443200

--COMPARE with separate value (single)
select concat('CG', id) as CandidateExtID
, 'add_cand_info' as Additional_type
, cf_value as Custom_value --Notice period
, 1005 as Form_id
, 1020 as Field_id
, getdate() as Insert_timestamp
from candidates_custom_fields_153638_value
where cf_value is not NULL
and cf_id = 153638
order by id --3353 rows


--COMPARE from CAND combined all (drop down)
select concat('CG',candidates_id) as CandidateExtID
, 'add_cand_info' as Additional_type
, c1.label as Custom_value --ID Number
, 1005 as Form_id
, 1016 as Field_id
, getdate() as Insert_timestamp
from candidates_custom_fields_value cv
left join candidates_custom_fields_153623 c1 on c1.id = cv.cf_value
where cf_value <> ''
and cf_id = 153623
order by candidates_id --231899

--COMPARE with separate value (drop down)
select concat('CG', cv.id) as CandidateExtID
, 'add_cand_info' as Additional_type
, trim(c1.label) as Custom_value --Placement status
, 1005 as Form_id
, 1019 as Field_id
, getdate() as Insert_timestamp
from candidates_custom_fields_153623_value cv
left join candidates_custom_fields_153623 c1 on c1.id = cv.cf_value
where cf_value is not NULL
order by cv.id --231899 rows