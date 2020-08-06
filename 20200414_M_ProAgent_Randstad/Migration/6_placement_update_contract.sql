--Update Contract placement in VC
update offer
set country_code = 'JP'
, charge_rate_type = 'markup'
, profit = total_pay_rate
, contract_rate_type = 1
, contract_length = 0
, contract_length_type = 1
, margin_percent = 50
, markup_percent = 100
where insert_timestamp > '2019-11-20'
and position_type = 2

--Update Temp to Perm placement in VC
update offer
set country_code = 'JP'
, charge_rate_type = 'markup'
, profit = total_pay_rate
, contract_rate_type = 1
, contract_length = 0
, contract_length_type = 1
, margin_percent = 50
, markup_percent = 100
where insert_timestamp > '2019-11-20'
and position_type = 3
and use_quick_fee_forecast = 0


--Update Contract offer_personal_info
update offer_personal_info o
set client_contact_name = concat_ws(' ', c.first_name, c.last_name)
from contact c
where 1=1
and o.client_contact_id = c.id
and o.insert_timestamp > '2019-11-30'