--Company payments
/*BACKUP COMPANIES
select *
into company_bkup_20200301
from company
where deleted_timestamp is NULL--53908 rows
*/
with merged_payment_terms as (select m.vc_company_id
	, m.vc_pa_company_id
	, m.com_ext_id
	, m.rn
	, c.billing_group_name as pa_billing_group_name
	, c.company_number as pa_company_number
	, concat_ws(chr(10), ('【Merged from PA: ' || m.com_ext_id || '】') , nullif(c.company_payment_term, '')) as pa_company_payment_term
	, c.trading_name as pa_trading_name
	--, nullif(c2.billing_group_name, '') billing_group_name
	--, nullif(c2.company_number, '') company_number
	--, nullif(c2.company_payment_term, '') company_payment_term
	--, nullif(c2.trading_name, '') trading_name
	--, concat_ws(chr(10), ('【Merged from PA: ' || m.com_ext_id || '】') , c.note) as merged_notes
	from mike_tmp_company_dup_check m
	join company c on c.id = m.vc_pa_company_id
	--join company c2 on c2.id = m.vc_company_id
	where 1=1
	--and nullif(billing_group_name, '') is not NULL
	--and nullif(company_number, '') is not NULL --20 rows
	--and nullif(company_payment_term, '') is not NULL
	--and nullif(trading_name, '') is not NULL
	and coalesce(nullif(billing_group_name, ''), nullif(company_number, ''), nullif(company_payment_term, ''), nullif(trading_name, '')) is not NULL
	and rn = 1
	)
/* DOUBLE CHECK EXISTING COMPANY DATA
select id, billing_group_name, company_number, trading_name
from company
where id in (select vc_company_id from mike_tmp_company_dup_check) --4650 rows
and coalesce(nullif(billing_group_name, ''), nullif(company_number, ''), nullif(company_payment_term, ''), nullif(trading_name, '')) is not NULL --17 rows
*/
/* AUDIT LENGTH
select vc_company_id, vc_pa_company_id, com_ext_id, length(pa_company_payment_term)
from merged_payment_terms where length(pa_company_payment_term) >= 200 --length more than 400 - company_id = 59572 (from PAID CPY021724)
*/
--> UPDATE LATEST PA COMPANY (rn = 1)
update company c
set billing_group_name = pa_billing_group_name
, company_number = pa_company_number
, trading_name = pa_trading_name
, company_payment_term = left(pa_company_payment_term, 400)
from merged_payment_terms m
where m.vc_company_id = c.id


-->> UPDATE OLDER PA COMPANY (rn > 1)
with merged_payment_terms_2 as (select m.vc_company_id
	, m.vc_pa_company_id
	, m.com_ext_id
	, m.rn
	, c.billing_group_name as pa_billing_group_name
	, c.company_number as pa_company_number
	, c.trading_name as pa_trading_name
	, c.company_payment_term as pa_company_payment_term
	, concat_ws(chr(10), ('【Merged from PA: ' || m.com_ext_id || '】')
		, coalesce('[請求先 部署名]' || nullif(billing_group_name, ''), NULL)
		, coalesce('[請求先 担当者名]' || nullif(trading_name, ''), NULL) --contact_name
		, coalesce('[請求先 TEL]' || nullif(company_number, ''), NULL) --company_business_no
		, nullif(company_payment_term, '')
	) as new_pa_company_payment_term	
	from mike_tmp_company_dup_check m
	join company c on c.id = m.vc_pa_company_id
	--join company c2 on c2.id = m.vc_company_id
	where 1=1
	and coalesce(nullif(billing_group_name, ''), nullif(company_number, ''), nullif(company_payment_term, ''), nullif(trading_name, '')) is not NULL
	and rn > 1
	)

update company c
set company_payment_term = concat_ws(chr(10) || chr(13), c.company_payment_term, m.new_pa_company_payment_term)
from merged_payment_terms_2 m
where m.vc_company_id = c.id


-->>UPDATE CASES WITH OLDER COMPANY HAVING INFO
with merged_payment as (select m.vc_company_id
	, m.vc_pa_company_id
	, m.com_ext_id
	, m.rn
	, c.billing_group_name as pa_billing_group_name
	, c.company_number as pa_company_number
	, c.trading_name as pa_trading_name
	, company_payment_term
	from mike_tmp_company_dup_check m
	join company c on c.id = m.vc_pa_company_id
	where 1=1
	and vc_company_id in (select vc_company_id from mike_tmp_company_dup_check where rn > 1)
	and coalesce(nullif(billing_group_name, ''), nullif(company_number, ''), nullif(company_payment_term, ''), nullif(trading_name, '')) is not NULL
	) --21 rows


update company c
set billing_group_name = case when nullif(c.billing_group_name, '') is NULL then m.pa_billing_group_name else c.billing_group_name end
, company_number = case when nullif(c.company_number, '') is NULL then m.pa_company_number else c.company_number end
, trading_name = case when nullif(c.trading_name, '') is NULL then m.pa_trading_name else c.trading_name end
from merged_payment m
where m.vc_company_id = c.id
and m.rn > 1

/* AUDIT CASE WITH MISSING INFO
select *
from mike_tmp_company_dup_check
where vc_company_id = 31074

select m.vc_company_id
	, m.vc_pa_company_id
	, m.com_ext_id
	, m.rn
	, c.billing_group_name as pa_billing_group_name
	, c.company_number as pa_company_number
	, c.trading_name as pa_trading_name
	, company_payment_term
	from mike_tmp_company_dup_check m
	join company c on c.id = m.vc_pa_company_id
	where 1=1
	and m.vc_pa_company_id in (62620, 57421)
*/