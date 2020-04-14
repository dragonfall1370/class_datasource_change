--COMPANY PAYMENT TERMS
with billing as (select [企業 PANO ] as com_ext_id
, [請求先 部署名1] as billing_group_name --department name
, [請求先 担当者名1] as contact_name
, [請求先 TEL1] as company_business_no
, concat_ws(char(10)
		, coalesce('[請求先 〒1]' + nullif([請求先 〒1],''), NULL) --Street address: Post code
		, coalesce('[請求先 都道府県1]' + nullif([請求先 都道府県1],''), NULL) --Street address: Prefecture
		, coalesce('[請求先 住所詳細1]' + nullif([請求先 住所詳細1],'') + char(10), NULL) --Street address: Address
		
		, coalesce('[請求先 部署名2]' + nullif([請求先 部署名2],''), NULL)
		, coalesce('[請求先 担当者名2]' + nullif([請求先 担当者名2],''), NULL)
		, coalesce('[請求先 TEL2]' + nullif([請求先 TEL2],'') + char(10), NULL)
		, coalesce('[請求先 〒2]' + nullif([請求先 〒2],''), NULL)
		, coalesce('[請求先 都道府県2]' + nullif([請求先 都道府県2],''), NULL)
		, coalesce('[請求先 住所詳細2]' + nullif([請求先 住所詳細2],''), NULL)
		, coalesce('[その他請求先]' + nullif([その他請求先],''), NULL)
		) as company_payment_terms
from csv_recf_claim)

/*--Audit maximum characters
select max(len(company_payment_terms))
from billing --maximum 343 chars | VC max 400
*/

select *
from billing
where coalesce(nullif(billing_group_name,''), nullif(contact_name,''), nullif(company_payment_terms,''), nullif(company_business_no,'')) is not NULL