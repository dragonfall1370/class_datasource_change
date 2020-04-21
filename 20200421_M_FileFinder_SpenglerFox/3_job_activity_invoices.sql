WITH cte_contact AS (
	SELECT cp.idperson
	, ROW_NUMBER() OVER(PARTITION BY cp.idperson ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole) rn
	FROM company_person cp
	JOIN (select * from personx where isdeleted = '0') px ON cp.idperson = px.idperson
	JOIN (select * from person where isdeleted = '0') p ON cp.idperson = p.idperson
)

SELECT i.idinvoice
, i.idassignment as job_ext_id
, ip.idperson as con_ext_id
, i.createdon::timestamp as created_date
, concat_ws(chr(10), '[Job invoice]'
		, coalesce('Created by: ' || nullif(REPLACE(i.createdby, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Reference: ' || nullif(REPLACE(i.reference, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Invoice date: ' || nullif(REPLACE(i.invoicedate, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Invoice type: ' || nullif(REPLACE(i.invoicetype, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Invoice type: ' || nullif(REPLACE(i.invoicetype, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Contact: ' || nullif(REPLACE(i.contact, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Contact jobtitle: ' || nullif(REPLACE(i.contactjobtitle, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Where created: ' || nullif(REPLACE(i.wherecreated, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Signed off by: ' || nullif(REPLACE(i.signedoffby, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Is signed off: ' || nullif(REPLACE(case when i.issignedoff = '1' then 'YES' else 'NO' end, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Expected on: ' || nullif(REPLACE(i.expectedon, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Invoice address: ' || nullif(REPLACE(i.invoiceaddress, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Invoice number: ' || nullif(REPLACE(i.number, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Conversion rate: ' || nullif(REPLACE(i.conversionrate, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Currency name: ' || nullif(REPLACE(cur.currencyname, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Currency symbol: ' || nullif(REPLACE(cur.currencysymbol, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Invoice description: ' || nullif(REPLACE(i.invoicedescription, '\x0d\x0a', ' '), ''), NULL)
		, chr(10) || '[Invoice item details]'
		, coalesce('Created by: ' || nullif(REPLACE(ii.createdby, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Created on: ' || nullif(REPLACE(ii.createdon, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Modified by: ' || nullif(REPLACE(ii.modifiedby, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Modified on: ' || nullif(REPLACE(ii.modifiedon, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Invoice item type: ' || nullif(REPLACE(ii.invoiceitemtype, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Invoice item comment: ' || nullif(REPLACE(ii.invoiceitemcomment, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Tax rate: ' || nullif(REPLACE(ii.taxrate, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Tax amount: ' || nullif(REPLACE(ii.taxamount, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Net amount: ' || nullif(REPLACE(ii.netamount, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Total amount: ' || nullif(REPLACE(ii.totalamount, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Item note: ' || chr(10) || nullif(REPLACE(ii.itemnote, '\x0d\x0a', ' '), ''), NULL)
	) description
, cast('-10' as int) as user_account_id
, 'comment' as category
, 'job' as type
FROM invoice i
left join invoiceitem ii on ii.idinvoice = i.idinvoice
left join invoice_person ip on ip.idinvoice = i.idinvoice
left join currency cur on cur.idcurrency = i.idcurrency
left join (select * from cte_contact where rn = 1) c on c.idperson = ip.idperson