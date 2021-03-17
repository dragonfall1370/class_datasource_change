select distinct f03."2 company xref" as company_id
	, "20 contact xref"
	, 'DEF' || f03."2 company xref" as contact_id
	, 'Default contact [Company Ref ' || f02."6 ref no numeric" || ']' as contact_lastname
	from f03
	left join f02 on f02.uniqueid = f03."2 company xref"
	where 1=1
	and (nullif("20 contact xref", '') is NULL OR "20 contact xref" not in (select uniqueid from f01 where "100 contact codegroup  23" = 'Y'))
	and "2 company xref" in (select uniqueid from f02) --244
	
UNION
select 'MM999999999' as company_id
, '' as "20 contact xref"
, 'MM999999999' as contact_id
, 'Default contact' as contact_lastname