--TALENT POOL
select ContactServiceID
	from ContactCategoriesTable
	where CategoryName in ('A-lister') --id: 21
	group by ContactServiceID
--> 128 rows

select distinct ContactServiceID
	from ContactCategoriesTable
	where CategoryName in ('Geplaatst door Catalize') --id: 22
	group by ContactServiceID
--> 73 rows

select distinct ContactServiceID
	from ContactCategoriesTable
	where CategoryName in ('Interim Manager') --id: 23
	group by ContactServiceID
--> 838 rows

select distinct ContactServiceID
	from ContactCategoriesTable
	where CategoryName in ('Opgepast !!!') --id: 24
	group by ContactServiceID
--> 62 rows

-------------------------
--TALENT POOL MAIN SCRIPT
select concat('CA',ContactServiceID) as CA_CandidateExtID
, case 
	when CategoryName = 'A-lister' then 21
	when CategoryName = 'Geplaatst door Catalize' then 22
	when CategoryName = 'Interim Manager' then 23
	when CategoryName = 'Opgepast !!!' then 24
	else NULL end as CA_CandidateGroup
, getdate() as CA_insertTimestamp
from ContactCategoriesTable
where CategoryName in ('A-lister','Geplaatst door Catalize','Interim Manager','Opgepast !!!')
and ContactServiceID in (select ContactServiceID from ContactMainTable where Type = 1 and IsDeletedLocally = 0)
order by ContactServiceID

-----------------------------------------------------------------
--DISTRIBUTION LIST
select distinct ContactServiceID
	from ContactCategoriesTable
	where CategoryName in ('Non-finance') --id: 2
	group by ContactServiceID
--> 461 rows

-------------------------------
--DISTRIBUTION LIST MAIN SCRIPT
select concat('CA',ContactServiceID) as CA_ContactExtID
, '2' as CA_ContactGroup
, getdate() as CA_insertTimestamp
from ContactCategoriesTable
where CategoryName in ('Non-finance')
order by ContactServiceID

----------------------------------------------------------------
---FUNCTIONAL EXPERTISE
select distinct ContactServiceID
	from ContactCategoriesTable
	where CategoryName in ('Credit Manager') --id: 2983
	group by ContactServiceID
--> 81 rows

select distinct ContactServiceID
	from ContactCategoriesTable
	where CategoryName in ('Treasury') --id: 2984
	group by ContactServiceID
--> 151 rows

----------------------------------
--FUNCTIONAL EXPERTISE MAIN SCRIPT
select concat('CA',ContactServiceID) as CA_CandidateExtID
, case 
	when CategoryName = 'Credit Manager' then '2983'
	when CategoryName = 'Treasury' then '2984'
	else '' end as CA_CandidateFE
, getdate() as CA_insertTimestamp
from ContactCategoriesTable
where CategoryName in ('Treasury','Credit Manager')
order by ContactServiceID


--CUSTOM FIELDS: OPT OUT - MASS MAILING | YES - NO
---with contacts/candidates having WebAddress, mark YES
select ContactServiceID, WebAddress from ContactMainTable
where WebAddress is not NULL and WebAddress <> ''
and type = 1 and IsDeletedLocally = 0

--CUSTOM FIELDS PRODUCTION
select a.form_id, cffv.field_id, a.translate, cffv.field_value, cfl.translate, cfl.language_code
from configurable_form_field_value cffv
left join configurable_form_language cfl on cffv.title_language_code = cfl.language_code
left join (select cff.form_id, cff.id, cff.field_type, cfl.language_code, cfl.translate
	from configurable_form_field cff
	left join configurable_form_language cfl on cff.label_language_code = cfl.language_code) a on a.id = cffv.field_id
order by cffv.field_id, cffv.title_language_code
--1005: "add_con_info" | 1006: "add_cand_info"
--field_id: 1015 -> contact | 1016 -> candidate -> value: YES

select * from contact where external_id in ('CA19','CA101','CA936')

select * from candidate where external_id in ('CA19','CA101','CA936')
