--Fee SPLIT
with 
--Selected users
selected_user as (select iduser, idorganizationunit, title, firstname, lastname
		, fullname, replace(useremail, 'spenglerfox.eu', 'spenglerfox.com') as useremail, createdon
		from "user"
		where isdisabled = '0'
		and useremail ilike '%_@_%.__%'
		and (firstname not ilike '%partner%' and jobtitle not ilike '%partner%')
		)

, users as (select a.idassignment
		, string_agg(useremail, ', ') as users
		, string_agg(fullname, ', ') as users_name
		from "assignment" a
		left join selected_user u on u.iduser = a.iduser
		where a.iduser is not NULL
		group by idassignment)
		
, associate as (select a.idassignment
		, string_agg(useremail, ', ') as associates
		, string_agg(fullname, ', ') as associates_name
		from assignmentassociate a
		left join "user" u on u.iduser = a.iduser
		where a.iduser is not NULL
		group by idassignment)

select f.idfee --used for tracking activities external id
, f.idassignment job_ext_id
, a.assignmenttitle
, u.users_name as assignment_owners
, f.createdon::timestamp as created_date
, concat_ws(chr(10), '[Fee information]'
	, coalesce('Assignment owners: ' || nullif(REPLACE(u.users_name, '\x0d\x0a', ' '), ''), NULL)
	, coalesce('Assignment associates: ' || nullif(REPLACE(ac.associates_name, '\x0d\x0a', ' '), ''), NULL)
	, coalesce('Fee reference: ' || nullif(REPLACE(f.feereference, '\x0d\x0a', ' '), ''), NULL)
	, coalesce('Fee description: ' || nullif(REPLACE(f.feedescription, '\x0d\x0a', ' '), ''), NULL)
	, coalesce('Fee comment: ' || nullif(REPLACE(f.feecomment, '\x0d\x0a', ' '), ''), NULL)
	, coalesce('Expected date: ' || nullif(REPLACE(f.expecteddate, '\x0d\x0a', ' '), ''), NULL)
	, coalesce('Actual date: ' || nullif(REPLACE(f.actualdate, '\x0d\x0a', ' '), ''), NULL)
	, coalesce('Total amount: ' || nullif(REPLACE(f.totalamount, '\x0d\x0a', ' '), ''), NULL)
	, coalesce('Conversion rate: ' || nullif(REPLACE(f.conversionrate, '\x0d\x0a', ' '), ''), NULL)
	, coalesce('Net amount: ' || nullif(REPLACE(f.netamount, '\x0d\x0a', ' '), ''), NULL)
	, coalesce('Currency: ' || nullif(REPLACE(cy.value, '\x0d\x0a', ' '), ''), NULL)
	) as description
, cast('-10' as int) as user_account_id
, 'comment' as category
, 'job' as type
from fee f
join "assignment" a on a.idassignment = f.idassignment
join company c on c.idcompany = a.idcompany
join currency cy on cy.idcurrency = f.idcurrency
left join users u on u.idassignment = f.idassignment
left join associate ac on ac.idassignment = f.idassignment
order by f.idassignment