--INTERIM JOBS
select assignmenttitle
from "assignment"
where assignmenttitle ilike '%interim%' --376 rows
limit 10

--Fee SPLIT
with users as (select a.idassignment
		, string_agg(useremail, ', ') as users
		, string_agg(fullname, ', ') as users_name
		from "assignment" a
		left join "user" u on u.iduser = a.iduser
		where a.iduser is not NULL
		group by idassignment)
		
, associate as (select a.idassignment
		, string_agg(useremail, ', ') as associates
		, string_agg(fullname, ', ') as associates_name
		from assignmentassociate a
		left join "user" u on u.iduser = a.iduser
		where a.iduser is not NULL
		group by idassignment)

select f.idfee, f.idassignment, a.assignmenttitle
, u.users_name as assignment_owners
, ac.associates_name
, c.idcompany, c.companyname
, f.feereference
, cy.value as currency
, f.feedescription
, f.feecomment
, f.expecteddate
, f.actualdate
, f.totalamount
, f.conversionrate
, f.netamount
from fee f
join "assignment" a on a.idassignment = f.idassignment
join company c on c.idcompany = a.idcompany
join currency cy on cy.idcurrency = f.idcurrency
left join users u on u.idassignment = f.idassignment
left join associate ac on ac.idassignment = f.idassignment

--Consultant Split
select idfee
, idassignment
, consultant
, consultantfeenote
, consultantfeerate
, consultantfeepercent
, consultantfeedescription
, cy.value as currency
from consultantfee c
join currency cy on cy.idcurrency = c.idcurrency
where idfee is not NULL
order by idfee

--JOB vs JOB LEADS
select idassignment, a.idassignmentorigin, ao.value, estimatedfee, estimatedvalue
from assignment a
left join assignmentorigin ao on ao.idassignmentorigin = a.idassignmentorigin
where 1=1
and a.idassignmentorigin is not NULL
and ao.value in ('Lead / Referral', 'Lead') --576 rows

--pg_dump -U postgres -h localhost -p 5432 -d spenglerfox2 -f "H:\VC_SpenglerFoxPROD\prod_bkup"

with filterjob as (select idassignment
from "assignment"
where assignmentno::int in (1004879,1007680,1008886,1011960,1013354,2001160,2001522,2001595,2001616,2001645,2001646,2001647)
)

--List of contacts
select *
from assignmentcontact
--where idassignment in (select idassignment from filterjob) --18 rows

--List of multi contacts
select idassignment, count(*)
from assignmentcontact
where idassignment in (select idassignment from filterjob)
group by idassignment
having count(*) > 1


--JOBS
---Audit special cases
select *
from cte_contact
where idperson = '65ceb88c-8459-481a-88ac-3025c5ccc355'c

select *
from assignmentcontact
where idperson = '65ceb88c-8459-481a-88ac-3025c5ccc355'

select * 
from "assignment"
where idassignment in ('f19f412a-3491-40ce-9706-58e1af3d16b7', '846d3e8f-8633-49c3-9b57-c6869febbb70', '078eaa2e-3736-435b-9485-51644f11f1b3', '554d0819-530a-466d-a4ee-36b2f14585ff', '97ee6c5d-14f3-49a1-ba71-1e2b65e042a7') --1025f2ae-55d5-4fec-ae6c-b08029a29f8f

select *
from company_person
where idcompany = '1025f2ae-55d5-4fec-ae6c-b08029a29f8f'

select idperson, isdeleted
from personx
where idperson = '65ceb88c-8459-481a-88ac-3025c5ccc355'


--Check for other job (different companies from job / company_person)
select *
from company_person
where idperson = '478ed081-89ef-4e3b-9d00-3129cd5387f4' --2856787e-6d42-48d8-9867-581e684f9d52 <> f40df2e2-a6c1-4aee-9a82-9497ad2fef34


--Company not having any contacts
select *
from company_person
where idcompany = '4aac4af3-1dd3-4305-a13a-705cbb1e167e'

select *
from company
where idcompany = '4aac4af3-1dd3-4305-a13a-705cbb1e167e'