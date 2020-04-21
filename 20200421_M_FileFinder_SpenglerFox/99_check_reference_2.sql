select idperson
from personofflimit
group by idperson
having count(*) > 1

select *
from personofflimit

select *
from offlimittype

select linkedin
from personx

select *
from personx

--COMMUNICATION INFO
select *
from personcommunicationtype
order by sortorder
--69dac425-b23d-4874-96ee-1c456e63ac6e | Skype
--22cfe759-44fb-4378-9c23-16b19fa00935 | linkedin
--dced2973-8162-4152-a75a-a7d7991d1577 | switchboard
--6b3fd179-fb26-4c9f-a22e-6eed1cc03aae | GatedTalent
--8c7d16c4-125f-498b-b932-5465373a782b | URL
--12ccc8ca-f9ad-468b-b9e1-54ec119a6044 | Mobile
--81f188ee-0c28-4e3f-aede-165a58214528 | Mobile (private)
--65743ebe-a6c0-4465-b4e0-406b79d12fb2 | Direct Line
--91c782eb-cc5a-40c0-beb5-60f866af8b79 | Mobile (business)
--3285c9df-8eb2-4b26-97ef-4a3dc7452aa9 | Email (private)

select p.parentid
, p2.value as parentvalue
, p.idpersoncommunicationtype
, p.value as childvalue
, p.isdefault
, p.isactive
, p.sortorder
from personcommunicationtype p
left join personcommunicationtype p2 on p2.idpersoncommunicationtype = p.parentid
order by parentvalue, childvalue, sortorder

select pe.idperson, pe.idpersoncommunicationtype, e.commvalue
from person_eaddress pe
left join eaddress e on e.ideaddress = pe.ideaddress
where pe.idpersoncommunicationtype = '22cfe759-44fb-4378-9c23-16b19fa00935' --linkedin

select pe.idperson, pe.idpersoncommunicationtype, e.commvalue
from person_eaddress pe
left join eaddress e on e.ideaddress = pe.ideaddress
where pe.idpersoncommunicationtype = 'dced2973-8162-4152-a75a-a7d7991d1577' --switchboard

select pe.idperson, pe.idpersoncommunicationtype, e.commvalue
from person_eaddress pe
left join eaddress e on e.ideaddress = pe.ideaddress
where pe.idpersoncommunicationtype in ('12ccc8ca-f9ad-468b-b9e1-54ec119a6044', '81f188ee-0c28-4e3f-aede-165a58214528') --Mobile (private) 
and pe.idperson = 'ef6b12c1-0e10-46e9-b5c3-da33e359a91b'

select defaultphone, mobileprivate, defaultmobile, emailprivate, directlinephone, mobilebusiness, url
from personx
where idperson = 'ef6b12c1-0e10-46e9-b5c3-da33e359a91b' --44 (0) 20 7749 6100

select count(*)
from personx
where 1=1
and defaultmobile is not NULL --268581
and mobileprivate is not NULL --268230

select idperson, idpersoncommunicationtype, count(*)
from person_eaddress
group by idperson, idpersoncommunicationtype
having count(*) > 1

/* AUDIT ASSIGNMENT */
select a.idassignment, a.idcompany, ac.idperson, ac.contactedon, cp.idcompany, ac.createdon
, ROW_NUMBER() OVER(PARTITION BY a.idassignment ORDER BY ac.contactedon ASC) rn
from assignmentcontact ac
left join "assignment" a on ac.idassignment = a.idassignment
left join company_person cp on cp.idperson = ac.idperson
where a.isdeleted = '0'
--and a.idassignment = '000b11d0-0ba5-42ee-966a-2f39df74aa73'

/* EMAIL CHECK */
select defaultemail
from personx
where defaultemail is not NULL
and isdeleted = '0' --388113

select emailprivate
from personx
where emailprivate is not NULL
and isdeleted = '0' --108728