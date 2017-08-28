-- BULLHORN
select top 100
        Cl.clientID as 'contact-externalId'
	, case when (ltrim(replace(UC.firstName,'?','')) = '' or  UC.firstName is null) then 'Firstname' else ltrim(replace(UC.firstName,'?','')) end as 'candidate-firstName'
	, case when (ltrim(replace(UC.lastName,'?','')) = '' or  UC.lastName is null) then concat('Lastname-',Cl.clientID) else ltrim(replace(UC.lastName,'?','')) end as 'candidate-Lastname'
        , ltrim(rtrim(customText3)) as 'Industry'
from bullhorn1.BH_Client Cl
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
where isPrimaryOwner = 1 and Cl.clientID in (25,44,47) and
customText3 is not null and customText3 <> ''
order by Cl.clientID DESC;


-- VINCERE
select ci.*, v.* , c.first_name, c.last_name, c.external_id from contact_industry ci
left join vertical v on v.id = ci.industry_id
left join contact c on c.id = ci.contact_id
where c.external_id::int in (25,44,47)
limit 100

