-- JOB and COMPANY
select   pd.id,pd.external_id
        ,pd.name
        ,pd.company_id
        ,c.name
        ,pd.contact_id
        ,co.first_name, co.last_name
        ,pd2.*
from position_description pd
left join position_description2 pd2 on pd2.externalid = pd.external_id
left join company c on c.id = pd.company_id
left join contact co on co.id = pd.contact_id
where pd2.externalid is not null and pd.company_id = 61187 and pd2.company_name ilike '%Octopus Group%'

--select id,name from company where name ilike '%no c%'
--select count(*) from company where external_id is not null

select   pd.external_id
        ,pd.company_id
        ,c.id
        ,pd2.company_name
from position_description pd
left join position_description2 pd2 on pd2.externalid = pd.external_id
left join (select id,name from company) c on c.name ilike pd2.company_name
--left join contact co on co.id = pd.contact_id
where pd2.externalid is not null --and pd2.company_name ilike '%Octopus Group%'


update  position_description pd
set     company_id = c.id
--select pd.company_id, c.*
from --position_description pd left join
        ( select distinct c.id, c.name, pd2.externalid from company c
          left join position_description2 pd2 on pd2.company_name ilike c.name
          where pd2.externalid is not null --and pd2.company_name ilike '%Octopus Group%'        
        ) c --on c.externalid = pd.external_id
where pd.company_id = 61187 and c.externalid = pd.external_id





-- JOB and CONTACT
select   c.id,c.name
        ,co.first_name, co.last_name, co.external_id
        ,c2.company
--select *
from contact co --where company_id = 61187 
left join contact2 c2 on c2.externalid = co.external_id
left join company c on c.name ilike ltrim(rtrim(c2.company))
where co.company_id = 61187 and c2.externalid is not null --in ('380871-9048-16147','526987-5526-16311','512464-9185-16335')
--and c.id is null limit 300

--select co.first_name, co.last_name, co.external_id from contact co where co.company_id = 61187
--select * from company where name = 'AnalogFolk'
-- select * from contact2 where externalid in ('146365-6297-16270','248813-1946-13332','969914-5225-12174','358749-9469-1392','787232-2758-13338','188628-1976-12290','996248-7420-10158','191237-4138-137','379766-3854-1699','858305-2103-943','319318-5195-619','264792-3266-1699','309757-4973-1255','770633-6884-1265','741525-6407-12118','226300-9653-12205','610094-2952-16281','601301-4875-1366','502255-9434-9261','582641-8677-12205','444052-8827-12143','681915-9683-1365','191816-5262-14315','206308-6943-12200','155155-6613-12178','532335-6668-12241','538249-3482-146','839547-2341-773','607901-2697-16273','558425-4687-13196','686743-1290-1077','663262-6477-13196','399513-2136-13332','591649-9384-13338','469123-9253-137','601243-5609-12174','545258-6073-13289','417298-3644-1371','297902-6413-12114','974847-3066-13333','121285-6725-781','884614-5172-793','644219-7072-12347','227589-5267-13338','682831-1852-16315','Maur/62411/183','504152-7987-13170','283305-8425-16270','99999','796034-3875-1343','755931-1707-16336','961457-7398-16270')

update  contact co
set     company_id = c.id
-- select co.company_id, c.*
from --contact co left join
        ( select c.id
                ,co.first_name, co.last_name,co.external_id
                ,c2.company
        from contact co
        left join contact3 c2 on c2.externalid = co.external_id
        left join company c on c.name = ltrim(rtrim(c2.company))
        where co.company_id = 61187 and c2.externalid is not null and c.id is not null
        ) c --on c.external_id = co.external_id
where co.company_id = 61187 and c.external_id = co.external_id



/*
create table position_description2 (
        externalId varchar,
        title varchar,
        company_name varchar
        )

create table contact3 (
        externalId varchar,
        firstName varchar,
        lastName varchar,
        company varchar )
contact-externalId	contact-firstName	contact-lastName	CompanyName
*/