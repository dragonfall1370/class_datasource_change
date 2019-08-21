
-- INSERT TO VINCERE common_location table

select    
        convert(varchar(max),Cl.clientID) as 'externalId' -- nearest_train_station
	--, Cl.userID as '#UserID'
	, case when (ltrim(replace(UC.firstName,'?','')) = '' or  UC.firstName is null) then 'Firstname' else ltrim(replace(UC.firstName,'?','')) end as 'contact-firstName'
	, case when (ltrim(replace(UC.lastName,'?','')) = '' or  UC.lastName is null) then concat('Lastname-',Cl.clientID) else ltrim(replace(UC.lastName,'?','')) end as 'contact-Lastname'
        --, UC.address1 , UC.address2
        , Stuff( Coalesce(' ' + NULLIF(UC.address1, ''), '') + Coalesce(', ' + NULLIF(UC.address2, ''), ''), 1, 1, '') as 'address'
        , UC.city as 'city'
        , UC.state as 'state'
        , UC.zip as 'post_code'
        --, UC.countryID
        , CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation in ('NULL','ZR') ) THEN '' ELSE tc.abbreviation END as 'Country'
        , Stuff( Coalesce(' ' + NULLIF(UC.city, ''), '') 
                + Coalesce(', ' + NULLIF(UC.state, ''), '') 
                + Coalesce(', ' + NULLIF(UC.zip, ''), '')
                --+ Coalesce(', ' + NULLIF(tc.abbreviation, ''), '')
        , 1, 1, '') as 'location_name'
-- select count(*) --7487 -- select distinct tc.abbreviation
from bullhorn1.BH_Client Cl 
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
left join tmp_country tc ON UC.countryID = tc.code
where isPrimaryOwner = 1

