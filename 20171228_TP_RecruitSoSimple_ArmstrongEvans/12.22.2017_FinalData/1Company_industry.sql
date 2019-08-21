with ind as (
select
          c.company_externalId as 'company-externalId'
	, c.company_name as 'company-name'
	, case cast(c.sector as varchar(max))
                when 'Aerospace & Automotive' then 28887
                when 'Backup Power' then 28884
                when 'Building Services' then 28885
                when 'General Engineering' then 28888
                when 'Marine' then 28889
                when 'Plant & Materials Handling' then 28886
                when 'Power' then 28890
                when 'Renewable Energy' then 28891
                when 'Utilities' then 28778
	end as ind
-- select * -- select count (*) --3825 -- select distinct cast(c.sector as varchar(max))
from CompanyImportAutomappingTemplate c
)
select * from ind where ind is not null


with cf as (
        select
          cast(c.company_externalId as varchar(max)) as 'additional_id'
	, cast(c.company_name as varchar(max)) as 'company_name'
        , 'add_com_info' as additional_type
        , convert(int,1005) as form_id
        , convert(int,1019) as field_id
	, cast(c.customfield1 as varchar(max)) as 'field_value_'
	, case cast(c.customfield1 as varchar(max))
                when 'Agricultural' then 1
                when 'Anaerobic Digestion' then 2
                when 'Biomass' then 3
                when 'CHP' then 4
                when 'Compressed Air' then 5
                when 'Cranes' then 6
                when 'Fire & Security' then 7
                when 'Forklifts' then 8
                when 'Generators' then 9
                when 'Generators,CHP' then 10
                when 'HVAC' then 11
                when 'Lifts' then 12
                when 'Marine' then 13
                when 'Powered Access' then 14
                when 'Pumps' then 15
                when 'Pumps,Generators' then 16
                when 'Renewable Energy' then 17
                when 'Solar' then 18
                when 'Switchgear' then 19
                when 'Telecoms' then 20
                when 'UPS' then 21
                when 'UPS,Generators' then 22
                when 'UPS,Generators,DRUPS' then 23
                when 'UPS,Generators,HVAC' then 24
                when 'UPS,Generators,UPS' then 25
                when 'Wind' then 26
	end as field_value
        -- select * -- select count (*) --3825 -- select distinct cast(c.sector as varchar(max))
        from CompanyImportAutomappingTemplate c where cast(c.customfield1 as varchar(max)) <> ''
)
select additional_id,company_name,additional_type,form_id,field_id,field_value_,convert(varchar,field_value) as field_value from cf where field_value is not null and field_value <> ''