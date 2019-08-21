with location1 as (select CndProfInfo.id, description, '' as 'a' from CndProfInfo left join Location on CndProfInfo.curlocation = Location.id),

location2 as (select id, description,
case when description like '%-%' then '1' else '2' end as 'town_type'
from location1),


location3 as (select * from location2 cross apply string_split(description,'-') where town_type=1),

location4 as (select *, row_number() over (partition by id order by id) as row_num from location3)

select id, value from location4 where row_num=2
