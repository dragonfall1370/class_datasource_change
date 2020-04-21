--List of industry
select idindustry
, value as industry
, current_timestamp as insert_timestamp
from industry

UNION ALL

select idudpracticegroup
, '[Practice Group] ' || value as practice_group
, current_timestamp as insert_timestamp
from udpracticegroup
order by industry


/* List of parent industry
--Parent industry
select distinct i2.value as parent
from industry i
left join industry i2 on i.parentid = i2.idindustry


--Parent / sub industry
select i.parentid
, i2.value as parent
, i.idindustry
, i.value as industry
from industry i
left join industry i2 on i.parentid = i2.idindustry
*/