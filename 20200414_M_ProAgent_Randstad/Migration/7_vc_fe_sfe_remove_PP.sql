--Changing FE name if any concern
select *
, overlay(name placing '' from 1 for length('【PP】')) as new_name
from functional_expertise
where id > 3043
order by id