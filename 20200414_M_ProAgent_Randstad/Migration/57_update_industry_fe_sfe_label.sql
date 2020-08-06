--
select *
from vertical_detail_language
where language = 'ja'
and position('【PP】' in name) > 0


update vertical_detail_language
set name = overlay(name placing '' from 1 for length('【PP】'))
where language = 'ja'
and position('【PP】' in name) > 0


--
select *
from functional_expertise_detail_language
where language = 'ja'
and position('【PP】' in name) > 0


update functional_expertise_detail_language
set name = overlay(name placing '' from 1 for length('【PP】'))
where language = 'ja'
and position('【PP】' in name) > 0

--
select *
from sub_functional_expertise_detail_language
where language = 'ja'
and position('【PP】' in name) > 0