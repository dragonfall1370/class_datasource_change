---------address--------
with test as (select *, row_number() over (partition by person_ref order by person_ref) as rn from position)
select 
a.person_ref,
iif(address_line_1 is null or address_line_1 ='','',address_line_1) as location_name,
concat(iif(c.address_line_1 is null or c.address_line_1 = '','',c.address_line_1), 
nullif(concat(', ',c.post_town),', '),
nullif(concat(', ',c.county_state),', '),
nullif(concat(', ',c.zipcode),', ')
) as 'company-address',
iif(c.zc_telephone_number is null or c.zc_telephone_number = '','',c.zc_telephone_number) as 'contact-mobilephone',
iif(b.zc_mobile_telno is null or b.zc_mobile_telno = '','',b.zc_mobile_telno) as 'private_mobile'
from test a
left join person b on a.person_ref = b.person_ref
left join address c on a.person_ref = c.person_ref


----- job level

with test as (select * from lookup where code_type = 135)
select contact_status,person_ref,b.description from position a 
left join test b on a.contact_status = b.code
where contact_status is not null