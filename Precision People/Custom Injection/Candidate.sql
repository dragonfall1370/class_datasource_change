--Income Required
select candidate_ref,income_required from candidate where income_required is not null

--Income Mode
with persontype as (select * from person_type where type = 'C')
select cast(b.person_ref as int) as candidate_ref,income_mode, c.description,
case when income_mode = 'A' then '1'
when income_mode = 'D' then '1'
when income_mode = 'H' then '1'
when income_mode = 'W' then '1'
when income_mode = 'M' then '2'
end as salary_type
from candidate a left join lookup c  on a.income_mode = c.code
right join persontype d on a.person_type_ref = d.person_type_ref
left join person b on d.person_ref = b.person_ref
where income_mode is not null and c.code_type = 7

--Currency
select cast(candidate_ref as int) as candidate_ref,
case when currency = 'USD' then 'USD'
when currency = '£' then 'GBP'
when currency = '€' then 'EU' end as currency
from candidate where currency is not null


--National ins no ( Custom Field)
select person_ref,national_ins_no from temp_details where national_ins_no is not null

--Pay Method
select person_ref,pay_method from temp_details where pay_method is not null

--Bank Acc No
select person_ref,bank_account_no from temp_details where bank_account_no is not null


--Candidate Reg Call ( missing person_id)
select zc_event_date,* from event where z_last_type = 'N10A'

--CV Sent
select zc_event_date,* from event where z_last_type = 'P50'

--Email Sent
select zc_event_date,* from event where z_last_type = 'KE02'

-----Candidate Source
with test as (select * from lookup where code_type = 102)
,persontype2 as (select *,row_number() over (partition by person_ref order by person_ref) as row_num from person_type )
,persontype as (select * from persontype2 where row_num = 1)

select cast(b.person_ref as int) as 'External_ID'
,trim(c.description) as 'CustomValue'
,'add_cand_info' as 'Additional_type'
,'Company Source' as 'lookup_name'
,getdate() as insert_timestamp
from candidate a
left join persontype d on a.person_type_ref = d.person_type_ref
left join person b on d.person_ref = b.person_ref
left join test c on b.source = c.code 
where b.source is not null


