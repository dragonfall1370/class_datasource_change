--------Company Reg No
select organisation_ref, company_reg_no from organisation where company_reg_no is not null

--------------------Address 2
select a.organisation_ref, b.address_line_2 from organisation a left join address b on a.organisation_ref = b.organisation_ref where address_line_2 is not null

------------- 
select a.organisation_ref, b.address_line_3  from organisation a left join address b on a.organisation_ref = b.organisation_ref where address_line_3 is not null

------------
select * from lookup where code in ('ADVC','BDCC','BDCM','CANC','CANL','CLIL','EMLC','EX','GGLC','REF1','REFC') and code_type = 108

with test as (select * from lookup where code_type = 108)
select cast(a.organisation_ref as int) as 'External_ID'
,trim(b.description) as 'CustomValue'
,'add_com_info' as 'Additional_type'
,'Company Source' as 'lookup_name'
,getdate() as insert_timestamp
from organisation a left join test b on a.source = b.code 
where source is not null