/* CREATE COMPANY FEE MODEL ID SEQUENCE

CREATE SEQUENCE IF NOT EXISTS company_fee_model_id_seq OWNED BY company_fee_model.id;
ALTER TABLE company_fee_model ALTER COLUMN id SET DEFAULT nextval('company_fee_model_id_seq'::regclass);
SELECT setval('company_fee_model_id_seq', (SELECT MAX(id) FROM company_fee_model));

*/

--[Candidate placement] Fee model
select concat('PRTR',company_id) as ComExtID
, 'Candidate Placement' as fee_model_name
, 1 as fee_model_type --% based (Variable)
, 'thb' as currency --default for PRTR
, 1 as contract_type --Permanent by default
, 2 as contract_category
, 20 as charge_rate --20% as their default
, 1 as gross_annual_salary --gross annual salary as selection
, getdate() as insert_timestamp
from company.Companies

UNION ALL

select concat('PRTR',company_id) as ComExtID
, 'Deposit Fee' as fee_model_name
, 2 as fee_model_type --Fixed fee
, 'thb' as currency --default for PRTR
, NULL as contract_type --Permanent by default
, NULL as contract_category
, NULL as charge_rate --20% as their default
, NULL as gross_annual_salary --gross annual salary as selection
, getdate() as insert_timestamp
from company.Companies

UNION ALL

select concat('PRTR',company_id) as ComExtID
, 'Background Check' as fee_model_name
, 2 as fee_model_type --Fixed fee
, 'thb' as currency --default for PRTR
, NULL as contract_type --Permanent by default
, NULL as contract_category
, NULL as charge_rate --20% as their default
, NULL as gross_annual_salary --gross annual salary as selection
, getdate() as insert_timestamp
from company.Companies

UNION ALL

select concat('PRTR',company_id) as ComExtID
, 'Management Fee' as fee_model_name
, 2 as fee_model_type --Fixed fee
, 'thb' as currency --default for PRTR
, NULL as contract_type --Permanent by default
, NULL as contract_category
, NULL as charge_rate --20% as their default
, NULL as gross_annual_salary --gross annual salary as selection
, getdate() as insert_timestamp
from company.Companies
UNION ALL

select concat('PRTR',company_id) as ComExtID
, 'Multiple Placement' as fee_model_name
, 2 as fee_model_type --Fixed fee
, 'thb' as currency --default for PRTR
, NULL as contract_type --Permanent by default
, NULL as contract_category
, NULL as charge_rate --20% as their default
, NULL as gross_annual_salary --gross annual salary as selection
, getdate() as insert_timestamp
from company.Companies

UNION ALL

select concat('PRTR',company_id) as ComExtID
, 'Engagement Fee' as fee_model_name
, 2 as fee_model_type --Fixed fee
, 'thb' as currency --default for PRTR
, NULL as contract_type --Permanent by default
, NULL as contract_category
, NULL as charge_rate --20% as their default
, NULL as gross_annual_salary --gross annual salary as selection
, getdate() as insert_timestamp
from company.Companies

UNION ALL

select concat('PRTR',company_id) as ComExtID
, 'Deduction' as fee_model_name
, 2 as fee_model_type --Fixed fee
, 'thb' as currency --default for PRTR
, NULL as contract_type --Permanent by default
, NULL as contract_category
, NULL as charge_rate --20% as their default
, NULL as gross_annual_salary --gross annual salary as selection
, getdate() as insert_timestamp
from company.Companies