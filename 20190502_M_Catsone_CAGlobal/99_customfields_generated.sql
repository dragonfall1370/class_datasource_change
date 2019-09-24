--Company CF fields
create table companies_cf_value
(id numeric, 
cf_id numeric, 
cf_value nvarchar(max)
);

Bulk insert dbo.companies_cf_value
FROM 'E:\DataMigration\CAGlobal\PROD\Customfields\Company\companies_custom_fields_5.csv'
WITH (FORMAT = 'CSV',
      FIRSTROW=2,
      FIELDQUOTE = '"',
      FIELDTERMINATOR = ',', 
      ROWTERMINATOR = '0x0a');


update companies_cf_value
set cf_value = replace(replace(cf_value,'[',''),']','')

--Contact CF fields
create table contacts_cf_value
(id numeric, 
cf_id numeric, 
cf_value nvarchar(max)
);


/* CHECK CANDIDATE REFERENCE
select count(id) from candidates --443176

select *
from [candidates_custom_fields_159673_value]
where id in (select id
from [candidates_custom_fields_159673_value]
group by id 
having count(id) > 1)

delete top (1) from [candidates_custom_fields_159673_value] where id = 89456141


select *
from [candidates_custom_fields_171756_value]
where id in (select id
from [candidates_custom_fields_171756_value]
group by id 
having count(id) > 1)


select *
from [candidates_custom_fields_177043_value]
where id in (select id
from [candidates_custom_fields_177043_value]
group by id 
having count(id) > 1)

delete top (1) from [candidates_custom_fields_177043_value] where id = 89456141
*/