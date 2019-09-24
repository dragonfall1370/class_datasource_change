--System schema
SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
where DATA_TYPE = 'nvarchar' or DATA_TYPE = 'varchar'

--select json
Declare @JSON varchar(max)

SELECT @JSON = BulkColumn
FROM OPENROWSET (BULK 'E:\DataMigration\CAGlobal\PROD\companies_custom_fields.json', SINGLE_CLOB) as j

SELECT * 
FROM OPENJSON (@JSON)
with (id int, data_item_type varchar(4000),name varchar(4000), comment nvarchar(max)) as Dataset

--DELETE duplicate records in each entity
select id
, row_number() over (partition by id order by datecreated desc) as rn
from candidates
where id in (select id
from candidates
group by id having count(id) > 1)--48

--MAX ID per each entity
-->>company
select --id
--min(id) --11921407
max(id) --19411744
, max(datecreated) --2019-03-04 19:34:46.0000000
--count(id) --11299
from companies

--->>contact
select --id
--min(id) --24930875
max(id) --40196398
, max(datecreated) --2019-03-07 20:11:14.0000000
--count(id) --22800
from contacts

--->>job
select --id
--min(id) --3941810
max(id) --12160916
, max(datecreated) --2019-04-05 19:54:31.0000000
--count(id) --13700
from jobs

--->>candidate
select --id
--min(id) --87487853
--max(id) --283871828
--, max(datecreated) --2019-04-07 06:13:49.0000000
count(id) --443176
from candidates

--->>document


--->>activity
select --id
--min(id) --248081159
max(id) --853704935
--, try_parse(max(date_created) as datetime) --2019-04-09 15:45:50.000
--count(id) --3292300
from activities