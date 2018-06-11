CREATE TABLE Temp_Can_Emails1 (
id CHAR(36) PRIMARY KEY,
email_address_id CHAR(36) NOT NULL,
email_address VARCHAR(255),
first_name VARCHAR(100),
last_name VARCHAR(100),
date_modified DATETIME,
max_date_modified DATETIME
)

insert into Temp_Can_Emails1 
select tc.id, eabr.email_address_id, ea.email_address, tc.first_name, tc.last_name, eabr.date_modified, max(eabr.date_modified) as max_date_modified
from email_addr_bean_rel eabr
 left join email_addresses ea on eabr.email_address_id = ea.id
 left join Temp_Candidates tc on eabr.bean_id = tc.id
 where tc.id is not null
 and eabr.primary_address = 1
-- and eabr.bean_module = 'Contacts'
-- where eabr.bean_id in (select contact_id from accounts_contacts)
group by tc.id
-- select * from Temp_Can_Emails1

-- ---------------------
CREATE TABLE Temp_Can_Emails2 (
id CHAR(36) PRIMARY KEY,
email_address_id CHAR(36) NOT NULL,
email_address VARCHAR(255),
first_name VARCHAR(100),
last_name VARCHAR(100),
date_modified DATETIME,
max_date_modified DATETIME,
rn int
)

insert into Temp_Can_Emails2 -- this table will be used to added to Note: Candidate original email
select tce.id, tce.email_address_id, tce.email_address, tce.first_name, tce.last_name, tce.date_modified, tce.max_date_modified, count(*) as rn
from Temp_Can_Emails1 tce join Temp_Can_Emails1 tcea on tce.email_address = tcea.email_address
and tce.id >= tcea.id and tce.email_address like '%_@_%.__%'
group by tce.email_address, tce.id
-- limit 1000
-- select *, replace(email_address, ' ','') from Temp_Can_Emails2 where rn>2 order by email_address
-- select *, replace(email_address, char(9),'') from  Temp_Can_Emails2 order by email_address
-- ---------------------
CREATE TABLE Temp_Can_Emails3_main (
id CHAR(36) PRIMARY KEY,
email_address VARCHAR(255)
)

insert into Temp_Can_Emails3_main
(select id, 
case 
when rn=1 then replace(email_address,' ','')
else concat('DUPLICATE',rn,'_',replace(email_address,' ',''))
end as email_address
from Temp_Can_Emails2)


-- select * from Temp_Can_Emails3_main where email_address like 'DUPLICATE%'
-- select * from Temp_Can_Emails3_main where email_address like '%:%'
-- select *, left(email_address,locate('/',email_address)-1) from Temp_Can_Emails3_main where email_address like '%/%'

CREATE TABLE Temp_Can_Emails4_main (
id CHAR(36) PRIMARY KEY,
email_address VARCHAR(255)
)

insert into Temp_Can_Emails4_main -- use this for candidate emails
(select id
, case 
	  when locate(',',email_address) <> 0 then left(email_address,locate(',',email_address)-1)
      when locate(';',email_address) <> 0 then left(email_address,locate(';',email_address)-1)
      when locate('/',email_address) <> 0 then left(email_address,locate('/',email_address)-1)
	else ltrim(rtrim(replace(replace(replace(replace(email_address,' ',''),'!',''),'\t',''),':','')))
	end as email_address
from Temp_Can_Emails3_main
where email_address not like '%=%' and email_address not like '%*%' and email_address not like '%(%' and email_address not like '%@%@%')