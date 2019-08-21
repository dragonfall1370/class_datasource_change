select id, name, external_id, company_id, contact_id from position_description
-- where external_id = 'a0F0X00000gbflaUAA'
-- 	and deleted_timestamp is null
where company_id = 23454


--correct com id: 23386

select * from contact where id = 66671

select * from company where id = 23454

select * from company where id = 23386

-- UPDATE POSITION_DESCRIPTION
-- SET COMPANY_ID = 23454
-- --, CONTACT_ID = NULL
-- WHERE ID = 35600