select jobid, candid, createdate, salary, actionuserid, 
iif(Status = 'JOBREJECT', getdate(),getdate()) as 'status' from JobHistory where status in ('JOBREJECT','CNDREJECT')


select * from JobHistory
where candid = 442



with email as (select distinct email, lastname, firstname from Vuser where email like '%@e-merge%')
, userinfo as (select a.email, b.id, b.username, a.firstname, a.lastname, ROW_NUMBER() over (partition by a.email order by b.id) as 'row_num' from email a left join Vuser b on a.email = b.email)

select ltrim(rtrim(email)) as email, ltrim(rtrim(username)) as username, firstname, lastname from userinfo where row_num = 1

