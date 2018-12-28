select count(*) from JOBS_DATA_TABLE -- 22763

-- job not associated with contact
select count(*) from JOBS_DATA_TABLE
where CONT_ID is null
-- 10 => based on associated company

-- job not associated with both contact and company
select count(*) from JOBS_DATA_TABLE
where CONT_ID is null and CLNT_ID is null
-- 9 => associated with default contact of default company

select count(*) from JOBS_DATA_TABLE
where CONT_ID is null and CLNT_ID is not null
-- 1 => check if company exist or not

-- job not associated with contact, associated company is not exist
select count(*) from JOBS_DATA_TABLE
where CONT_ID is null and CLNT_ID is not null
and CLNT_ID not in (
	select CLNT_ID from CLNTINFO_DATA_TABLE
)
-- 0

-- job associated with contact, but contact not associated with any company (acctually those was associated with default company)


select count(*) from CONT_DATA_TABLE where CLNT_ID not in (select CLNT_ID from CLNTINFO_DATA_TABLE)

-- num of unique contact that need to be created
select distinct CONT_ID from JOBS_DATA_TABLE
where CONT_ID is not null and CONT_ID not in (
	select CONT_ID from CONT_DATA_TABLE
)
-- 783 => how many go to default company

select distinct CONT_ID, 'DefCom000' as CLNT_ID from JOBS_DATA_TABLE
where CONT_ID in (
	-- num of unique contact that need to be created
	select distinct CONT_ID from JOBS_DATA_TABLE
	where CONT_ID is not null and CONT_ID not in (
		select CONT_ID from CONT_DATA_TABLE
	)
)
and CLNT_ID not in (
	select CLNT_ID from CLNTINFO_DATA_TABLE
) -- 86

-- select 783 - 86

select distinct CONT_ID, CLNT_ID from JOBS_DATA_TABLE
where CONT_ID in (
	-- num of unique contact that need to be created
	select distinct CONT_ID from JOBS_DATA_TABLE
	where CONT_ID is not null and CONT_ID not in (
		select CONT_ID from CONT_DATA_TABLE
	)
)
and CLNT_ID in (
	select CLNT_ID from CLNTINFO_DATA_TABLE
)

-- job associated with contact but contact is not exist
select count(*) from JOBS_DATA_TABLE
where CONT_ID is not null and CONT_ID not in (
	select CONT_ID from CONT_DATA_TABLE
)
-- 1134 => 

select distinct * from (
select distinct x.CLNT_ID from (
select * from JOBS_DATA_TABLE
where CONT_ID is not null and CONT_ID not in (
	select CONT_ID from CONT_DATA_TABLE
)
and CLNT_ID is not null
) x -- 432
) y
where y.CLNT_ID not in (select CLNT_ID from CLNTINFO_DATA_TABLE) -- 56

--select 432 - 56 -- 376

select count(*) from JOBS_DATA_TABLE where CLNT_ID is null

select * from CLNTINFO_DATA_TABLE
where CLNT_ID not in (select CLNT_ID from CONT_DATA_TABLE where CLNT_ID is not null) -- 638
and CLNT_ID in (select CLNT_ID from JOBS_DATA_TABLE where CLNT_ID is not null)

-- 738 --783