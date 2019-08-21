/*
select count(*) from candidate

select id, first_name,last_name, email from candidate where id = 34386
update candidate set email = concat(email,'_',id) where id = 34386

select id, first_name,last_name, email from contact where id = 63925
update contact set email = concat(email,'_',id) where id = 63925

select id,name,email from user_account where id = 29035
update user_account set email = concat(email,'_',id) where id = 29035
*/

create table _company(
        No int,
        Date varchar,
        Time varchar,
        timestamp timestamp,
        ID varchar,
        name varchar,
        email varchar,
        comment varchar,
        doc varchar )
-- drop table _company
-- truncate _company
update _company set test = concat(replace(replace(date,'.17','.2017'),'.','/'),' ',replace(replace(time,'pm',''),'.',':')) where name = 'Madison Pacific' --name = 'Madison Pacific Trust Limited'

select    date,time
        --, replace(replace(date,'.17','.2017'),'.','/') as date1
        --, replace(replace(time,'pm',''),'.',':') as time1
        , concat(replace(replace(date,'.17','.2017'),'.','-'),' ',replace(replace(time,'pm',''),'.',':')) as test
        , concat(replace(replace(date,'.17','.2017'),'.','-'),' ',replace(replace(time,'pm',''),'.',':'))::timestamp as test2
from _company where name = 'Madison Pacific'
select now()

create table _contact (
        No int,
        Date varchar,
        Time varchar,
        timestamp timestamp,
        bondID varchar,
        FirstName varchar,
        MIDDLEName varchar,
        LASTName varchar,
        CompanyName varchar,
        Email varchar,
        CommentsBy varchar,
        Comments varchar,
        Doc varchar )

create table _job (
        No int,
        Date varchar,
        Time varchar,
        timestamp timestamp,
        email varchar,
        Companyname varchar,
        Title varchar,
        salaryfrom varchar,
        salaryto  varchar,
        Location varchar,
        description varchar )
-------------------------------
create table _candidate (
        No int,
        Date varchar,
        Time varchar,
        timestamp timestamp,
        BondID varchar,
        FirstName varchar,
        MiddleName varchar,
        Lastname varchar,
        Email varchar,
        CommentsBy varchar,
        Comments varchar,
        doc varchar )
-- drop table _candidate
truncate _candidate
select * from _candidate

 SELECT  cast('0.5' as time)
 select to_timestamp(1284352323)
 select to_char(125, '999')