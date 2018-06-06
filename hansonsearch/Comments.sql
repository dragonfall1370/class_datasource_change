------------------- 
CREATE SEQUENCE rid_seq START WITH 6000 INCREMENT BY 1;
alter table contact_comment alter column id set default nextval('rid_seq');
commit;
select * from contact limit 100

select * from contact_comment limit 100 
select count(*) from contact_comment --408
INSERT INTO contact_comment (contact_id, user_id, comment_content, insert_timestamp) VALUES ( 64167, -10, 'TESTING', '2019-01-01 00:00:00' )

select c.id,c.external_id ,cc.comment_content, cc.*
from contact_comment cc 
left join contact c on c.id = cc.contact_id 
where c.external_id is not null 
--and cc.comment_content like 'Comments%'
--in ('224941-3462-13149','732809-5396-1211','301111-8289-15155')
limit 100 


-------------------

-- CONTACT
with temp_1 as (
        select
                  c.contactid
                  , cast('-10' as int) as userid
                --, COALESCE(c.FirstName + ' ' + c.LastName, '') as ContactName
                , li.LogDate
                --, u.UserName, li.ShortUser
                , 'Date: ' + coalesce(convert(varchar(100), li.LogDate, 120),'') + char(10)
                + 'From: ' + COALESCE(u.UserName, li.ShortUser, '')  + char(10)
                + 'Subject: ' + COALESCE(li.Subject, '') + char(10)
                --+ 'Body: ' + COALESCE(ld.Text, '') as comment
                + COALESCE('Body: ' + ld.Text, '') + char(10) as comment
        -- select count(*) -- 237.906
        from dbo.Contacts c
        left outer join dbo.LogItems li ON c.ContactId = li.ItemId
        left outer join dbo.LogDataIndex ldi ON ldi.LogItemId = li.LogItemId
        left outer join dbo.LogData ld ON ldi.LogDataId = ld.LogDataId
        left outer join dbo.Users u ON li.userid = u.UserId
        where c.descriptor = 1 ) -- FOR CONTACT 235647 >> 70.955
        --and ld.Text <> '' ) --and ItemId is not null and ltrim(rtrim(ItemId)) <> '' )
select * from temp_1 where contactid in ('100016-4726-13112')--,'100265-5795-13115','100373-6207-13239','100354-8572-1444','100397-3096-16356','100407-1295-13170','100485-3984-13154','105107-6641-11189','105191-5708-13169','105226-3277-5178') -- ('471033-6602-1272','627889-3337-943','742569-3544-1274','968418-5370-10147')
order by contactid,LogDate asc --70955
--select contactid from temp_1 group by contactid having count(*) > 1





------------------- 
CREATE SEQUENCE rid_seq0 START WITH 1 INCREMENT BY 1;
alter table position_candidate_feedback alter column id set default nextval('rid_seq0');
commit;
select id,external_id from candidate where external_id in ('158672-6004-13345','158514-5567-15273','158554-8608-8296')
select * from position_candidate_feedback
select count(*) from position_candidate_feedback -- 0
insert into position_candidate_feedback(candidate_id,user_account_id,comment_body, feedback_timestamp, insert_timestamp, contact_method, related_status) values(128992, -10, 'TESTING', '2019-01-01 00:00:00', '2019-01-01 00:00:00', 4, 1)
------------------- 
-- CANDIDATE
with temp_1 as (
        select
                  c.contactid
                  , cast('-10' as int) as userid
                  , cast('4' as int) as contact_method
                  , cast('1' as int) as related_status
                --, COALESCE(c.FirstName + ' ' + c.LastName, '') as ContactName
                , li.LogDate
                --, u.UserName, li.ShortUser
                , 'Date: ' + coalesce(convert(varchar(100), li.LogDate, 120),'') + char(10)                
                + 'From: ' + COALESCE(u.UserName, li.ShortUser, '') + char(10)
                + 'Subject: ' + COALESCE(li.Subject + char(10), '')
                + COALESCE('Body: ' + ld.Text, '') + char(10) as comment
        -- select count(*)  --1.365.376
        from dbo.Contacts c
        left outer join dbo.LogItems li ON c.ContactId = li.ItemId
        left outer join dbo.LogDataIndex ldi ON ldi.LogItemId = li.LogItemId
        left outer join dbo.LogData ld ON ldi.LogDataId = ld.LogDataId
        left outer join dbo.Users u ON li.userid = u.UserId
        where c.descriptor = 2 )  -- FOR CANDIDATE 1338912 >> 437.513
        --and ld.Text <> '' ) --and ItemId is not null and ltrim(rtrim(ItemId)) <> '' )
select * from temp_1 where contactid in ('158672-6004-13345','158514-5567-15273','158554-8608-8296') --('952825-8677-14314') --('100016-4726-13112','100265-5795-13115','100373-6207-13239','100354-8572-1444','100397-3096-16356','100407-1295-13170','100485-3984-13154','105107-6641-11189','105191-5708-13169','105226-3277-5178') -- ('471033-6602-1272','627889-3337-943','742569-3544-1274','968418-5370-10147')
order by contactid,LogDate asc --70955
--select contactid from temp_1 group by contactid having count(*) > 1

--AND
with temp_1 as (
        select
                  c.contactid
                  ,c.FirstName, c.LastName
                  , cast('-10' as int) as userid
                  , cast('4' as int) as contact_method
                  , cast('1' as int) as related_status
                , c.lastupdate
                , c.comments
                --, COALESCE('Comments: ' + cast(c.comments as varchar(max)), '') as comment_content
                --, COALESCE('Comments: ' + cast(c.comments as varchar(max)), '') as comment_content
                , COALESCE('Comments: ' + nullif(dbo.DecodeUTF8String(c.comments),''), '') as comment_content
                --coalesce(NULLIF(CL.Country, ''), CL.Location)
                --, COALESCE('Comments: ' + convert(varchar(max),cast(c.comments as nvarchar(max)) ), '') as comment_content1
                --, COALESCE('Comments: ' + dbo.UTF8_TO_NVARCHAR(c.comments), '') as 'comment_content2'
                --,REPLACE(c.comments COLLATE Latin1_General_BIN,NCHAR(65533) COLLATE Latin1_General_BIN,'') as 'comment_content3'
        -- select count(*) 
        from dbo.Contacts c
        where c.descriptor = 2 and c.comments is not null and c.comments <> '' )
--select * from temp_1 where contactid in ('100335-1326-16117','827498-7644-15245','732235-5215-8122','508924-1686-15131') --> '43100' 
SELECT *, NewColumn1 = dbo.DecodeUTF8String(comments) FROM temp_1 WHERE comments <> dbo.DecodeUTF8String(comments) and contactid in ('100335-1326-16117','827498-7644-15245','732235-5215-8122','508924-1686-15131')
--select * from temp_1 where CONVERT(binary(5000),temp_1.comment_content) != CONVERT(binary(5000),CONVERT(nvarchar(1000),CONVERT(varchar(1000),temp_1.comment_content)))
SELECT * FROM temp_1 WHERE cast(comment_content AS varchar) like '%' + char(0) +'%';
UPDATE temp_1 SET comment_text = dbo.removeNullCharacters(comment_content) WHERE cast(comment_text AS varchar) like '%' + char(0) +'%';


--OLD-- CANDIDATE 

with temp_1 as (
        select
                  c.contactid
                  , cast('-10' as int) as userid
                  , cast('4' as int) as contact_method
                  , cast('1' as int) as related_status
                --, COALESCE(c.FirstName + ' ' + c.LastName, '') as ContactName
                , li.LogDate
                --, u.UserName, li.ShortUser
                , 'Date: ' + coalesce(convert(varchar(10), li.LogDate, 120),'') + char(10)
                + 'From: ' + COALESCE(u.UserName, li.ShortUser, '')  + char(10)
                + 'Subject: ' + COALESCE(li.Subject, '') + char(10)
                + 'Body: ' + COALESCE(ld.Text, '') as comment
        -- select count(*) 
        from dbo.Contacts c
        left outer join dbo.LogItems li ON c.ContactId = li.ItemId
        left outer join dbo.LogDataIndex ldi ON ldi.LogItemId = li.LogItemId
        left outer join dbo.LogData ld ON ldi.LogDataId = ld.LogDataId
        left outer join dbo.Users u ON li.userid = u.UserId
        where c.descriptor = 2  -- FOR CANDIDATE 1338912 >> 437.513
        and ld.Text <> '' ) --and ItemId is not null and ltrim(rtrim(ItemId)) <> '' )
select * from temp_1 where contactid in ('952825-8677-14314') --('100016-4726-13112','100265-5795-13115','100373-6207-13239','100354-8572-1444','100397-3096-16356','100407-1295-13170','100485-3984-13154','105107-6641-11189','105191-5708-13169','105226-3277-5178') -- ('471033-6602-1272','627889-3337-943','742569-3544-1274','968418-5370-10147')
order by contactid,LogDate asc --70955
--select contactid from temp_1 group by contactid having count(*) > 1


/* those statemants to combine collected comments as above
, x as ( SELECT contactid, comment = CONVERT(NVARCHAR(MAX), comment), r1 = ROW_NUMBER() OVER (PARTITION BY contactid ORDER BY LogDate desc) FROM temp_1 ),
  a AS ( SELECT contactid, comment, r1 FROM x WHERE r1 = 1 ),
  r AS ( SELECT contactid, comment, r1 FROM a WHERE r1 = 1
        UNION ALL
        SELECT x.contactid, r.comment + ' | ' + x.comment, x.r1 FROM x INNER JOIN r ON r.contactid = x.contactid AND x.r1 = r.r1 + 1 )
SELECT ContactName, comments = MAX(comment)
  FROM r
  GROUP BY ContactName 
  ORDER BY ContactName
  OPTION (MAXRECURSION 0);

*/

/*
select * from contact_comment where id > 5000 --or contact_id = 5604
-- delete from contact_comment where comment_content like 'Date: %'
select count(*) from contact_comment where comment_content like 'Date: %';
select c.first_name,c.last_name, cc.id,cc.contact_id,cc.user_id,cc.comment_timestamp,cc.comment_content,cc.insert_timestamp from contact_comment cc left join contact c on c.id = cc.contact_id where comment_content like 'Date: %' or contact_id = 56044

INSERT INTO contact_comment (contact_id, user_id, comment_content, insert_timestamp) VALUES ( 56044, -10, 'Date: TESTING', convert(varchar(20),current_timestamp, 120) )
insert into position_candidate_feedback(candidate_id,user_account_id,comment_body, feedback_timestamp, insert_timestamp, contact_method, related_status) values(194408, -10, 'ABC', '2015-10-16 12:07:10', '2015-10-16 12:07:10', 4, 1)
select count(*) from position_candidate_feedback

select now() + interval '20' second
SELECT SYSDATETIME()  
    ,SYSDATETIMEOFFSET()  
    ,SYSUTCDATETIME()  
    ,CURRENT_TIMESTAMP  
    ,GETDATE()  
    ,GETUTCDATE();
Select TO_CHAR(current_timestamp,'DD-MM-YY hh24:mi:SS') AS TIMESTAMP, TO_CHAR(current_timestamp+10/24/60/60,'DD-MM-YY hh24:mi:SS') AS TIMESTAMP_PLUS_10SEC
*/
