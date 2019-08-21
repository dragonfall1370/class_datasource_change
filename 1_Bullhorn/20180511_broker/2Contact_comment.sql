
------------
-- COMMENT - INJECT TO VINCERE
/*CREATE SEQUENCE seq_contact START WITH 10000 INCREMENT BY 1;
alter table contact_comment alter column id set default nextval('seq_contact');
commit;
select id,external_id,first_name,last_name from contact where external_id in ('2076','2083','1812','2037','2904','531','2439')
select * from contact_comment where contact_id = 50908
INSERT INTO contact_comment (contact_id, user_id, comment_content, insert_timestamp) VALUES ( 64167, -10, 'TESTING', '2019-01-01 00:00:00' )
*/
------------
with comments as (
                select Cl.clientID as 'contact_id' --, UC.userID, concat(UC1.firstName,' ',UC1.lastName) as fullname 
                        , cast('-10' as int) as user_account_id
                        , U.dateAdded as 'insert_timestamp'
                        , 'comment' as 'category'
                        , 'contact' as 'type'
                        , Stuff('COMMENT: ' + char(10)
                                + Coalesce('Created Date: ' + NULLIF(convert(varchar,U.dateAdded,120), '') + char(10), '')
                                + Coalesce('Commented by: ' + NULLIF(U.name, '') + char(10), '')
                                + Coalesce('Action: ' + NULLIF(UC.action, '') + char(10), '')
                                + Coalesce('Comments: ' + NULLIF(cast(UC.comments as nvarchar(max)), '') + char(10), '')
                        , 1, 0, '') as 'content'
                -- select top 100 *
                from bullhorn1.BH_UserComment UC
                left join bullhorn1.BH_Client Cl on Cl.userID = UC.userID
                left join bullhorn1.BH_UserContact UC1 ON Cl.userID = UC1.userID
                left join bullhorn1.BH_User U on U.userID = UC.commentingUserID
                where Cl.isPrimaryOwner = 1)

select count(*) from comments where contact_id is not null
--select * from comments where contact_id is not null and contact_id in (4054,7102) --538216 > 563579


/* Group comments by ContactID (option 2)
, y as ( SELECT userID
        ,concat(concat('Date added: ',convert(varchar(10),dateAdded,120),' || ')
        ,iif(action = '' or action is null,'',concat('ACTION: ',action,' || '))
        ,concat('Comments: ',CONVERT(NVARCHAR(MAX), comments))
        ) as  comments
        FROM bullhorn1.BH_UserComment )

, x as ( SELECT userID, comments, r1 = ROW_NUMBER() OVER (PARTITION BY userID ORDER BY comments) FROM y)
, a AS (  SELECT userID, comments, r1 FROM x WHERE r1 = 1)
, r AS (  SELECT userID, comments, r1 FROM a WHERE r1 = 1 UNION ALL SELECT x.userID, r.comments + char(10) + x.comments, x.r1 FROM x INNER JOIN r ON r.userID = x.userID AND x.r1 = r.r1 + 1 )
--SELECT userID, comments = MAX(comments) FROM r GROUP BY userID ORDER BY userID OPTION (MAXRECURSION 0)

, comments (userID, comments) as (SELECT userID, comments = MAX(comments) FROM r GROUP BY userID)
select top 10 *,len(comments) as '(length-contact-comment)' from comments
*/
