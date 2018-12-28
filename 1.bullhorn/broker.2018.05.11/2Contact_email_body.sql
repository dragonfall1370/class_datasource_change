
 -- EMAIL BODY

with EmailBody as (
        -- RECEIVER
        select ca.clientID, ca.userid
                ,um.userMessageID,um.sendingUserID,um.subject, um.externalFrom
                , [dbo].[fn_ConvertHTMLToText](um.comments) as comments
                , convert(nvarchar(max),um.externalTo) as externalTo, convert(nvarchar(max),um.externalCC) as externalCC, convert(nvarchar(max),um.externalBCC) as externalBCC, um.SMTPSendDate
                --mr.userID,mr.folder,mr.recipientType, mr.email, mr.sendingUserID, mr.subjectSort
                --,mr.userID,mr.folder,mr.recipientType, mr.email, mr.externalFrom
                ,umf.att --,umf.name, umf.directory
        -- select count(*) -- select top 200 *
        from bullhorn1.BH_UserMessage um
        left join bullhorn1.BH_MessageRecipient mr on mr.userMessageID = um.userMessageID
        --left join bullhorn1.BH_UserMessageFile umf on umf.userMessageID = um.userMessageID
        --left join (SELECT userMessageID, STUFF((SELECT ', ' + name from bullhorn1.BH_UserMessageFile WHERE userMessageID = a.userMessageID order by name asc FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS att FROM bullhorn1.BH_UserMessageFile AS a GROUP BY a.userMessageID ) umf on umf.userMessageID = um.userMessageID
        left join
        (
                SELECT --top 100
                     userMessageID,
                     STUFF(
                         (SELECT REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( cast(name as nvarchar(64))
                                        ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                                        ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                                        ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                                        ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                                        ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                                        ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'')
                          --(SELECT [dbo].[fn_ConvertHTMLToText](name)
                          from  bullhorn1.BH_UserMessageFile
                          WHERE userMessageID = a.userMessageID
                                  --order by userMessageID desc
                          FOR XML PATH (''), TYPE).value('.', 'nvarchar(64)')
                          , 1, 4, '')  AS att
                FROM bullhorn1.BH_UserMessageFile as a
                GROUP BY a.userMessageID        
        )  umf on umf.userMessageID = um.userMessageID        
        left join bullhorn1.BH_Client ca on ca.userid = mr.userid
        where mr.isDeleted = 0 and mr.isSpam = 0 and ca.userid is not null --5893
        --and um.comments is not null and convert(varchar(max),um.comments) <> '' 
        --and ca.clientID in (4054)     
        --and convert(nvarchar(max),um.externalCC) != '' and um.externalCC is not null and convert(nvarchar(max),um.externalBCC) != '' and um.externalBCC is not null
UNION
        -- SENDER
        select ca.clientID, ca.userid
                ,um.userMessageID,um.sendingUserID,um.subject,um.externalFrom
                , [dbo].[fn_ConvertHTMLToText](um.comments) as comments
                ,convert(nvarchar(max),um.externalTo) as externalTo, convert(nvarchar(max),um.externalCC) as externalCC, convert(nvarchar(max),um.externalBCC) as externalBCC, um.SMTPSendDate
                --mr.userID,mr.folder,mr.recipientType, mr.email, mr.sendingUserID, mr.subjectSort
                --,mr.userID,mr.folder,mr.recipientType, mr.email, mr.externalFrom
                ,umf.att --,umf.name, umf.directory
        -- select count(*)
        from bullhorn1.BH_UserMessage um
        left join bullhorn1.BH_MessageRecipient mr on mr.userMessageID = um.userMessageID
        --left join bullhorn1.BH_UserMessageFile umf on umf.userMessageID = um.userMessageID
        --left join (SELECT userMessageID, STUFF((SELECT ', ' + name from bullhorn1.BH_UserMessageFile WHERE userMessageID = a.userMessageID order by name asc FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS att FROM bullhorn1.BH_UserMessageFile AS a GROUP BY a.userMessageID ) umf on umf.userMessageID = um.userMessageID
        left join
        (
                SELECT --top 100
                     userMessageID,
                     STUFF(
                         (SELECT REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( cast(name as nvarchar(64))
                                        ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                                        ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                                        ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                                        ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                                        ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                                        ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'')
                          --(SELECT [dbo].[fn_ConvertHTMLToText](name)
                          from  bullhorn1.BH_UserMessageFile
                          WHERE userMessageID = a.userMessageID
                                  --order by userMessageID desc
                          FOR XML PATH (''), TYPE).value('.', 'nvarchar(64)')
                          , 1, 4, '')  AS att
                FROM bullhorn1.BH_UserMessageFile as a
                GROUP BY a.userMessageID        
        )  umf on umf.userMessageID = um.userMessageID
        left join bullhorn1.BH_Client ca on ca.userid = um.sendingUserID
        where um.isSenderDeleted = 0 and ca.userid is not null
        --and convert(nvarchar(max),um.comments) <> ''
        --and ca.clientID in (4054)
)

--select count(*) from EmailBody --344458
--select top 100 * from EmailBody where candidateid between 124602 and 128601 -> 25914
/*select candidateid, concat('insert into position_candidate_feedback (candidate_id, user_account_id, feedback_timestamp, comment_body, feedback_score, insert_timestamp, contact_method, json_relate_info, related_status) values ( 
        (select id from candidate where external_id = ''',candidateid,'''), ',-10,', now(), ''',replace(concat('Date Created: ',convert(varchar,SMTPSendDate,120),char(10),'From: ',externalFrom,char(10),'To: ',externalTo,char(10),'CC: ',externalCC,char(10),'Subject: ',subject,char(10),char(10),'Content: ',comments),'''',''''''),''', 0, now(), 4, ''[]'', 1);') as EmailSQL
from EmailBody --where candidateid = 33
order by SMTPSendDate desc
*/



select --top 200
        clientID as 'contact_id'
        , userid
        , cast('-10' as int) as 'user_account_id'
        , 'comment' as category
        , 'contact' as type
        , coalesce(NULLIF(convert(varchar(19), SMTPSendDate, 120),''), CURRENT_TIMESTAMP) as 'insert_timestamp'
        /*, concat('From: ' , convert(nvarchar(max), externalFrom, 120) , char(10) ,
                   'To: ' , convert(nvarchar(max), externalTo, 120) , char(10) ,
                   'CC: ' , convert(nvarchar(max), externalCC, 120) , char(10) ,
                   'BCC: ' , convert(nvarchar(max), externalBCC, 120) , char(10) ,
                   'Subject: ' , convert(nvarchar(max), subject, 120) , char(10) ,
                   'Content: ' , cast(comments as varchar(max)) , char(10) ,
                   --coalesce('Comments: ' + NULLIF(cast(comments as varchar(max)) + char(10),''), ''),
                   'Attachments: ' , convert(nvarchar(max),att) --, char(10) 
                   --'Directory: ' , convert(nvarchar(max),directory)
          ) as content */
         , Stuff('EMAIL: ' + char(10)
                + Coalesce('From: ' + NULLIF(convert(varchar(max),externalFrom), '') + char(10), '')
                + Coalesce('To: ' + NULLIF(convert(varchar(max),externalTo), '') + char(10), '')
                + Coalesce('CC: ' + NULLIF(convert(varchar(max),externalCC), '') + char(10), '')
                + Coalesce('BCC: ' + NULLIF(convert(varchar(max),externalBCC), '') + char(10), '')
                + Coalesce('Subject: ' + NULLIF(convert(varchar(max),subject), '') + char(10), '')
                + Coalesce('Content: ' + NULLIF(convert(varchar(max),comments), '') + char(10), 'Content: ' + char(10))
                + Coalesce('Attachments: ' + NULLIF(convert(varchar(max),att), '') + char(10), '')
        , 1, 0, '') as 'content '
        --, umf.name             --ATTACHMENTS
        --, umf.directory        -- PATHS STORE ATTACHMENTS
from EmailBody
--where comments <> ''
--where candidateID = 2001
--where clientID in (4054)
    
    