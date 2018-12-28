with EmailBody as (
        select ca.candidateid, ca.userid,ca.recruiteruserid,ca.firstname,ca.lastname,ca.companyname,
                um.userMessageID,um.sendingUserID,um.subject,um.externalFrom, convert(nvarchar(max),um.comments) as comments ,convert(nvarchar(max),um.externalTo) as externalTo, convert(nvarchar(max),um.externalCC) as externalCC, convert(nvarchar(max),um.externalBCC) as externalBCC, um.SMTPSendDate
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
/*                SELECT --top 3000
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
                          from  bullhorn1.BH_UserMessageFile
                          WHERE userMessageID = a.userMessageID
                                  --order by userMessageID desc
                          FOR XML PATH (''), TYPE).value('.', 'nvarchar(64)')
                          , 1, 4, '')  AS att
                FROM bullhorn1.BH_UserMessageFile as a
                GROUP BY a.userMessageID        
*/
      SELECT --top 6000
                     userMessageID,
                     STUFF(
                         (SELECT  ', ' 
                                --+ cast(name as varchar(64)) 
                                + cast(REPLACE(name,char(0),'') as nvarchar(64))
                          from  bullhorn1.BH_UserMessageFile
                          WHERE userMessageID = a.userMessageID
                          --FOR XML PATH (''), TYPE).value('.', 'nvarchar(64)'), 1, 4, '')  AS att
                          --FOR XML PATH(''), TYPE).value('(./text())[1]', 'VARCHAR(max)'), 1, 2, '') AS att
                          FOR XML path('')), 1, 1, '') AS att
                FROM bullhorn1.BH_UserMessageFile as a
                GROUP BY a.userMessageID     
                
        ) umf on umf.userMessageID = um.userMessageID
        left join bullhorn1.Candidate ca on ca.userid = um.sendingUserID
        where um.isSenderDeleted = 0 and ca.userid is not null )
        --and convert(nvarchar(max),um.comments) <> '' ) 
        --and ca.candidateid = 565


--select count(*) from EmailBody --143774
--select top 1000 * from EmailBody

select --top 200
        candidateid as 'candidate_id'
        , firstname
        , lastname
        , userid
        , recruiteruserid
        , companyname
        , userMessageID
        , sendingUserID
        , cast('-10' as int) as 'user_account_id'
        , cast('4' as int) as 'contact_method'
        , cast('1' as int) as 'related_status'
        , coalesce(NULLIF(convert(varchar(19), SMTPSendDate, 120),''), CURRENT_TIMESTAMP) as 'feedback_timestamp_insert_timestamp'
        , concat(  'From: ' , convert(nvarchar(max), externalFrom, 120) , char(10) ,
                   'To: ' , convert(nvarchar(max), externalTo, 120) , char(10) ,
                   'CC: ' , convert(nvarchar(max), externalCC, 120) , char(10) ,
                   'BCC: ' , convert(nvarchar(max), externalBCC, 120) , char(10) ,
                   'Subject: ' , convert(nvarchar(max), subject, 120) , char(10) ,
                   'Content: ' , cast(comments as varchar(max)) , char(10) ,
                   --coalesce('Comments: ' + NULLIF(cast(comments as varchar(max)) + char(10),''), ''),
                   'Attachments: ' , convert(nvarchar(64),att) --, char(10) 
                   --'Directory: ' , convert(nvarchar(max),directory)
          ) as comment_body
        --, umf.name             --ATTACHMENTS
        --, umf.directory        -- PATHS STORE ATTACHMENTS
from EmailBody
--where comments <> ''
where candidateID = 1