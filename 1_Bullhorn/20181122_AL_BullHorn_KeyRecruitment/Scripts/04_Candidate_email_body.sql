
-- EMAIL BODY

with EmailBody as (
        -- RECEIVER
        select ca.candidateid, ca.userid,ca.recruiteruserid,ca.firstname,ca.lastname,ca.companyname
                --um.userMessageID,um.sendingUserID,um.subject, um.externalFrom, convert(nvarchar(max),um.comments) as comments , convert(nvarchar(max),um.externalTo) as externalTo, convert(nvarchar(max),um.externalCC) as externalCC, convert(nvarchar(max),um.externalBCC) as externalBCC, um.SMTPSendDate
                , um.userMessageID,um.sendingUserID,um.subject, um.externalFrom
                , convert(varchar(max),um.comments) as comments
                ,[dbo].[fn_ConvertHTMLToText](
                      replace(replace(replace(replace(replace(replace( convert(varchar(max),um.email_content)
                      ,'Â','')
                      ,'Â·','')
                      ,'v\:* {behavior:url(#default#VML);}','')
                      ,'o\:* {behavior:url(#default#VML);}','')
                      ,'w\:* {behavior:url(#default#VML);}','')
                      ,'.shape {behavior:url(#default#VML);}','')
                ) as email_content             
                , convert(nvarchar(max),um.externalTo) as externalTo, convert(nvarchar(max),um.externalCC) as externalCC, convert(nvarchar(max),um.externalBCC) as externalBCC, um.SMTPSendDate
                --mr.userID,mr.folder,mr.recipientType, mr.email, mr.sendingUserID, mr.subjectSort
                --,mr.userID,mr.folder,mr.recipientType, mr.email, mr.externalFrom
                , umf.att --,umf.name, umf.directory
        -- select count(*) -- select top 2000 *
        from bullhorn1.BH_UserMessage um
        --left join INT_UncompressedMessages cont on cont.userMessageID = um.userMessageID
        left join bullhorn1.BH_MessageRecipient mr on mr.userMessageID = um.userMessageID
        --left join bullhorn1.BH_UserMessageFile umf on umf.userMessageID = um.userMessageID
        --left join (SELECT userMessageID, STUFF((SELECT ', ' + [dbo].[fn_ConvertHTMLToText](name) from bullhorn1.BH_UserMessageFile WHERE userMessageID = a.userMessageID order by name asc FOR XML PATH (''), TYPE).value('.', 'nvarchar(64)'), 1, 1, '') AS att FROM bullhorn1.BH_UserMessageFile AS a GROUP BY a.userMessageID ) umf on umf.userMessageID = um.userMessageID
        left join
        (
                SELECT --top 1000
                     userMessageID,
                     STUFF(
                         (SELECT [dbo].[ufn_RemoveForXMLUnsupportedCharacters](name)
                        --(SELECT [dbo].[fn_ConvertHTMLToText](name)
                          from  bullhorn1.BH_UserMessageFile
                          WHERE userMessageID = a.userMessageID
                                  --order by userMessageID desc
                          FOR XML PATH (''), TYPE).value('.', 'nvarchar(64)')
                          , 1, 4, '')  AS att
                FROM bullhorn1.BH_UserMessageFile as a
                GROUP BY a.userMessageID        
        ) umf on umf.userMessageID = um.userMessageID        
        left join bullhorn1.Candidate ca on ca.userid = mr.userid
        where mr.isDeleted = 0 and mr.isSpam = 0 and ca.userid is not null
        --and um.comments is not null and convert(varchar(max),um.comments) <> '' 
        --and ca.candidateid = 565        
        --and convert(nvarchar(max),um.externalCC) != '' and um.externalCC is not null and convert(nvarchar(max),um.externalBCC) != '' and um.externalBCC is not null
UNION
        -- SENDER
        select ca.candidateid, ca.userid, ca.recruiteruserid, ca.firstname, ca.lastname, ca.companyname
--                um.userMessageID,um.sendingUserID,um.subject,um.externalFrom, convert(nvarchar(max),um.comments) as comments ,convert(nvarchar(max),um.externalTo) as externalTo, convert(nvarchar(max),um.externalCC) as externalCC, convert(nvarchar(max),um.externalBCC) as externalBCC, um.SMTPSendDate
                , um.userMessageID, um.sendingUserID, um.subject, um.externalFrom
                , convert(varchar(max),um.comments) as comments
                ,[dbo].[fn_ConvertHTMLToText](
                      replace(replace(replace(replace(replace(replace(convert(varchar(max), um.email_content)
                      ,'Â','')
                      ,'Â·','')
                      ,'v\:* {behavior:url(#default#VML);}','')
                      ,'o\:* {behavior:url(#default#VML);}','')
                      ,'w\:* {behavior:url(#default#VML);}','')
                      ,'.shape {behavior:url(#default#VML);}','')
                ) as email_content
                , convert(nvarchar(max),um.externalTo) as externalTo, convert(nvarchar(max),um.externalCC) as externalCC, convert(nvarchar(max),um.externalBCC) as externalBCC, um.SMTPSendDate
                --mr.userID,mr.folder,mr.recipientType, mr.email, mr.sendingUserID, mr.subjectSort
                --,mr.userID,mr.folder,mr.recipientType, mr.email, mr.externalFrom
                , umf.att --,umf.name, umf.directory
        -- select count(*)
        from bullhorn1.BH_UserMessage um
        --left join INT_UncompressedMessages cont on cont.userMessageID = um.userMessageID
        left join bullhorn1.BH_MessageRecipient mr on mr.userMessageID = um.userMessageID
        --left join bullhorn1.BH_UserMessageFile umf on umf.userMessageID = um.userMessageID
        --left join (SELECT userMessageID, STUFF((SELECT ', ' + name from bullhorn1.BH_UserMessageFile WHERE userMessageID = a.userMessageID order by name asc FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS att FROM bullhorn1.BH_UserMessageFile AS a GROUP BY a.userMessageID ) umf on umf.userMessageID = um.userMessageID
        left join
        (
                SELECT --top 1000
                     userMessageID,
                     STUFF(
                         (SELECT [dbo].[ufn_RemoveForXMLUnsupportedCharacters](name)
                        --(SELECT [dbo].[fn_ConvertHTMLToText](name)
                          from  bullhorn1.BH_UserMessageFile
                          WHERE userMessageID = a.userMessageID
                                  --order by userMessageID desc
                          FOR XML PATH (''), TYPE).value('.', 'nvarchar(64)')
                          , 1, 4, '')  AS att
                FROM bullhorn1.BH_UserMessageFile as a
                GROUP BY a.userMessageID        
        ) umf on umf.userMessageID = um.userMessageID        
        left join bullhorn1.Candidate ca on ca.userid = um.sendingUserID
        where um.isSenderDeleted = 0 and ca.userid is not null )
        --and convert(nvarchar(max),um.comments) <> '' ) 
        --and ca.candidateid = 565


--select count(*) from EmailBody --46306
-- select top 100 * from EmailBody where candidateid between 124602 and 128601
/* select candidateid, concat('insert into position_candidate_feedback (candidate_id, user_account_id, feedback_timestamp, comment_body, feedback_score, insert_timestamp, contact_method, json_relate_info, related_status) values ( 
        (select id from candidate where external_id = ''',candidateid,'''), ',-10,', now(), ''',replace(concat('Date Created: ',convert(varchar,SMTPSendDate,120),char(10),'From: ',externalFrom,char(10),'To: ',externalTo,char(10),'CC: ',externalCC,char(10),'Subject: ',subject,char(10),char(10),'Content: ',comments),'''',''''''),''', 0, now(), 4, ''[]'', 1);') as EmailSQL
from EmailBody --where candidateid = 33
order by SMTPSendDate desc */


select --top 200
          userid
        , concat(firstname, ' ',lastname) as fullname
        , recruiteruserid
        , companyname
        , userMessageID
        , sendingUserID
        , candidateid as 'candidate_id'
        , cast('-10' as int) as 'user_account_id'
        , 'comment' as category
        , 'candidate' as type
        , coalesce(NULLIF(convert(varchar(19), SMTPSendDate, 120),''), CURRENT_TIMESTAMP) as 'insert_timestamp'
         , Stuff('EMAIL: ' + char(10)
                + Coalesce('From: ' + NULLIF(convert(varchar(max),externalFrom), '') + char(10), '')
                + Coalesce('To: ' + NULLIF(convert(varchar(max),externalTo), '') + char(10), '')
                + Coalesce('CC: ' + NULLIF(convert(varchar(max),externalCC), '') + char(10), '')
                + Coalesce('BCC: ' + NULLIF(convert(varchar(max),externalBCC), '') + char(10), '')
                + Coalesce('Subject: ' + NULLIF(convert(varchar(max),subject), '') + char(10), '')
                + Coalesce('Body: ' + NULLIF(convert(varchar(max),email_content), '') + char(10), 'Body: ' + char(10))
                + Coalesce('Comments: ' + NULLIF(convert(varchar(max),comments), '') + char(10), '')
                + Coalesce('Attachments: ' + NULLIF(convert(varchar(max),att), '') + char(10), '')
        , 1, 0, '') as 'content' 
        --, umf.name             --ATTACHMENTS
        --, umf.directory        -- PATHS STORE ATTACHMENTS
from EmailBody
--where comments <> ''
--where candidateID = 10001

