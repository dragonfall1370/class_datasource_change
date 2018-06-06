select * from INT_UncompressedMessages
select * from bullhorn1.BH_UserMessage

alter table bullhorn1.BH_UserMessage add comment varchar(max);                

update bullhorn1.BH_UserMessage set comment = [dbo].[fn_ConvertHTMLToText](
                      replace(replace(replace(replace(replace(replace(convert(varchar(max),t.commentsUTF8)
                      ,'Â','')
                      ,'Â·','')
                      ,'v\:* {behavior:url(#default#VML);}','')
                      ,'o\:* {behavior:url(#default#VML);}','')
                      ,'w\:* {behavior:url(#default#VML);}','')
                      ,'.shape {behavior:url(#default#VML);}',''))
        from  (select userMessageID,commentsUTF8 from INT_UncompressedMessages) t where t.userMessageID = bullhorn1.BH_UserMessage.userMessageID
        and t.userMessageID = 39
        