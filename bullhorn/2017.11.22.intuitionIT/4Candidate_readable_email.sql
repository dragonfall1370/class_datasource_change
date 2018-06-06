
--select count(*) from bullhorn1.BH_UserMessage um --594.669
--select top 300 *  from bullhorn1.BH_UserMessage um

-- select count(*) from INT_UncompressedMessages
--select top 100 * from INT_UncompressedMessages

select top 100 
       userMessageID
       ,commentsUTF8
       --,[dbo].[udf_StripHTML]( convert(varchar(max),commentsUTF8)  ) as ok
       --, [dbo].[fn_ConvertHTMLToText]( convert(nvarchar(max),commentsUTF8) ) as commentsUTF8_1
       , [dbo].[fn_ConvertHTMLToText]( convert(varchar(max),commentsUTF8) ) as commentsUTF8_OK
       --, [dbo].[fn_ConvertHTMLToText]( cast(commentsUTF8 as varchar(max)) ) as commentsUTF8_3
       --, dbo.UTF8_TO_NVARCHAR([dbo].[fn_ConvertHTMLToText_truong]( commentsUTF8 )) as commentsUTF8_2
       --, [dbo].[fn_ConvertHTMLToText]( dbo.UTF8_TO_NVARCHAR(convert(varchar(max),commentsUTF8)) ) as commentsUTF8_2
       --, [dbo].[fn_ConvertHTMLToText]( dbo.UTF8_TO_NVARCHAR(commentsUTF8) ) as commentsUTF8_3
       --, [dbo].[fn_ConvertHTMLToText]( convert(varchar(max),commentsUTF8) COLLATE Latin1_General_BIN) as commentsUTF8_3
       --, [dbo].[fn_ConvertHTMLToText]([dbo].[udf_StripHTML](commentsUTF8)) as commentsUTF8_2
      --,commentsString
   --    , dbo.UTF8_TO_NVARCHAR(commentsString) as test
--, [dbo].[fn_ConvertHTMLToText_truong]( dbo.UTF8_TO_NVARCHAR(commentsString) ) as commentsString_1       
       --, [dbo].[fn_ConvertHTMLToText_truong]( convert(varchar(max),commentsString) ) as commentsString_1       
       --, [dbo].[fn_ConvertHTMLToText_truong]([dbo].[udf_StripHTML](commentsString)) as commentsString_2         
      --, [dbo].[fn_ConvertHTMLToText_truong](commentsString) as commentsString              
-- select *
from INT_UncompressedMessages
where userMessageID in (218,35,46)      


select top 1000
       userMessageID
       --,commentsUTF8     
--       ,replace([dbo].[fn_ConvertHTMLToText]( convert(varchar(max),commentsUTF8) ),'Â ','') as new_commentsUTF8
       --,replace([dbo].[fn_ConvertHTMLToText](commentsUTF8),'Â ','') as new_commentsUTF8
       ,[dbo].[fn_ConvertHTMLToText](
              replace(replace(replace(replace(replace(replace(commentsUTF8
              ,'Â','')
              ,'Â·','')
              ,'v\:* {behavior:url(#default#VML);}','')
              ,'o\:* {behavior:url(#default#VML);}','')
              ,'w\:* {behavior:url(#default#VML);}','')
              ,'.shape {behavior:url(#default#VML);}','')
       ) as new_commentsUTF8
       --,commentsString
--       , [dbo].[fn_ConvertHTMLToText]( convert(varchar(max),commentsString) ) as new_commentsString
       --, [dbo].[fn_ConvertHTMLToText](commentsString) as new_commentsString
from INT_UncompressedMessages
where userMessageID in (897,218,35,46)      
 
