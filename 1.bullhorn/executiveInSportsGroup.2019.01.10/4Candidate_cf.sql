

with comments as (
        SELECT --top 1000
                  C.candidateID, UC.Userid, C.fullname
                , UC.dateAdded
                        , Stuff(--'COMMENT: ' + char(10)
                                 Coalesce('Created Date: ' + NULLIF(convert(varchar,UC.dateAdded,120), '') + char(10), '')
                                + Coalesce('Author: ' + NULLIF(U.name, '') + char(10), '')
                                + Coalesce('Action: ' + NULLIF(UC.action, '') + char(10), '')
                                + Coalesce('Comments: ' + NULLIF( 
                                          replace(replace(replace(replace(replace(replace( [dbo].[fn_ConvertHTMLToText](UC.comments)
                                          ,'Â','')
                                          ,'Â·','')
                                          ,'v\:* {behavior:url(#default#VML);}','')
                                          ,'o\:* {behavior:url(#default#VML);}','')
                                          ,'w\:* {behavior:url(#default#VML);}','')
                                          ,'.shape {behavior:url(#default#VML);}','') , '') + char(10), '')
                        , 1, 0, '') as 'content'
        -- select count(*) --12292
        -- select top 100 *
        from bullhorn1.BH_UserComment UC
        left join ( select candidateID, concat(FirstName,' ',LastName) as fullname, userid, isPrimaryOwner from bullhorn1.Candidate) C on C.userID = UC.Userid 
        left join bullhorn1.BH_User U on U.userID = UC.commentingUserID
        where C.isPrimaryOwner = 1 and C.candidateID is not null --and cast(UC.comments as nvarchar(max)) <> ''
)

, comments2 /*(clientCorporationID,ResumeId)*/ as (
        SELECT candidateID
                     , 
                     
                     STUFF((SELECT '---------------------------------' + char(10) + 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE(
                     content
                     ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                     ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                     ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                     ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                     ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                     ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'') as content                 
                      from comments WHERE candidateID = a.candidateID order by dateAdded desc FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 0, '') 
 as comment 
        FROM (select candidateID from comments) as a GROUP BY a.candidateID )
--select * from comments2 where candidateID = 1
        
-- FORM
SELECT --top 100
         candidateID as additional_id --, userid, fullname 
        , 'add_cand_info' as additional_type
        , 1005 as form_id
        , 1015 as field_id
        , [dbo].[fn_ConvertHTMLToText](comment) as field_value
        , 1015 as constraint_id
from comments2 where comment <> '' 
--and candidateID = 1