
-- contact
select convert(datetime, C.[date], 103) from ccon c
select date,convert(datetime, convert(varchar(100),C.[date]), 103) from ccon c

select 
       UC.userID, UC.firstName, UC.lastName
       ,c.*
from ccon c
left join bullhorn1.BH_UserContact UC ON UC.userID = c.BullhornID


                select iif(Cl.clientID is null, c.Bullhornid, Cl.clientID) as 'contact_id' --, c.BullhornID, UC.userID, concat(UC.firstName,' ',UC.lastName) as fullname, UC.email,UC.email2,UC.email3
                        , cast('-10' as int) as 'user_account_id'
                        , convert(datetime, C.[date], 103) as 'insert_timestamp'
                        , 'comment' as 'category'
                        , 'contact' as 'type'
                        , 'newcomment' as 'tags'
                        , Stuff('COMMENT: ' + char(10)
                                + Coalesce('Created Date: ' + NULLIF(convert(varchar,c.Date,120), '') + char(10), '')
                                + Coalesce('Commented by: ' + NULLIF(c.CommentsBy_emailaddressofuser, '') + char(10), '')
                                + Coalesce('Comments: ' + NULLIF(cast(c.comments as nvarchar(max)), '') + char(10), '')
                                + Coalesce('Document Files to be uploaded: ' + NULLIF(cast(c.DocumentFilestobeuploaded_ActualFilenamewithextension as nvarchar(max)), '') + char(10), '')
                        , 1, 0, '') as 'content'
                from ccon c
                left join bullhorn1.BH_UserContact UC ON UC.userID = c.BullhornID
                left join bullhorn1.BH_Client Cl on Cl.userID = UC.userID



-- candidate
select 
       UC.userID, UC.firstName, UC.lastName,UC.email,UC.email2,UC.email3
       ,c.*
from ccan c
left join bullhorn1.BH_UserContact UC ON UC.userID = c.BullhornID
where c.BullhornID in ('12238','12246','12244','12243','12240','12246','12224','12242','12241','12240','12239','12238','12224','12237','12236','12235','12232','12231','12230','12229','12227','12236','12239','12241','12242','12245','12247','12230','12231','12243','12235','12247','12229','12226','12225','12224')

select * from ccan
update ccan set EmailAddress = concat('candidate_',BullhornID,'@noemailaddress.co')  where EmailAddress = '0'


                select iif(can.candidateID is null, c.Bullhornid, can.candidateID) as 'candidateID' ,can.candidateID as external_id, c.BullhornID, UC.userID, concat(UC.firstName,' ',UC.lastName) as fullname, UC.email,UC.email2,UC.email3
                        , cast('-10' as int) as 'user_account_id'
                        , convert(datetime, C.[date], 103) as 'insert_timestamp'
                        , 'comment' as 'category'
                        , 'candidate' as 'type'
                        , 'newcomment' as 'tags'
                        , Stuff('COMMENT: ' + char(10)
                                + Coalesce('Created Date: ' + NULLIF(convert(varchar,c.Date,120), '') + char(10), '')
                                + Coalesce('Commented by: ' + NULLIF(c.CommentsBy_emailaddressofuser, '') + char(10), '')
                                + Coalesce('Comments: ' + NULLIF(cast(c.comments as nvarchar(max)), '') + char(10), '')
                        , 1, 0, '') as 'content'
                from ccan c
                left join bullhorn1.BH_UserContact UC ON UC.userID = c.BullhornID
                left join bullhorn1.Candidate can on can.userID = UC.userID
                