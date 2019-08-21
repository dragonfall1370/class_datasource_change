with 
t as (
       select 
                 UC1.clientCorporationID
               , UC.dateAdded , author.name as 'authorname', author.email as 'authoremail'--author.userID, 
               , about.name as 'aboutname', about.email as 'aboutemail'--, about.userID 
               , UC.action, UC.comments
       from bullhorn1.BH_UserComment UC
       left join (select * from bullhorn1.BH_UserContact) author ON author.userID = UC.commentingUserID
       left join (select * from bullhorn1.BH_UserContact) about ON about.userID = UC.userID
       --left join bullhorn1.BH_ClientCorporation on Cl.userID = UC.userID
       --left join bullhorn1.BH_Client Cl on Cl.userID = UC.userID
       left join bullhorn1.BH_UserContact UC1 ON UC1.userID = UC.userID
       where UC1.clientCorporationID is not null
       --and UC.dateAdded between '2017-09-01' AND '2017-11-01' --<<<<<
       --and (UC.dateAdded < '2017-09-01' or '2017-11-01' < UC.dateAdded ) --<<<<<
       /*order by UC.dateAdded desc 
       and UC1.clientCorporationID = 4666 
       and UC.comments like 'Can you put%'  
       or UC1.userid in (98297,180534) or U.userid in (98297,180534) */
       )

--select count(*) from t
select
             clientCorporationID as external_id
          , cast('-10' as int) as 'user_account_id'
          , 'comment' as category
          , 'company' as type	
          , dateAdded as insert_timestamp
         ,  [dbo].[fn_ConvertHTMLToText](
              Stuff(  Coalesce('Author Name: ' + nullif(convert(varchar(max),authorname), '') + char(10), '')
                        + Coalesce('Author Email: ' + nullif(convert(varchar(max),authoremail), '') + char(10), '')
                        + Coalesce('Author Name: ' + nullif(convert(varchar(max),aboutname), '') + char(10), '')
                        + Coalesce('About Email: ' + nullif(convert(varchar(max),aboutemail), '') + char(10), '')
                        + Coalesce('Action: ' + nullif(convert(varchar(max),[action]), '') + char(10), '')
                        + Coalesce('Comments: ' + nullif(convert(varchar(max),comments), '') + char(10), '')
              , 1, 0, '')) as content
        -- select  top 10 *
from t

