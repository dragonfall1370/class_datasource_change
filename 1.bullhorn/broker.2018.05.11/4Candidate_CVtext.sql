
-- 1. Create temporary table
alter table bullhorn1.BH_UserWork add description_truong varchar(max);


-- 2. Conversation
select userid, description,
                                        [dbo].[fn_ConvertHTMLToText](
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( cast(description as varchar(max))
                                                ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                                                ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                                                ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                                                ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                                                ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                                                ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'') 
                                        )as description
                                        from bullhorn1.BH_UserWork
where userid in (40942,40943,40944,40945,40946,40947,40948,40949,40950,40951,40952)

                                      
-- 3. Update with readable text
update bullhorn1.BH_UserWork set description_truong = [dbo].[fn_ConvertHTMLToText](
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( cast(description as varchar(max))
                                                ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                                                ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                                                ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                                                ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                                                ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                                                ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'') 
                                        )
                                        where userid in (9,11,13)                                                                     

select count(*) from bullhorn1.BH_UserWork where description_truong <> ''

/*                                                                                                                                    
       select top 100
              userid, description
              , [dbo].[fn_ConvertHTMLToText](description)
       -- select count(*) --10951
       from bullhorn1.BH_UserWork
       where userid in (28) or description like '%DIV style%' 
union all
       select top 100
              userid, description 
              , [dbo].[fn_ConvertHTMLToText](description)
       -- select count(*) --105868
       from bullhorn1.BH_UserContact
       where userid in (28)
*/