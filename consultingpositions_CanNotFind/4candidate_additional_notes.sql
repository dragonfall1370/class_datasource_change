
       select id, firstname, lastname
       ,'Experience' as title
       , REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( [dbo].[fn_ConvertHTMLToText](l.Experience__c)
                                        ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                                        ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                                        ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                                        ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                                        ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                                        ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'') as 'note'       
        from Lead l
       where l.Experience__c <> '' and id in ('00QC000001BH6mgMAD')
UNION ALL
       select id, firstname, lastname
       ,'Chronology' as title
       , REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( [dbo].[fn_ConvertHTMLToText](l.Chronology__c)
                                        ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                                        ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                                        ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                                        ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                                        ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                                        ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'') as 'note'          
        from Lead l
       where l.Chronology__c <> '' and id in ('00QC000001BH6mgMAD')
UNION ALL
       select id, firstname, lastname
       ,'Introduction' as title
       , REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( [dbo].[fn_ConvertHTMLToText](l.Introduction__c)
                                        ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                                        ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                                        ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                                        ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                                        ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                                        ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'') as 'note'          
        from Lead l
       where l.Introduction__c <> '' and id in ('00QC000001BH6mgMAD')