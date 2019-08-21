

-- 1. Create temporary table
alter table WorkHistory add WHDuties_ varchar(max);

-- 3. Update with readable text
update WorkHistory set WHDuties_ = [dbo].[fn_ConvertHTMLToText](
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( WHDuties
                                                ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                                                ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                                                ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                                                ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                                                ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                                                ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'') 
                                        )
