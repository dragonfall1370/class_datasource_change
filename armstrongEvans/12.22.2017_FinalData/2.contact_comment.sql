
with comment (Contacts,date,comment) as (
	select
	  j.Contacts
	, j.Date as 'date'
	, Stuff(          Coalesce('Date: ' + NULLIF(convert(varchar(10),j.Date,120), '') + char(10), '')
                        + Coalesce('Subject: ' + NULLIF(cast(j.Subject as varchar(max)), '') + char(10), '')
                        +
                         REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE(  Coalesce('Body: ' + NULLIF(cast(j.Body as varchar(max)), '') + char(10), '') 
                                        ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                                        ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                                        ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                                        ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                                        ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                                        ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'')
                        + Coalesce('Type: ' + NULLIF(cast(j.Type as varchar(max)), '') + char(10), '')
                        + Coalesce('Consultant: ' + NULLIF(cast(Consultant as varchar(max)), '') + char(10), '')
                        + Coalesce('Company Name: ' + NULLIF(cast(c.company_name as varchar(max)), '') + char(10), '')
                        + Coalesce('Contact Name: ' + NULLIF(cast(con.fullname as varchar(max)), '') + char(10), '')
                        + Coalesce('Job Title: ' + NULLIF(cast(con.contact_jobTitle as varchar(max)), '') + char(10), '')
                , 1, 0, '') as comment
        --  select top 1000 * 
        from Journals j
        left join CompanyImportAutomappingTemplate c on cast(c.company_externalid as varchar(max))= cast(j.Clients as varchar(max))
        left join (select contact_externalId, concat(contact_firstName,' ',contact_lastName) as fullname,contact_jobTitle from ContactsImportAutomappingTemplate) con on cast(con.contact_externalId as varchar(max)) = cast(j.Contacts as varchar(max))
        --where cast(Contacts as varchar(max)) <> ''
        where (cast(j.Contacts as varchar(max)) <> '' and cast(j.Contacts as varchar(max)) not LIKE '%,%')
              and (cast(j.date as varchar(max)) LIKE '%/%' or cast(j.date as varchar(max)) LIKE '')        
)
--select count(*) from comment --42557
select 
        Contacts as 'externalId'
        , cast('-10' as int) as userid
        , CONVERT(datetime, CONVERT(VARCHAR(19),replace(convert(varchar(50),date),'',''),120) , 103) as 'comment_timestamp|insert_timestamp'
        ,  replace(comment,ascii(0x0C),'') as 'comment_content'
       /* , REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( comment 
                                        ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                                        ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                                        ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                                        ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                                        ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                                        ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'')  as 'comment_content' */
from comment