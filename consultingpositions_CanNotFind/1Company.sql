--select top 200 *         from Attachment at where id = '00P0y00000jWAm8EAG'

with 
  doc0 as ( select at.ParentId, concat(at.id,'_',replace(at.Name,',','') ) as doc, a.name
        -- select count(*) --119557 -- select top 200 * 
        from Attachment at
        left join Account a on a.id = at.ParentId
        --where (at.name like '%doc' or at.name like '%docx' or at.name like '%pdf' or at.name like '%rtf' or at.name like '%xls' or at.name like '%xlsx' or at.name like '%msg')
        and a.id is not null
         )
, doc (ParentId, docs) as (SELECT ParentId, STUFF((SELECT ', ' + doc from doc0 WHERE doc0.ParentId <> '' and ParentId = a.ParentId FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS docs FROM doc0 as a where a.ParentId <> '' GROUP BY a.ParentId)
--select top 200 * from doc

, dup as (SELECT ID,name,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(a.name)) ORDER BY a.ID ASC) AS rn FROM Account a) --where name in ('LAgroup','ProConseil','Sustainable','You Improve','Azurance')
--select * from dup where rn > 1

, headquarter as ( select distinct a.ID, h.name 
                   from Account a
                   left join (select ID,NAME from Account ) h on a.ID = h.ID
                   where a.ID is not null and a.ID <> '' )
--select * from headquarter 

, note as (
	select a.ID
	, Stuff(         Coalesce('ID: ' + NULLIF(cast(a.ID as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Name: ' + NULLIF(a.name, '') + char(10), '')
                        + Coalesce('Parent Company: ' + NULLIF(b.name, '') + char(10), '')
                        --+ Coalesce('Source: ' + NULLIF(a.Source__c, '') + char(10), '')
                        + Coalesce('Account Status: ' + NULLIF(a.Account_status__c, '') + char(10), '')
                        + Coalesce('Priority: ' + NULLIF(a.Priority__c, '') + char(10), '')
                        + Coalesce('Description: ' + NULLIF(a.Description, '') + char(10), '')
                        + Coalesce('Comments Franck: ' + NULLIF(a.Comments_FJ_Fr__c, '') + char(10), '')
                        + Coalesce('Comments Others: ' + NULLIF(a.Comments_FNJ_Fr__c, '') + char(10), '')
                        + Coalesce('Operational Consulting?: ' + NULLIF(a.Operational_consulting__c, '') + char(10), '')
                , 1, 0, '') as note
                -- select  top 10 * -- select count(*) --2101
        from Account a
        left join ( select distinct ID, name from Account ) b on b.ID = a.parentID --where b.ID <> a.ID and a.id in ('0018000000YbT5zAAF','0018000000Zb0g0AAB','001C000000o382tIAA')
        )
--select * from note where note like '%&%;%'

select
          a.id as "company-externalId"
        , u.username as 'company-owners' --, 'franck@consultingpositions.net' as "owners" , a.OwnerID as "owners"
        --, iif(a.ID in (select ID from dup where dup.rn > 1),concat(dup.name,' - ',a.BillingCountry), iif(a.NAME = '' or a.name is null,'No CompanyName',a.NAME)) as 'company-name'   --NOTE: CHANGE "Z_Punkt" TO "Z_Punkt Germany"
        , iif(dup.rn > 1,concat(dup.name,' - ',a.BillingCountry), iif(dup.name = '' or dup.name is null, 'No CompanyName',dup.name)) as 'company-name'   --NOTE: CHANGE "Z_Punkt" TO "Z_Punkt Germany"
        , a.Phone as "Switchboard"
        , a.Fax as "Fax"
        , a.website as "Website"
        --, a.Source__c
        , h.name as 'company-headquarter'        
        , a.BillingCity as "locationCity"
        , a.BillingState as "locationState"
        , a.BillingPostalCode as "locationZipCode"
        , case
		when a.BillingCountry like '59491%' then ''
		when a.BillingCountry like '75248%' then ''
		when a.BillingCountry like 'Africa%' then 'ZA'
		when a.BillingCountry like 'Austral%' then 'AU'
		when a.BillingCountry like 'Austria%' then 'AT'
		when a.BillingCountry like 'Belgium%' then 'BE'
		when a.BillingCountry like 'Cambodi%' then 'KH'
		when a.BillingCountry like 'Canada%' then 'CA'
		when a.BillingCountry like 'Denmark%' then 'DK'
		when a.BillingCountry like 'Finland%' then 'FI'
		when a.BillingCountry like 'France%' then 'FR'
		when a.BillingCountry like 'FR%' then 'FR'
		when a.BillingCountry like 'Germany%' then 'DE'
		when a.BillingCountry like 'Gremany%' then 'DE'
		when a.BillingCountry like 'Holland%' then 'NL'
		when a.BillingCountry like 'Hong%' then 'HK'
		when a.BillingCountry like 'India%' then 'IN'
		when a.BillingCountry like 'Indones%' then 'ID'
		when a.BillingCountry like 'Ireland%' then 'IE'
		when a.BillingCountry like 'Israel%' then 'IL'
		when a.BillingCountry like 'Italy%' then 'IT'
		when a.BillingCountry like 'Luxembo%' then 'LU'
		when a.BillingCountry like 'Malaysi%' then 'MY'
		when a.BillingCountry like 'Netherl%' then 'NL'
		when a.BillingCountry like 'Norway%' then 'NO'
		when a.BillingCountry like 'Poland%' then 'PL'
		when a.BillingCountry like 'Singapo%' then 'SG'
		when a.BillingCountry like 'State of Qatar%' then 'QA'
		when a.BillingCountry like 'SUA%' then ''
		when a.BillingCountry like 'Sweden%' then 'SE'
		when a.BillingCountry like 'Switzer%' then 'CH'
		when a.BillingCountry like 'Thailan%' then 'TH'
		when a.BillingCountry like 'UK%' then 'GB'
		when a.BillingCountry like 'United%Kingdom' then 'GB'
		when a.BillingCountry like 'USA%' then 'US'
		when a.BillingCountry like 'US%' then 'US'
		when a.BillingCountry like 'Vietnam%' then 'VN'
		when a.BillingCountry like '%UNITED%ARAB%' then 'AE'
		when a.BillingCountry like '%UAE%' then 'AE'
		when a.BillingCountry like '%U.A.E%' then 'AE'
		when a.BillingCountry like '%UNITED%KINGDOM%' then 'GB'
		when a.BillingCountry like '%UNITED%STATES%' then 'US'
        else '' end as "locationCountry"
        , ltrim(Stuff(    Coalesce(' ' + NULLIF(a.BillingStreet, ''), '')
                        + Coalesce(', ' + NULLIF(a.BillingCity, ''), '')
                        + Coalesce(', ' + NULLIF(a.BillingState, ''), '')
                        + Coalesce(', ' + NULLIF(a.BillingPostalCode, ''), '')
                        + Coalesce(', ' + NULLIF(a.BillingCountry, ''), '')
                , 1, 1, '') ) as 'company-locationAddress'
        , Coalesce(' BILLING: ' + 
                         NULLIF(ltrim(Stuff(
                            Coalesce(NULLIF(a.BillingCity, ''), '')
                        + Coalesce(' - ' + NULLIF(a.BillingState, ''), '')
                        + Coalesce(' - ' + NULLIF(a.BillingCountry, ''), '')
                        ,1,0,'')),'')
        ,'')
                 as 'company-locationName'                              
        , n.note as 'company-note'
        , doc.docs as 'company-document'
-- select distinct Source__c -- select top 20 * -- select count(*) -- select a.id, a.ownerid, u.username
from Account a
left join Users u on u.ID = a.OwnerId
left join dup on dup.ID = a.ID
left join headquarter h on h.ID = a.ID
left join note n on n.ID = a.ID
left join doc on doc.ParentId = a.id
--where a.name like '%Brooks%'
--where a.name in ('LAgroup','ProConseil','Sustainable','You Improve','Azurance')
;



with t as (
       -- COMMENT
       select  
                a.ID
              , CONVERT(datetime,convert(varchar(50),n.CreatedDate,120)) as 'comment_timestamp|insert_timestamp'
              , Stuff( 'NOTE: ' + char(10)
                       + Coalesce('Title: ' + NULLIF(n.title, '') + char(10), '')
                       + Coalesce('Body: ' + NULLIF(n.body, '') + char(10), '')
                       + Coalesce('Created By: ' + NULLIF(u1.name, '') + char(10), '')
                       + Coalesce('Modified By:' + NULLIF(u2.name, '') + char(10), '')
                       + Coalesce('Modified Date: ' + NULLIF(n.LastModifiedDate, '') + char(10), '')
                       , 1, 0, '') as 'content'
       from Note n 
       left join (select id, concat(firstname,' ',lastname, ' - ', username) as name from users) u1 on u1.id = n.createdbyid
       left join (select id, concat(firstname,' ',lastname, ' - ', username) as name from users) u2 on u2.id = n.LastModifiedById
       left join account a on a.id = n.parentid
       where a.id is not null
UNION ALL
       select 
              a.id
              , CONVERT(datetime,convert(varchar(50),t.CreatedDate,120)) as 'comment_timestamp|insert_timestamp'
              , Stuff( 'TASK: ' + char(10)
                       + Coalesce('Who: ' + NULLIF(u3.name, '') + char(10), '')
                       + Coalesce('Subject: ' + NULLIF(t.subject, '') + char(10), '')
                       + Coalesce('Status: ' + NULLIF(t.status, '') + char(10), '')
                       + Coalesce('Priority: ' + NULLIF(t.Priority, '') + char(10), '')
                       + Coalesce('Description: ' + NULLIF(replace(t.Description,'.  ',char(10)), '') + char(10), '')
                       + Coalesce('Created By: ' + NULLIF(u1.name, '') + char(10), '')
                       + Coalesce('Modified By:' + NULLIF(u2.name, '') + char(10), '')
                       + Coalesce('Modified Date: ' + NULLIF(t.LastModifiedDate, '') + char(10), '')
                       + Coalesce('Reminder Date Time: ' + NULLIF(t.ReminderDateTime, '') + char(10), '')
                       , 1, 0, '') as 'content'
       from task t
       left join (select id, concat(firstname,' ',lastname, ' - ', username) as name from users) u1 on u1.id = t.createdbyid
       left join (select id, concat(firstname,' ',lastname, ' - ', username) as name from users) u2 on u2.id = t.LastModifiedById
       left join ( select id, concat(firstname,' ',lastname) as name from Contact ) u3 on u3.id = t.whoid
       left join Account a on a.id = t.whatid
       where a.id is not null
       --and t.whoid <> '000000000000000AAA'
       --and t.subject like 'Email%'
        )

--select count(*) from t where content is not null --4753
select --top 100
                   id as 'external_id'
                  , cast('-10' as int) as 'user_account_id'
                  , 'comment' as 'category'
                  , 'contact' as 'type'
                  , [comment_timestamp|insert_timestamp] as 'comment_timestamp|insert_timestamp'
                  , content as 'content'
from t --where note <> '' 

