-- FORM
with cf as (
       select id, name, Source__c -- select top 20 * -- select count(*) -- select a.id, a.ownerid, u.username
       , case
              when Source__c = 'Base_Benelux' then 1
              when Source__c = 'Base_CO_UK' then 2
              when Source__c = 'Base_FR' then 3
              when Source__c = 'Base_FR_Consultants' then 4
              when Source__c = 'Base_France' then 5
              when Source__c = 'Base_Marketing' then 6
              when Source__c = 'Base_NonEU' then 7
              when Source__c = 'Base_PME' then 8
              when Source__c = 'Base_Prospects_Ken' then 9
              when Source__c = 'Clients' then 10
              when Source__c = 'Consulting_Prospection' then 11
              when Source__c = 'DELETED - DON''T CONTACT' then 12
              when Source__c = 'FranckColin' then 13
              when Source__c = 'Others' then 14    
       end as field_value
       from Account a
       where source__c <> ''
)
SELECT
         id as additional_id
        , 'add_com_info' as additional_type
        , 1006 as form_id
        , 1015 as field_id
        , convert(varchar,field_value) as field_value
from cf --where field_value is not null
