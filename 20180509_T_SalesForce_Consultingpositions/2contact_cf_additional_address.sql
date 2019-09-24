
-- MAILLING ADDRESS
with cf as (
	select c.ID , FirstName, LastName
	, ltrim(Stuff(
                                           Coalesce(' ' + NULLIF(c.MailingStreet, '') + char(10), '')
                                        + Coalesce(', ' + NULLIF(c.MailingCity, '') + char(10), '')
                                        + Coalesce(', ' + NULLIF(c.MailingState, '') + char(10), '')
                                        + Coalesce(', ' + NULLIF(c.MailingPostalCode, '') + char(10), '')
                                        + Coalesce(', ' + NULLIF(c.MailingCountry, '') + char(10), '')            
                , 1, 1, '')) as field_value
        from Contact c )
--select * from cf where field_value is not null        
SELECT
         id as additional_id
        , 'add_con_info' as additional_type
        , 1007 as form_id
        , 1016 as field_id
        , field_value
from cf where field_value is not null        



-- OTHER ADDRESS
with cf as (        
	select c.ID , FirstName, LastName
	, ltrim(Stuff(       
                                           Coalesce(' ' + NULLIF(c.OtherStreet, '') + char(10), '')
                                        + Coalesce(', ' + NULLIF(c.OtherCity, '') + char(10), '')
                                        + Coalesce(', ' + NULLIF(c.OtherState, '') + char(10), '')
                                        + Coalesce(', ' + NULLIF(c.OtherPostalCode, '') + char(10), '')
                                        + Coalesce(', ' + NULLIF(c.OtherCountry, '') + char(10), '')
                , 1, 1, '')) as field_value
        from Contact c )    
--select * from cf where note is not null
SELECT
         id as additional_id
        , 'add_con_info' as additional_type
        , 1007 as form_id
        , 1018 as field_id
        , field_value
from cf where field_value is not null

