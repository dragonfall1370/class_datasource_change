with
  contact0 (CLIENT,name,CONTACT,rn) as (SELECT cc.CLIENT, cg.name, cc.CONTACT,ROW_NUMBER() OVER(PARTITION BY CONTACT ORDER BY cc.BISUNIQUEID DESC) AS rn FROM PROP_X_CLIENT_CON cc left join PROP_CLIENT_GEN cg on cg.reference = cc.client )
, contact as (select CLIENT, name, CONTACT from contact0 where rn = 1)
--select count(*) from contact --15702
--select * from contact where CONTACT = 38077


, t as (
select
--select --distinct
	  ccc.CONTACT as 'contact-externalId', pg.person_id
	, ccc.CLIENT as 'contact-companyId', ccc.name as 'company-name'
	, replace(pg.FIRST_NAME,'?','') as 'contact-firstName'
	, case when (replace(pg.LAST_NAME,'?','') = '' or pg.LAST_NAME is null) then 'No Lastname' else replace(pg.LAST_NAME,'?','') end as 'contact-lastName'
	  
       , ltrim(Stuff( Coalesce(' ' + NULLIF(address.STREET1, ''), '') + Coalesce(', ' + NULLIF(address.STREET2, ''), '') , 1, 1, '') ) as 'locationAddress'
       , ltrim(Stuff( Coalesce(' ' + NULLIF(address.STREET1, ''), '') + Coalesce(', ' + NULLIF(address.STREET2, ''), '') , 1, 1, '') ) as 'locationName'
       , address.locality
       , address.TOWN as 'city'
       , address.county as 'state'
       , address.POST_CODE as 'post_code'
       , address.COUNTRY
       , address.country_name as 'country_code'
       , 'PERSONAL_ADDRESS' as location_type
	, getdate() as insert_timestamp

--select count(*) --15780--select count(distinct cc.CONTACT) --15701 rows -- select * from contact
--from PROP_X_CLIENT_CON cc
--left join contact ccc on ccc.CONTACT = cc.CONTACT --
from contact ccc
left join PROP_PERSON_GEN pg on ccc.CONTACT = pg.REFERENCE
left join ( 
       SELECT REFERENCE, STREET1, STREET2, LOCALITY, TOWN, county, POST_CODE, COUNTRY, config_name
              , case
		when DESCRIPTION like 'AFGHANI%' then 'AF'
		when DESCRIPTION like 'ALBANIA%' then 'AL'
		when DESCRIPTION like 'ARGENTI%' then 'AR'
		when DESCRIPTION like 'AUSTRAL%' then 'AU'
		when DESCRIPTION like 'AUSTRIA%' then 'AT'
		when DESCRIPTION like 'BELARUS%' then 'BY'
		when DESCRIPTION like 'BELGIUM%' then 'BE'
		when DESCRIPTION like 'BRAZIL%' then 'BR'
		when DESCRIPTION like 'BULGARI%' then 'BG'
		when DESCRIPTION like 'CANADA%' then 'CA'
		when DESCRIPTION like 'CHILE%' then 'CL'
		when DESCRIPTION like 'CHINA%' then 'CN'
		when DESCRIPTION like 'COLOMBI%' then 'CO'
		when DESCRIPTION like 'CYPRUS%' then 'CY'
		when DESCRIPTION like 'CZECH%' then 'CZ'
		when DESCRIPTION like 'DENMARK%' then 'DK'
		when DESCRIPTION like 'ECUADOR%' then 'EC'
		when DESCRIPTION like 'EGYPT%' then 'EG'
		when DESCRIPTION like 'ESTONIA%' then 'EE'
		when DESCRIPTION like 'FINLAND%' then 'FI'
		when DESCRIPTION like 'FRANCE%' then 'FR'
		when DESCRIPTION like 'GERMANY%' then 'DE'
		when DESCRIPTION like 'GIBRALT%' then 'ES'
		when DESCRIPTION like 'GREECE%' then 'GR'
		when DESCRIPTION like 'HUNGARY%' then 'HU'
		when DESCRIPTION like 'ICELAND%' then 'IS'
		when DESCRIPTION like 'INDIA%' then 'IN'
		when DESCRIPTION like 'INDONES%' then 'ID'
		when DESCRIPTION like 'IRELAND%' then 'IE'
		when DESCRIPTION like 'ISRAEL%' then 'IL'
		when DESCRIPTION like 'ITALY%' then 'IT'
		when DESCRIPTION like 'JORDAN%' then 'JO'
		when DESCRIPTION like 'KAZAKHS%' then 'KZ'
		when DESCRIPTION like 'KENYA%' then 'KE'
		when DESCRIPTION like 'LATVIA%' then 'LV'
		when DESCRIPTION like 'LEBANON%' then 'LB'
		when DESCRIPTION like 'LIECHTE%' then 'LI'
		when DESCRIPTION like 'LITHUAN%' then 'LT'
		when DESCRIPTION like 'LUXEMBO%' then 'LU'
		when DESCRIPTION like 'MACEDON%' then 'MK'
		when DESCRIPTION like 'MALAYSI%' then 'MY'
		when DESCRIPTION like 'MALTA%' then 'MT'
		when DESCRIPTION like 'MOLDOVA%' then 'MD'
		when DESCRIPTION like 'NETHERL%' then 'NL'
		when DESCRIPTION like 'NEW ZEALAND%' then 'NZ'
		when DESCRIPTION like 'NORWAY%' then 'NO'
		when DESCRIPTION like 'PANAMA%' then 'PA'
		when DESCRIPTION like 'PHILIPP%' then 'PH'
		when DESCRIPTION like 'POLAND%' then 'PL'
		when DESCRIPTION like 'PORTUGA%' then 'PT'
		when DESCRIPTION like 'QATAR%' then 'QA'
		when DESCRIPTION like 'ROMANIA%' then 'RO'
		when DESCRIPTION like 'RUSSIAN%' then 'RU'
		when DESCRIPTION like 'SAUDI%' then 'SA'
		when DESCRIPTION like 'SERBIA%' then 'RS'
		when DESCRIPTION like 'SINGAPO%' then 'SG'
		when DESCRIPTION like 'SLOVAKI%' then 'SK'
		when DESCRIPTION like 'SLOVENI%' then 'SI'
		when DESCRIPTION like 'SOUTH AFRICA%' then 'ZA'
		when DESCRIPTION like 'SPAIN%' then 'ES'
		when DESCRIPTION like 'SRI%' then 'LK'
		when DESCRIPTION like 'SWAZILA%' then 'SZ'
		when DESCRIPTION like 'SWEDEN%' then 'SE'
		when DESCRIPTION like 'SWITZER%' then 'CH'
		when DESCRIPTION like 'TAIWAN%' then 'TW'
		when DESCRIPTION like 'TAJIKIS%' then 'TJ'
		when DESCRIPTION like 'TURKEY%' then 'TR'
		when DESCRIPTION like 'UKRAINE%' then 'UA'
		when DESCRIPTION like '%UNITED%ARAB%' then 'AE'
		when DESCRIPTION like '%UAE%' then 'AE'
		when DESCRIPTION like '%U.A.E%' then 'AE'
		when DESCRIPTION like '%UNITED%KINGDOM%' then 'GB'
		when DESCRIPTION like '%UNITED%STATES%' then 'US'
		when DESCRIPTION like '%US%' then 'US'
              end as 'country_name'
       from (
              select REFERENCE, STREET1, STREET2, LOCALITY, TOWN, county, POST_CODE, COUNTRY, MN.DESCRIPTION , CONFIG_NAME, rn = ROW_NUMBER() OVER (PARTITION BY REFERENCE /*, STREET1, STREET2, LOCALITY, TOWN, county, POST_CODE ,COUNTRY, MN.DESCRIPTION*/ ORDER BY CONFIG_NAME desc /*, STREET1 desc*/ )
              -- select distinct OCC.config_name 
              from PROP_ADDRESS ADDRESS 
              left JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID = ADDRESS.OCC_ID
              left JOIN MD_MULTI_NAMES MN ON MN.ID = ADDRESS.COUNTRY
              --where OCC.config_name = 'Primary'
              --where MN.LANGUAGE = 10010 --and REFERENCE in (45315)
              --order by OCC.config_name asc
              where (STREET1 is not null and STREET1 <> '') --and REFERENCE in (116674210691,116656235074,47987,116657338952)
              ) a
       where a.rn = 1 --and REFERENCE in (116674210691,116656235074,47987,116657338952)
       ) address on address.REFERENCE = ccc.CONTACT
)

--select [contact-externalId] from t group by [contact-externalId] having count(*) > 1 
select * from t where [contact-externalId] in (116674210691,116656235074,47987,116657338952)


