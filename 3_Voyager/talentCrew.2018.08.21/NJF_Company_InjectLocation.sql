
with 
tempLocation as (
       select 
                ct.intCompanyTierId
              , iif(right(ct.vchCompanyTierName,1) = ':',left(ct.vchCompanyTierName,len(ct.vchCompanyTierName)-1),ct.vchCompanyTierName) as locationName
              , ct.intCompanyId, ct.vchAddressLine1, ct.vchAddressLine2 ,ct.vchAddressLine3
              , ct.vchTown, ct.vchCounty,ct.sintCountryId, rc.vchCountryName,rc.vchCountryCode, ct.vchPostcode
              , Stuff(
                              Coalesce(' ' + NULLIF(ltrim(rtrim(vchAddressLine1)), ''), '')
                            + Coalesce(', ' + NULLIF(ltrim(rtrim(vchAddressLine2)), ''), '')
                            + Coalesce(', ' + NULLIF(ltrim(rtrim(vchAddressLine3)), ''), '')
                            + Coalesce(', ' + NULLIF(ltrim(rtrim(vchTown)), ''), '')
                            + Coalesce(', ' + NULLIF(ltrim(rtrim(vchCounty)), ''), '')
                            + Coalesce(', ' + NULLIF(rc.vchCountryName, ''), '')
                            + Coalesce(', ' + NULLIF(ltrim(rtrim(vchPostCode)), ''), '')
                            , 1, 1, '') as 'fullAddress'
       from dCompanyTier ct 
       left join refCountry rc on ct.sintCountryId = rc.sintCountryId)
 --select * from tempLocation order by intCompanyId

, TierPhone1 as (
       select intCompanyTierTelecomId, intCompanyTierId, iif(vchExtension <> '', concat(vchNumber,vchExtension), vchNumber) as vchNumber 
       from dCompanyTierTelecom
       where vchNumber is not null and vchNumber <> '' and (vchDescription = 'Location Tel No' or vchDescription = '')
       )
--select * from TierPhone1 

, TierPhone as (
       SELECT 
              intCompanyTierId, 
              STUFF(
                (SELECT ',' + vchNumber
                 from  TierPhone1
                 WHERE intCompanyTierId = T.intCompanyTierId
                 order by intCompanyTierId asc
                 FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
                 , 1, 1, '')  AS Num
       FROM TierPhone1 as T
       GROUP BY T.intCompanyTierId)

select top 100
                concat('NJF',intCompanyId) as externalID
		, iif(Num = '' or Num is null, locationName,concat(locationName,' - Ph. ',Num)) as locationName
		--, vchCountryName as countryName
		, vchPostcode as postCode
		, concat(replace(replace(replace(replace(fullAddress,',,',','),'Lo, ndon','London'),'"',''),'  ',''),iif(Num = '' or Num is null, '',concat(' - Ph. ',Num))) as address
		/*, case when fullAddress like '%Londo%' then 'GB'
			 when fullAddress like '%United%' then 'US'
			 when fullAddress like '%New York%' then 'US'
			 --when fullAddress like '%NY%' then 'US'
			 when fullAddress like '%USA%' then 'US'
			 when fullAddress like '%Chicago%' then 'US'
			 when fullAddress like '%Chicage%' then 'US'
			 when fullAddress like '%Frankfurt%' then 'DE'
			 when fullAddress like '%German%' then 'DE'
			 when fullAddress like '%China%' then 'CN'
			 when fullAddress like '%Brasil%' then 'BR'
			 when fullAddress like '%Italy%' then 'IT'
			 when fullAddress like '%Madrid%' then 'ES'
			 else vchCountryCode end as countryCode */
              , tl.vchCountryCode as countryCode
		/*, case when fullAddress like '%Londo%' then 'United Kingdom'
			 when fullAddress like '%United%' then 'United States'
			 when fullAddress like '%New York%' then 'United States'
			 when fullAddress like '%Chicage%' then 'United States'
			 --when fullAddress like '%NY%' then 'United States'
			 when fullAddress like '%USA%' then 'United States'
			 when fullAddress like '%Chicago%' then 'United States'
			 when fullAddress like '%Frankfurt%' then 'Germany'
			 when fullAddress like '%German%' then 'Germany'
			 when fullAddress like '%China%' then 'China'
			 when fullAddress like '%Italy%' then 'Italy'
			 when fullAddress like '%Madrid%' then 'Spain'
			 else vchCountryName end  as countryNameupdated
			 */
              , tl.vchCountryCode as countryNameupdated
		, tl.intCompanyTierId
		, vchTown as City
from tempLocation tl 
left join TierPhone tp on tl.intCompanyTierId = tp.intCompanyTierId--where fullAddress is not null
--where intCompanyId in (2,455)--,1887,4546,6397,1436,6251,1670,2054,6275,499,5081,6676,5508,592,6504,6803,6488,2)
order by tl.intCompanyTierId

