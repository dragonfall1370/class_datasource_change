
with
ind (clientCorporationID,ind) as (SELECT clientCorporationID, ind.value as ind FROM bullhorn1.BH_ClientCorporation m CROSS APPLY STRING_SPLIT( ltrim(rtrim( convert(varchar(100),m.businessSectorList) )), ';') AS ind)

/*select
       distinct ind as 'company-industry'
       , current_timestamp as insert_timestamp
from ind
where ind <> ''
*/
  
 
SELECT
         a.jobPostingID as 'externalId'
       , a.title
       , ind.ind 'job-industry'
from bullhorn1.BH_JobPosting a
left join bullhorn1.BH_Client b on a.clientUserID = b.userID
left JOIN bullhorn1.BH_ClientCorporation CC ON b.clientCorporationID = CC.clientCorporationID
left join ind on ind.clientCorporationID =  CC.clientCorporationID
