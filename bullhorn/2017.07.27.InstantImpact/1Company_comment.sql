select
        CC.clientCorporationID as 'company-externalId'
        , cast('-10' as int) as userid
        , dateadded as 'comment_timestamp|insert_timestamp'
        ,Coalesce('Company Overview: ' + NULLIF([dbo].[udf_StripHTML](CC.notes), '') + char(10), '') as 'comment_content'
from bullhorn1.BH_ClientCorporation CC
where cast(CC.notes as varchar(max)) is not null and cast(CC.notes as varchar(max)) <> ''
--where CC.clientCorporationID = '143'
