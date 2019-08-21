

with comments as (
        SELECT --top 1000
                  clientCorporationID, name
                , cast(feeArrangement as varchar(max)) as feeArrangement
        from bullhorn1.BH_ClientCorporation CC
)


-- FORM
SELECT --top 100
         clientCorporationID as additional_id, name
        , 'add_com_info' as additional_type
        , 1006 as form_id
        , 1017 as field_id
        , feeArrangement as field_value
        , 1017 as constraint_id
from comments where feeArrangement <> '' 
--and clientCorporationID = 1159