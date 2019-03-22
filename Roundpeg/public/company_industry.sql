WITH cte_industry AS (
SELECT
account_id company_id,
s.industry,
CURRENT_TIMESTAMP insert_timestamp
from account a, UNNEST(string_to_array(industry, ';')) s(industry)
)
SELECT
company_id,
industry,
insert_timestamp
FROM cte_industry