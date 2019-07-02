with cte_industry as ( 
	SELECT 
	contact_id candidate_id, 
	s.best_fit industry,
	c.best_fit original
	FROM contact c, UNNEST(string_to_array(c.best_fit, '~')) s(best_fit)
)

select
candidate_id,
industry,
current_timestamp as insert_timestamp
from cte_industry
where industry <> ''