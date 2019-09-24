--Update company phones from embedded fields
---Import [companies2] table before updating
select concat('CG',id) as CompanyExtID
, name
, phonesprimary
, phonessecondary
, concat_ws(', ', nullif(embeddedphones0number,''), nullif(embeddedphones1number,'')) as companyphone
from companies2
where (phonesprimary is NULL and phonessecondary is NULL)
and (embeddedphones0number is not NULL or embeddedphones1number is not NULL)

