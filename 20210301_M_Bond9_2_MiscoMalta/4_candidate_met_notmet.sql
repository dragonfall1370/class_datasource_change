select uniqueid as cand_ext_id
, "105 perm cons xref" perm_owner
, 1 as met_notmet --1 met
from f01
where nullif("105 perm cons xref", '') is not NULL