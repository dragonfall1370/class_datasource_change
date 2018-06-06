--COMPANY breakdown
---Clients counts:	5659
---Sites counts:	5831


--CONTACT
---Contact counts:	38443


--JOBS
---Counts:			5579


--CANDIDATE
---Counts:			16662


--JOB APPLICATION
---Matches			74905
---Placements		2169

---Duplicate records in COMPANY (already removed)
with dup as (
select SiteUniqueID, Organisation, ROW_NUMBER() OVER(PARTITION BY SiteUniqueID ORDER BY SiteUniqueID ASC) AS rn
from Sites)

delete from dup
where rn > 1