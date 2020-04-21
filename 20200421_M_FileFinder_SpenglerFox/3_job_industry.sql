select a.idassignment as job_ext_id
, a.idassignmentsector
, ast.value as industry
from assignment a
left join assignmentsector ast on ast.idassignmentsector = a.idassignmentsector
where a.idassignmentsector is not NULL