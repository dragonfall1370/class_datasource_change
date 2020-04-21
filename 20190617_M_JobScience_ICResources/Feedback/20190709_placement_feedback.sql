select j.id as job_id
, j.name
, con.id as candidate_id
, con.firstname
, con.lastname
, con.recordtypeid
, p.*
from ts2_placement_c p
left join ts2_job_c j on p.ts2_job_c = j.id
left join contact con on p.ts2_employee_c = con.id
where p.name = 'PLC-061918-08120'