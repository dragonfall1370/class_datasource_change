with SalCat as (select JobListing.id, 
(case when JobListing.salarycat = 1 then 'Contact'
when JobListing.salarycat = 2  then 'Perm'
when JobListing.salarycat = 3 then 'Contract or Perm' 
else '' end) as 'Description'
from JobListing),
-------
EEStat as (select JobListing.eestatus, JobListing.id, EEStatus.description from JobListing left join EEStatus on JobListing.eestatus = EEStatus.id),
-------
AgencyNote as ( select JobListing.id, chunk from JobListing left join longtextcache on JobListing.agencynotes = longtextcache.id ),
-------
CVsend as ( select JobListing.id, chunk from JobListing left join longtextcache on JobListing.cvsendinstr = longtextcache.id ),
-------
jobdoc as ( SELECT jobid as 'JobID',
    STUFF((SELECT DISTINCT ', ' + filename
           FROM JobFiles a 
           WHERE a.jobid = b.jobid 
          FOR XML PATH('')), 1, 2, '') as 'Filename'
FROM JobFiles b
GROUP BY jobid ),
--------
dupjob as (select jobid, candid, createdate,status,salary, ROW_NUMBER() over (partition by jobid order by jobid asc) as 'jobrow'
 from JobHistory),

jobdate as ( select jobid,candid, createdate,status,salary from dupjob where jobrow=1 ),

jobcontactdup as ( select jobid, contactid, row_number() over (partition by jobid order by jobid) as 'jobrow' from jobContacts ),

jobcontact as ( select jobid, contactid from jobcontactdup where jobrow=1 ),

pos1 as (select jobcontact.contactid, JobListing.id, jobdate.createdate, concat(JobListing.jobtitletxt,'-', jobcontact.contactid,'-', jobdate.createdate) as 'jobtitle' from joblisting left join jobcontact on JobListing.id = jobcontact.jobid
left join jobdate on joblisting.id = jobdate.jobid),

pos2 as (select a.id , a.contactid, a.jobtitle, row_number() over (partition by a.jobtitle order by a.contactid) as 'posrow' from pos1 a )


select
iif(jobcontact.contactid is null or jobcontact.contactid='','0',jobcontact.contactid) as 'position-contactId',
JobListing.id as 'position-externalId',
case when (pos2.posrow = 1 and JobListing.jobtitletxt is not null) then JobListing.jobtitletxt
when (pos2.posrow = 1 and JobListing.jobtitletxt is null) then concat('No Name Job','-dupjob-',pos2.posrow)
when (pos2.posrow <> 1) then concat(JobListing.jobtitletxt,'-dupjob-',pos2.posrow)
when (JobListing.jobtitletxt = '') then concat('No Name Job',pos2.id) else concat('No Name Job','-dupjob-',pos2.posrow)
end as 'position-title',
iif(Joblisting.consultantid = Vuser.id, Vuser.email,'') as 'company-owner',
iif(joblisting.createdate='' or joblisting.createdate is null,'',joblisting.createdate) as 'position-startDate',
iif(jobdate.status = 'ACTVTJOB','2019-11-11',convert(datetime,dateadd(dd,-1, cast(getdate() as date)),11)) as 'position-endDate',
concat('EXTERNAL ID: ',JobListing.id,(char(13)+char(10)),
'JOB REFERENCE: ', Joblisting.jobref,(char(13)+char(10)),
(iif(JobListing.location = Location.id, concat('Location: ',location.description) ,'')),(char(13)+char(10)),
nullif(concat('Salary Cat: ',SalCat.Description),'Salary Cat: '),(char(13)+char(10)),
nullif(concat('EE Status: ', EEStat.description),'EE Status: '),(char(13)+char(10)),
nullif(concat('Agency Notes: ', AgencyNote.chunk),'Agency Notes: '),(char(13)+char(10)),
nullif(concat('CV Send Instructions: ', CVsend.chunk),'CV Send Instructions: ')) as 'position-note',
 iif(JobListing.longdesc = longtextcache.id,longtextcache.chunk,'') as 'position-publicDescription',
iif(Jobdoc.Filename is null or Jobdoc.Filename = '','',Jobdoc.Filename) 'position-document',
iif(jobdate.salary is null or jobdate.salary='','',jobdate.salary) as 'position-actualSalary'



from JobListing
left join Location on JobListing.location = Location.id
left join SalCat on JobListing.id = SalCat.id
left join EEstat on JobListing.id = EEStat.id
left join longtextcache on JobListing.longdesc = longtextcache.id
left join AgencyNote on JobListing.id = AgencyNote.id
left join CVSend on JobListing.id = CVsend.id
left join jobdate on JobListing.id = jobdate.jobid
left join jobdoc on JobListing.id = jobdoc.JobID
left join jobContact on JobListing.id = jobcontact.jobid
left join pos2 on JobListing.id = pos2.id
left join Vuser on JobListing.consultantid = Vuser.id