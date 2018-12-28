
with

 job (JobOpeningId,ClientId,PostingTitle,rn) as (
       SELECT  a.JobOpeningId as JobOpeningId
		, cl.clientID as clientID
		, a.PostingTitle as PostingTitle
		, ROW_NUMBER() OVER(PARTITION BY cl.ClientId,a.PostingTitle ORDER BY a.JobOpeningId) AS rn 
	from jobopenings a 
	left join (select ClientId, ClientName from Clients) cl on cl.ClientId = a.ClientId )
--select * from job

select
         j.JobOpeningId As 'position-externalId'
       , case when job.rn > 1 then concat(job.PostingTitle,' ',rn) else job.PostingTitle end as 'position-title' --, j.PostingTitle As 'position-title'
        , ltrim(Stuff(    
                          Coalesce('Industry: ' + NULLIF(j.Industry, '') + char(10), '')
                        + Coalesce('No of Candidates Associated: ' + NULLIF(j.NoofCandidatesAssociated, '') + char(10), '')
                        + Coalesce('Country: ' + NULLIF(j.Country, '') + char(10), '')
                        + Coalesce('Created By: ' + NULLIF(concat(u1.FirstName,' ',u1.LastName,' - ',u1.email), '') + char(10), '') --j.CreatedBy
                        + Coalesce('Modified By: ' + NULLIF(concat(u2.FirstName,' ',u2.LastName,' - ',u2.email), '') + char(10), '') --j.ModifiedBy
                , 1, 0, '') ) as 'position-note'
-- select count(*) -- select distinct Industry -- select top 100 *
from JobOpenings J
left join (select ClientId, ClientName from Clients) cl on cl.ClientId = j.ClientId
left join (select userid, email from users) u0 on u0.userid = j.AssignedRecruiter_s
left join (select userid, FirstName, LastName, email from users) u1 on u1.userid = j.CreatedBy
left join (select userid, FirstName, LastName, email from users) u2 on u2.userid = j.ModifiedBy
left join job on j.JobOpeningId = job.JobOpeningId