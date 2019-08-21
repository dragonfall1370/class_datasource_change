
with sl as (
--select * from PROP_X_SHORT_CAND SHORT_CAND INNER JOIN
select  --top 100 
        job,CANDIDATE,SHORTLIST,DESCRIPTION 
-- select count(*) 
from PROP_X_SHORT_CAND SHORT_CAND
-- select DISTINCT DESCRIPTION from PROP_X_SHORT_CAND SHORT_CAND
INNER JOIN PROP_SHORT_GEN SHORT_GEN ON SHORT_GEN.REFERENCE = SHORT_CAND.SHORTLIST 
INNER JOIN MD_MULTI_NAMES MN ON MN.ID = SHORT_GEN.STATUS--WHERE SHORT_CAND.CANDIDATE = <<PROP_PERSON_GEN.REFERENCE>>"
)



select
	candidate as [application-candidateExternalId]
	, job as [application-positionExternalId]
	, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace
		(description,'Applied from Web','SHORTLISTED')
		,'Considering','SHORTLISTED')
		,'Consultant Interview','1ST_INTERVIEW')
		,'CV  Sent','SENT')
		,'Float','SHORTLISTED')
		,'Interview Arranged','1ST_INTERVIEW')
		,'Interview Cancelled','1ST_INTERVIEW')
		,'No','SHORTLISTED')
		,'Offer Accepted','OFFERED')
		,'Offer Rejected','OFFERED')
		,'Placed By Us','PLACED')
		,'Rejected','SHORTLISTED')
		,'Shortlisted','SHORTLISTED')
		,'Under Offer','OFFERED')
		,'Yes','SHORTLISTED') as stage
	--This field only accepts: SHORTLISTED,SENT,1ST_INTERVIEW,2ND_INTERVIEW,OFFERED,PLACED,INVOICED,Other values will not be recognized.
from sl
where job is not null
order by candidate,job

/*
select * from PROP_PERSON_GEN
where person_id in (203951,221109,220598)
*/
