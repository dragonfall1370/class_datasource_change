/*
select count(*) from VacanciesCandidates
select count (*) --12791 from CandidateImportAutomappingTemplateversion c
select top 10 candidate_externalId, * from CandidateImportAutomappingTemplateversion c

*/
-- select distinct convert(varchar(max),Status) from VacanciesCandidates
with jobapp as (
        select
          v.Candidate as 'application-candidateExternalId'
        , v.Vacancy as 'application-positionExternalId'
        , v.Status
                 --This field only accepts: SHORTLISTED,SENT,1ST_INTERVIEW,2ND_INTERVIEW,OFFERED,PLACED, INVOICED, Other values will not be recognized.
                , Coalesce(NULLIF(case convert(varchar(max),v.Status)
                                when 'Placed' then 'PLACED'
                                when 'Under Offer' then 'OFFERED'
                                when 'Interviewed (2+)' then '2ND_INTERVIEW'
                                when 'Accepted' then '1ST_INTERVIEW'
                                when 'Awaiting Interview' then '1ST_INTERVIEW'
                                when 'Interviewed' then '1ST_INTERVIEW'
                                when 'CV Sent' then 'SENT'
                                when 'Not Shortlisted' then 'SENT'
                                when 'Prospective' then 'SHORTLISTED'
                                when 'Shortlisted' then 'SHORTLISTED'
                        else '' end, ''), '') as 'application-stage'    
        , v.AddedDate
        -- select count(*) -- select top 10 *
        from VacanciesCandidates v
        left join CandidatesImportAutomappingTemplate c on convert(varchar(max),c.candidate_externalId) = convert(varchar(max),v.Candidate)
        where convert(varchar(max),c.candidate_externalId) is not null )

select * from jobapp where [application-stage] <> '' order by convert(varchar(max),AddedDate) desc


	
/*
with
  JPInfo as (
  	select    JP.jobPostingID as JobID
  		, JP.title as JobTitle
		, Cl.clientID as ContactID
		, Cl.userID as ClientUserID
		, UC.name as ContactName
		, UC.email as ContactEmail
		, CC.clientCorporationID as CompanyID
		, CC.name as CompanyName
	from bullhorn1.BH_JobPosting JP
	left join bullhorn1.BH_Client Cl on JP.clientUserID = Cl.userID
	left join bullhorn1.BH_UserContact UC on JP.clientUserID = UC.userID
	left join bullhorn1.BH_ClientCorporation CC on JP.clientCorporationID = CC.clientCorporationID
	where 1=1 and JP.title <> '' and Cl.isPrimaryOwner = 1 )
        --select * from JPInfo order by JobID
*/
