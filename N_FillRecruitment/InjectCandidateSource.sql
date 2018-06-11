with CandidateSource as (select ApplicantId, Source
, case
	 when Source = 'Applicant directly' then 29094
	 when Source = 'Applied Directly' then 29094
	 when Source = 'Broadbean' then 29089
	 when Source = 'Campaign' then 29095
	 when Source = 'Campaign Magazine' then 29111
	 when Source = 'Campaign Website' then 29113
	 when Source = 'Candidate Referral' then 29096
	 when Source = 'Client Referral' then 29097
	 when Source = 'Cold call' then 29098
	 when Source = 'Creativepool' then 29100
	 when Source = 'CV Library' then 29101
	 when Source = 'Dova' then 29086
	 when Source = 'Fill website' then 29093
	 when Source = 'Google' then 29112
	 when Source = 'Guardian' then 29103
	 when Source = 'Head Hunting' then 29092
	 when Source = 'Indeed' then 29094
	 when Source = 'Instant Job Board' then 29087
	 when Source = 'Internet search' then 29102
	 when Source = 'LinkedIn' then 29090
	 when Source = 'LinkedIn shoutout' then 29099
	 when Source = 'Monster' then 29104
	 when Source = 'Only Marketing Jobs' then 29105
	 when Source = 'Other' then 29110
	 when Source = 'Personal contact' then 29106
	 when Source = 'Reed' then 29107
	 when Source like 'Referral%' then 29091
	 when Source = 'To be advised' then 29108
	 when Source = 'Twitter' then 29109
	 when Source = 'Volcanic' then 29084
	 else '' end as CanSourceId
 from VW_APPLICANT_GRID_VIEW)
 select
 distinct(CanSourceId)
  --ApplicantId, CanSourceId
 from CandidateSource where CanSourceId <> 0
--select distinct(Source) from VW_APPLICANT_GRID_VIEW order by Source