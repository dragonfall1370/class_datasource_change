with JobStatus as (select VacancyRef,ContactID,Notes, iif(LastStatusEN is null or LastStatusEN = '',StatusEN,LastStatusEN) as 'Status', LastStatusEN, StatusEN from tblVacancyCandidate),

LastStt as (select a.VacancyRef, a.ContactID, a.Notes, a.Status, b.StatusText, a.LastStatusEN, a.StatusEN from JobStatus a left join jlc_tblVacancyCandidateStatus b on a.Status = b.StatusID),

-----rejected candidate------
/*select ltrim(rtrim(VacancyRef)), ContactID, Notes from LastStt where StatusEN in ('10','20')*/

-----successful candidate------
JobApp as (select ltrim(rtrim(VacancyRef)) as 'application-positionExternalId',
ContactID as 'application-candidateExternalId',
case when StatusText = 'Unsuccessful' then 'SHORTLISTED'
when LastStatusEN = 20 then cast(StatusEN as varchar(10))
when LastStatusEN = 10 then cast(StatusEN as varchar(10))
when StatusText = 'Shortlisted' then 'SHORTLISTED'
when StatusText = 'CV Sent' then 'SENT'
when StatusText = 'Awaiting Interview' then 'FIRST_INTERVIEW'
when StatusText = 'Interviewed' then 'FIRST_INTERVIEW'
when StatusText = 'Awaiting Interview(2+)' then 'SECOND_INTERVIEW'
when StatusText = 'Interviewed(2+)' then 'SECOND_INTERVIEW'
when StatusText = 'Under Offer' then 'OFFERED'
when StatusText = 'Accepted' then 'OFFERED'
when StatusText = 'Placed' then 'PLACEMENT_PERMANENT' else '' end
as 'application-stage'
from LastStt)

select [application-positionExternalId], [application-candidateExternalId],
case when [application-stage] = '10' then 'SHORTLISTED'
when [application-stage] = '20' then 'SHORTLISTED'
when [application-stage] = '50' then 'FIRST_INTERVIEW'
when [application-stage] = '60' then 'FIRST_INTERVIEW'
when [application-stage] = '90' then 'OFFERED'
when [application-stage] = 'SHORTLISTED' then 'SHORTLISTED'
when [application-stage] = 'SENT' then 'SENT'
when [application-stage] = 'FIRST_INTERVIEW' then 'FIRST_INTERVIEW'
when [application-stage] = 'FIRST_INTERVIEW' then 'FIRST_INTERVIEW'
when [application-stage] = 'SECOND_INTERVIEW' then 'SECOND_INTERVIEW'
when [application-stage] = 'Interviewed(2+)' then 'SECOND_INTERVIEW'
when [application-stage] = 'SECOND_INTERVIEW' then 'OFFERED'
when [application-stage] = 'OFFERED' then 'OFFERED'
when [application-stage] = 'PLACEMENT_PERMANENT' then 'PLACEMENT_PERMANENT' else 'SHORTLISTED' end as 'application-stage'
from JobApp