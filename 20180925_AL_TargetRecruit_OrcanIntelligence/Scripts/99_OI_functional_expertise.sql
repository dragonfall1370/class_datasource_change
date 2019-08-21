--select * from VCOIFunctionalExpertise

drop table if exists #ExpertiseBreakDown

select
Id as
--CanExtId
ConExtId
, value as Expertise

into #ExpertiseBreakDown

from Contact
	cross apply string_split(AVTRRT__Candidate_Short_List__c, ';')
where [RecordTypeId] =
--'012b0000000J2RD' -- candidate
'012b0000000J2RE' -- contact
and len(trim(isnull(AVTRRT__Candidate_Short_List__c, ''))) > 0

--drop table if exists VCCanFuntionalExpertise
drop table if exists VCConFuntionalExpertise

select

--eb.CanExtId
eb.ConExtId
, fe.Id as ExpertiseId
, fe.Expertise as ExpertiseName

into VCConFuntionalExpertise

from #ExpertiseBreakDown eb
left join [dbo].[VCOIFunctionalExpertise] fe on eb.Expertise = fe.Expertise

drop table if exists #ExpertiseBreakDown

--select * from VCCanFuntionalExpertise
--select * from VCConFuntionalExpertise


--select * from VCCanFuntionalExpertise
select * from VCConFuntionalExpertise
where ExpertiseId is not null