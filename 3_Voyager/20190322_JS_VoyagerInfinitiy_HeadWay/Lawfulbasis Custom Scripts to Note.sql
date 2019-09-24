
with test as (select a.intCandidateId
,iif(a.tintContractualLegalLawfulBasisId = '' or a.tintContractualLegalLawfulBasisId is null,'','Contractual') as Contractual_Legal_Lawful_Basis
,a.datContractualLegalLawfulBasisExpiry
,b.vchLawfulBasisName
,d.vchLawfulBasisStatus 
,a.datExpiry
,e.vchLawfulBasisReason
,iif(a.bitIsManuallyRestricted = 0,concat('Restrict Recruitment Processing: ','No'),concat('Restrict Recruitment Processing: ','Yes')) as 'Restrict Recruitment Processing'
,f.vchRestrictionReason
,a.datManuallyRestrictedUntil
from dCandidatePrivacy a
left join sLawfulBasis b on a.tintLawfulBasisId = b.tintLawfulBasisId
left join lLawfulBasisLawfulBasisStatus c on a.intLawfulBasisLawfulBasisStatusId = c.intLawfulBasisLawfulBasisStatusId
left join sLawfulBasisStatus d on c.tintLawfulBasisStatusId = d.tintLawfulBasisStatusId
left join refLawfulBasisReason e on a.intLawfulBasisReasonId = e.intLawfulBasisReasonId
left join refRestrictionReason f on a.intManuallyRestrictedReasonId = f.intRestrictionReasonId)


select 
intCandidateId,
concat(
nullif(concat('Contractual Legal Lawful Basis: ', Contractual_Legal_Lawful_Basis,(char(13)+char(10))),concat('Contractual Legal Lawful Basis: ',(char(13)+char(10))))
,nullif(concat('Contractual Legal Lawful Basis Expiration Date: ', datContractualLegalLawfulBasisExpiry,(char(13)+char(10))),concat('Contractual Legal Lawful Basis Expiration Date: ',(char(13)+char(10))))
,nullif(concat('Lawful Basis Name: ', vchLawfulBasisName,(char(13)+char(10))),concat('Lawful Basis Name: ',(char(13)+char(10))))
,nullif(concat('Lawful Basis Status: ', vchLawfulBasisStatus,(char(13)+char(10))),concat('Lawful Basis Status: ',(char(13)+char(10))))
,nullif(concat('Lawful Basis Expiration Date: ', datExpiry,(char(13)+char(10))),concat('Lawful Basis Expiration Date: ',(char(13)+char(10))))
,nullif(concat('Lawful Basis Reason: ', vchLawfulBasisReason,(char(13)+char(10))),concat('Lawful Basis Reason: ',(char(13)+char(10))))
,[Restrict Recruitment Processing],(char(13)+char(10))
,nullif(concat('Restriction Reason: ', vchRestrictionReason,(char(13)+char(10))),concat('Restriction Reason: ',(char(13)+char(10))))
,nullif(concat('Restriction Until: ', datManuallyRestrictedUntil,(char(13)+char(10))),concat('Restriction Until: ',(char(13)+char(10))))
) as 'Note2'
from test