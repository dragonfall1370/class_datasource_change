select tblCandidate.ContactID,iif(tblCandidate.ContactID=tblCandidateEmployer.ContactID,'1','2') as 'isClientContact' from tblCandidate
left join tblCandidateEmployer on tblCandidate.ContactID = tblCandidateEmployer.ContactID



----- 1 = yes, 2 = no -------

