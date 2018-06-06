select * from bullhorn1.BH_Client

select * from bullhorn1.BH_UserContact

select * from bullhorn1.Candidate where 

select * from bullhorn1.BH_JobPosting

select * from bullhorn1.View_JobSubmission

select * from bullhorn1.BH_JobResponse where jobResponseID = 533

select count(distinct status) from bullhorn1.BH_JobResponse group by status

select candidateID, customText2, businessSectorIDList from bullhorn1.Candidate where email3 like '%davidphng@hotmail.com'

select * from bullhorn1.BH_BusinessSectorList

select CA.candidateID, UCOI.instanceID, COI.text1, COI.text2, concat(text1,' ',text2) as 'Text' from bullhorn1.Candidate CA
left outer join bullhorn1.BH_UserCustomObjectInstance UCOI on CA.userID = UCOI.userID
left outer join bullhorn1.BH_CustomObjectInstance COI on UCOI.instanceID = COI.instanceID
where email3 like '%TKMISTRAL@aol.com'

select * from bullhorn1.BH_CustomObjectInstance

select * from bullhorn1.BH_UserCustomObjectInstance