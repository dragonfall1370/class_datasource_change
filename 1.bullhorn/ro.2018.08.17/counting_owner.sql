-----MAIN SCRIPT------
select
       UC2.email as 'contact-owners' --, UC2.name as '#Owners Name'
       , count(*) as count
-- select count(*) --7487 -- select distinct Cl.recruiterUserID, UC2.email , UC2.name
from bullhorn1.BH_Client Cl 
left join bullhorn1.BH_UserContact UC2 on Cl.recruiterUserID = UC2.userID
where isPrimaryOwner = 1
--group by Cl.recruiterUserID
group by UC2.email 


with
owner as (select distinct CA.recruiterUserID, UC.email, UC.name from bullhorn1.Candidate CA left join bullhorn1.BH_UserContact UC on CA.recruiterUserID = UC.userID where CA.isPrimaryOwner = 1)
select 
       owner.email as 'candidate-owners'
       , count(*) as count
from bullhorn1.Candidate C --where C.isPrimaryOwner = 1 --8545
left join owner on C.recruiterUserID = owner.recruiterUserID
group by owner.email