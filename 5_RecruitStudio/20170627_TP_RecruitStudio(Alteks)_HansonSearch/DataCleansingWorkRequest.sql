
--  1. Reassign all Candidate owners to the list of users in the attached.
with t as (
        select
                  CL.ContactId as 'candidate-externalId'
                , Coalesce(NULLIF(replace(CL.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
                , Coalesce(NULLIF(replace(CL.LastName,'?',''), ''), 'No Lastname') as 'contact-lastName'
                , CL.username --, owner.email
                , case
                        when CL.username = 'AD Tech' then '[{"ownerId":29037,"primary":"true"}]'
                        when CL.username = 'Admin' then '[{"ownerId":29037,"primary":"true"}]'
                        when CL.username = 'Adminl' then '[{"ownerId":29037,"primary":"true"}]'
                        when CL.username = 'Alice ' then '[{"ownerId":28987,"primary":"true"}]'
                        when CL.username = 'Alice Weightman' then '[{"ownerId":28987,"primary":"true"}]'
                        when CL.username = 'Alice Weightman' then '[{"ownerId":28987,"primary":"true"}]'
                        when CL.username = 'Alive Weightman' then '[{"ownerId":28987,"primary":"true"}]'
                        when CL.username = 'Alive Weightman' then '[{"ownerId":28987,"primary":"true"}]'
                        when CL.username = 'Amandeep Gill' then '[{"ownerId":29006,"primary":"true"}]'
                        when CL.username = 'Amandeep Gilll' then '[{"ownerId":29006,"primary":"true"}]'
                        when CL.username = 'Amir Hedayat' then '[{"ownerId":28993,"primary":"true"}]'
                        when CL.username = 'Amy Hayer' then '[{"ownerId":29001,"primary":"true"}]'
                        when CL.username = 'Amy Hayer' then '[{"ownerId":29001,"primary":"true"}]'
                        when CL.username = 'April Dudley' then '[{"ownerId":28987,"primary":"true"}]'
                        when CL.username = 'Charlotte' then '[{"ownerId":28997,"primary":"true"}]'
                        when CL.username = 'Charlotte Fisher' then '[{"ownerId":28997,"primary":"true"}]'
                        when CL.username = 'Elaine Hardman' then '[{"ownerId":29032,"primary":"true"}]'
                        when CL.username = 'Felice' then '[{"ownerId":28994,"primary":"true"}]'
                        when CL.username = 'Felice  Hurst' then '[{"ownerId":28994,"primary":"true"}]'
                        when CL.username = 'Felice Hurst' then '[{"ownerId":28994,"primary":"true"}]'
                        when CL.username = 'Fin Tech' then '[{"ownerId":29014,"primary":"true"}]'
                        when CL.username = 'Grant' then '[{"ownerId":29008,"primary":"true"}]'
                        when CL.username = 'Grant Somerville' then '[{"ownerId":29008,"primary":"true"}]'
                        when CL.username = 'Grant Sommerville' then '[{"ownerId":29008,"primary":"true"}]'
                        when CL.username = 'Headhunter ' then '[{"ownerId":29037,"primary":"true"}]'
                        when CL.username = 'Headhunter 1' then '[{"ownerId":29037,"primary":"true"}]'
                        when CL.username = 'Headhunter1' then '[{"ownerId":29037,"primary":"true"}]'
                        when CL.username = 'Healthcare Advertising' then '[{"ownerId":28992,"primary":"true"}]'
                        when CL.username = 'Henry Liddell' then '[{"ownerId":28998,"primary":"true"}]'
                        when CL.username = 'James Sandford' then '[{"ownerId":28984,"primary":"true"}]'
                        when CL.username = 'Jamie Emmerson' then '[{"ownerId":28983,"primary":"true"}]'
                        when CL.username = 'Janie Ememrson' then '[{"ownerId":28983,"primary":"true"}]'
                        when CL.username = 'Janie Ememrson' then '[{"ownerId":28983,"primary":"true"}]'
                        when CL.username = 'Janie Ememrson' then '[{"ownerId":28983,"primary":"true"}]'
                        when CL.username = 'Janie Emerson' then '[{"ownerId":28983,"primary":"true"}]'
                        when CL.username = 'Janie Emmason' then '[{"ownerId":28983,"primary":"true"}]'
                        when CL.username = 'Janie Emmerson' then '[{"ownerId":28983,"primary":"true"}]'
                        when CL.username = 'Janine Emmerson' then '[{"ownerId":28983,"primary":"true"}]'
                        when CL.username = 'Jannie Emerson ' then '[{"ownerId":28983,"primary":"true"}]'
                        when CL.username = 'Jannie Emmerson' then '[{"ownerId":28983,"primary":"true"}]'
                        when CL.username = 'Joseph Ducrocq' then '[{"ownerId":29034,"primary":"true"}]'
                        when CL.username = 'Joseph Ducroq' then '[{"ownerId":29034,"primary":"true"}]'
                        when CL.username = 'Julien Wondja Dooh' then '[{"ownerId":29003,"primary":"true"}]'
                        when CL.username = 'Kally Simmonds' then '[{"ownerId":28995,"primary":"true"}]'
                        when CL.username = 'Kate O Rourke' then '[{"ownerId":29010,"primary":"true"}]'
                        when CL.username = 'Kate O''Rourke ' then '[{"ownerId":29010,"primary":"true"}]'
                        when CL.username = 'Katie' then '[{"ownerId":29016,"primary":"true"}]'
                        when CL.username = 'Katie Simpson' then '[{"ownerId":29016,"primary":"true"}]'
                        when CL.username = 'Katie Simspson' then '[{"ownerId":29016,"primary":"true"}]'
                        when CL.username = 'Laurence Levy' then '[{"ownerId":28990,"primary":"true"}]'
                        when CL.username = 'Linda Andersson' then '[{"ownerId":29009,"primary":"true"}]'
                        when CL.username = 'Luke Boobbyer' then '[{"ownerId":28996,"primary":"true"}]'
                        when CL.username = 'Madeleine Weightman' then '[{"ownerId":28987,"primary":"true"}]'
                        when CL.username = 'Market Research' then '[{"ownerId":28993,"primary":"true"}]'
                        when CL.username = 'Nikki Samson' then '[{"ownerId":29004,"primary":"true"}]'
                        when CL.username = 'Other Dubai' then '[{"ownerId":28994,"primary":"true"}]'
                        when CL.username = 'Other UK' then '[{"ownerId":29037,"primary":"true"}]'
                        when CL.username = 'Paidraic Carey' then '[{"ownerId":28992,"primary":"true"}]'
                        when CL.username = 'Richard Savage' then '[{"ownerId":29014,"primary":"true"}]'
                        when CL.username = 'Russell Weir' then '[{"ownerId":29010,"primary":"true"}]'
                        when CL.username = 'Sarag Hadj' then '[{"ownerId":29005,"primary":"true"}]'
                        when CL.username = 'Seraj Jaghbeer' then '[{"ownerId":28994,"primary":"true"}]'
                        when CL.username = 'Simon' then '[{"ownerId":28993,"primary":"true"}]'
                        when CL.username = 'Simon Jacob' then '[{"ownerId":28993,"primary":"true"}]'
                        when CL.username = 'Sorcha Dunphy' then '[{"ownerId":29017,"primary":"true"}]'
                        when CL.username = 'Tamara Bullock' then '[{"ownerId":29038,"primary":"true"}]'
                        when CL.username = 'Technical' then '[{"ownerId":29037,"primary":"true"}]'
                        when CL.username = 'Vickiy Hodson' then '[{"ownerId":29005,"primary":"true"}]'
                        when CL.username = 'Vicky Hodosn ' then '[{"ownerId":29005,"primary":"true"}]'
                        when CL.username = 'Vicky Hodson ' then '[{"ownerId":29005,"primary":"true"}]'
                        when CL.username = 'Vicky Hosdon ' then '[{"ownerId":29005,"primary":"true"}]'
                        when CL.username = 'Vikcy Hodson ' then '[{"ownerId":29005,"primary":"true"}]'
                        when CL.username like 'Sarah%' then '[{"ownerId":"29005","primary":"true"}]'
                        when CL.username = 'Other France' then '[{"ownerId":"28990","primary":"true"}]'
                  else '[]'
                  end as owner
        from Contacts CL
        left join (select CL.username as name, Coalesce( NULLIF(cl.Email, ''), '') as email from Contacts CL where CL.Email like '%_@_%.__%' and CL.displayname = CL.username) owner on CL.username = owner.name
        where descriptor = 2 ) --and CL.username is not null
select count(*) from t
select top 100 [candidate-externalId],[contact-firstName],[contact-lastName],[owner] from t where [candidate-externalId] in ('100066-4655-17130','100104-2412-164')
select [candidate-externalId],[contact-firstName],[contact-lastName],[owner] from t where [candidate-externalId] in ('100066-4655-17130','100104-2412-164')
--select distinct(username) from t where owner <> '[]'
--select distinct(username), count(*) from t where username like '%sara%' group by username
select distinct(owner), count(*) from t group by owner
-- select distinct(username),count(*) username from contacts where descriptor = 2 group by username

/*
with t as (
        select
                  CL.ContactId as 'candidate-externalId'
                , Coalesce(NULLIF(replace(CL.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
                , Coalesce(NULLIF(replace(CL.LastName,'?',''), ''), 'No Lastname') as 'contact-lastName'
                , CL.username --, owner.email
                , case
                        when CL.username = 'AD Tech' then '[{"ownerId":"29037","primary":"true"}]'
                        when CL.username = 'Admin' then '[{"ownerId":"29037","primary":"true"}]'
                        when CL.username = 'Adminl' then '[{"ownerId":"29037","primary":"true"}]'
                        when CL.username = 'Alice ' then '[{"ownerId":"28987","primary":"true"}]'
                        when CL.username = 'Alice Weightman' then '[{"ownerId":"28987","primary":"true"}]'
                        when CL.username = 'Alice Weightman' then '[{"ownerId":"28987","primary":"true"}]'
                        when CL.username = 'Alive Weightman' then '[{"ownerId":"28987","primary":"true"}]'
                        when CL.username = 'Alive Weightman' then '[{"ownerId":"28987","primary":"true"}]'
                        when CL.username = 'Amandeep Gill' then '[{"ownerId":"29006","primary":"true"}]'
                        when CL.username = 'Amandeep Gilll' then '[{"ownerId":"29006","primary":"true"}]'
                        when CL.username = 'Amir Hedayat' then '[{"ownerId":"28993","primary":"true"}]'
                        when CL.username = 'Amy Hayer' then '[{"ownerId":"29001","primary":"true"}]'
                        when CL.username = 'Amy Hayer' then '[{"ownerId":"29001","primary":"true"}]'
                        when CL.username = 'April Dudley' then '[{"ownerId":"28987","primary":"true"}]'
                        when CL.username = 'Charlotte' then '[{"ownerId":"28997","primary":"true"}]'
                        when CL.username = 'Charlotte Fisher' then '[{"ownerId":"28997","primary":"true"}]'
                        when CL.username = 'Elaine Hardman' then '[{"ownerId":"29032","primary":"true"}]'
                        when CL.username = 'Felice' then '[{"ownerId":"28994","primary":"true"}]'
                        when CL.username = 'Felice  Hurst' then '[{"ownerId":"28994","primary":"true"}]'
                        when CL.username = 'Felice Hurst' then '[{"ownerId":"28994","primary":"true"}]'
                        when CL.username = 'Fin Tech' then '[{"ownerId":"29014","primary":"true"}]'
                        when CL.username = 'Grant' then '[{"ownerId":"29008","primary":"true"}]'
                        when CL.username = 'Grant Somerville' then '[{"ownerId":"29008","primary":"true"}]'
                        when CL.username = 'Grant Sommerville' then '[{"ownerId":"29008","primary":"true"}]'
                        when CL.username = 'Headhunter ' then '[{"ownerId":"29037","primary":"true"}]'
                        when CL.username = 'Headhunter 1' then '[{"ownerId":"29037","primary":"true"}]'
                        when CL.username = 'Headhunter1' then '[{"ownerId":"29037","primary":"true"}]'
                        when CL.username = 'Healthcare Advertising' then '[{"ownerId":"28992","primary":"true"}]'
                        when CL.username = 'Henry Liddell' then '[{"ownerId":"28998","primary":"true"}]'
                        when CL.username = 'James Sandford' then '[{"ownerId":"28984","primary":"true"}]'
                        when CL.username = 'Jamie Emmerson' then '[{"ownerId":"28983","primary":"true"}]'
                        when CL.username = 'Janie Ememrson' then '[{"ownerId":"28983","primary":"true"}]'
                        when CL.username = 'Janie Ememrson' then '[{"ownerId":"28983","primary":"true"}]'
                        when CL.username = 'Janie Ememrson' then '[{"ownerId":"28983","primary":"true"}]'
                        when CL.username = 'Janie Emerson' then '[{"ownerId":"28983","primary":"true"}]'
                        when CL.username = 'Janie Emmason' then '[{"ownerId":"28983","primary":"true"}]'
                        when CL.username = 'Janie Emmerson' then '[{"ownerId":"28983","primary":"true"}]'
                        when CL.username = 'Janine Emmerson' then '[{"ownerId":"28983","primary":"true"}]'
                        when CL.username = 'Jannie Emerson ' then '[{"ownerId":"28983","primary":"true"}]'
                        when CL.username = 'Jannie Emmerson' then '[{"ownerId":"28983","primary":"true"}]'
                        when CL.username = 'Joseph Ducrocq' then '[{"ownerId":"29034","primary":"true"}]'
                        when CL.username = 'Joseph Ducroq' then '[{"ownerId":"29034","primary":"true"}]'
                        when CL.username = 'Julien Wondja Dooh' then '[{"ownerId":"29003","primary":"true"}]'
                        when CL.username = 'Kally Simmonds' then '[{"ownerId":"28995","primary":"true"}]'
                        when CL.username = 'Kate O Rourke' then '[{"ownerId":"29010","primary":"true"}]'
                        when CL.username = 'Kate O''Rourke ' then '[{"ownerId":"29010","primary":"true"}]'
                        when CL.username = 'Katie' then '[{"ownerId":"29016","primary":"true"}]'
                        when CL.username = 'Katie Simpson' then '[{"ownerId":"29016","primary":"true"}]'
                        when CL.username = 'Katie Simspson' then '[{"ownerId":"29016","primary":"true"}]'
                        when CL.username = 'Laurence Levy' then '[{"ownerId":"28990","primary":"true"}]'
                        when CL.username = 'Linda Andersson' then '[{"ownerId":"29009","primary":"true"}]'
                        when CL.username = 'Luke Boobbyer' then '[{"ownerId":"28996","primary":"true"}]'
                        when CL.username = 'Madeleine Weightman' then '[{"ownerId":"28987","primary":"true"}]'
                        when CL.username = 'Market Research' then '[{"ownerId":"28993","primary":"true"}]'
                        when CL.username = 'Nikki Samson' then '[{"ownerId":"29004","primary":"true"}]'
                        when CL.username = 'Other Dubai' then '[{"ownerId":"28994","primary":"true"}]'
                        when CL.username = 'Other Dubai' then '[{"ownerId":"28994","primary":"true"}]'
                        when CL.username = 'Other France' then '[{"ownerId":"28990","primary":"true"}]'
                        when CL.username = 'Paidraic Carey' then '[{"ownerId":"28992","primary":"true"}]'
                        when CL.username = 'Richard Savage' then '[{"ownerId":"29014","primary":"true"}]'
                        when CL.username = 'Russell Weir' then '[{"ownerId":"29010","primary":"true"}]'
                        when CL.username = 'Sarag Hadj' then '[{"ownerId":"29005","primary":"true"}]'
                        when CL.username like 'Sarah%' then '[{"ownerId":"29005","primary":"true"}]'
                        when CL.username = 'Seraj Jaghbeer' then '[{"ownerId":"28994","primary":"true"}]'
                        when CL.username = 'Simon' then '[{"ownerId":"28993","primary":"true"}]'
                        when CL.username = 'Simon Jacob' then '[{"ownerId":"28993","primary":"true"}]'
                        when CL.username = 'Sorcha Dunphy' then '[{"ownerId":"29017","primary":"true"}]'
                        when CL.username = 'Tamara Bullock' then '[{"ownerId":"29038","primary":"true"}]'
                        when CL.username = 'Technical' then '[{"ownerId":"29037","primary":"true"}]'
                        when CL.username = 'Vickiy Hodson' then '[{"ownerId":"29005","primary":"true"}]'
                        when CL.username = 'Vicky Hodosn ' then '[{"ownerId":"29005","primary":"true"}]'
                        when CL.username = 'Vicky Hodson ' then '[{"ownerId":"29005","primary":"true"}]'
                        when CL.username = 'Vicky Hosdon ' then '[{"ownerId":"29005","primary":"true"}]'
                        when CL.username = 'Vikcy Hodson ' then '[{"ownerId":"29005","primary":"true"}]'
                  else '[]'
                  end as owner
        from Contacts CL
        left join (select CL.username as name, Coalesce( NULLIF(cl.Email, ''), '') as email from Contacts CL where CL.Email like '%_@_%.__%' and CL.displayname = CL.username) owner on CL.username = owner.name
        where descriptor = 2 ) --and CL.username is not null
*/


--  2. Set the Met / Not Met field to MET value for all candidates that have the status called "interviewed".
with t as (
        select
                  CL.ContactId as 'candidate-externalId'
                , Coalesce(NULLIF(replace(CL.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
                , Coalesce(NULLIF(replace(CL.LastName,'?',''), ''), 'No Lastname') as 'contact-lastName'
                , CL.ContactStatus
        from Contacts CL
        where descriptor = 2
        and ContactStatus = 'Interviewed')
select count(*) from t --2398
/* VINCERE MET/NOT MET
update candidate set status = 1 where note like '%Status: Interviewed%'
*/



--  3. Create a talent pool and select all candidates that falls to the criteria below and include it to the Talent Pool.
with t as (
        select
                  CL.ContactId as 'candidate-externalId'
                , Coalesce(NULLIF(replace(CL.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
                , Coalesce(NULLIF(replace(CL.LastName,'?',''), ''), 'No Lastname') as 'contact-lastName'
                , CL.ContactStatus
        from Contacts CL
        where descriptor = 2
        and ContactStatus = 'Works For Client')
select count(*) from t --2587
/* VINCERE Talen Pool
select * from candidate_group limit 100
select * from candidate_group_candidate where candidate_group_id = 37 or candidate_id = 181818
insert into candidate_group_candidate (candidate_id,candidate_group_id) values (181818,37);
insert into candidate_group_candidate (candidate_id,candidate_group_id) (select can.id,37 as vincereID from candidate can where can.note like '%Status: Works For Client%' and first_name = 'Tim' and last_name = 'Falconer')
insert into candidate_group_candidate (candidate_id,candidate_group_id) (select can.id,37 as vincereID from candidate can where can.note like '%Status: Works For Client%')
*/