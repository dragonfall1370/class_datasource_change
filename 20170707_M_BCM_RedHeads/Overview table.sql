select * from ContactMainTable where Type = 1 and IsDeletedLocally = 0 and FullName like '%Pieter%'

select * from ContactMainTable where ContactServiceID in (23832,29124,25779)

select * from ContactDetailsTable where FirstName like '%Mohan%' and LastName like '%Krishna%'

select * from ContactAttachments where ContactServiceID = 25779

select * from ContactDetailsTable

select * from ContactCategoriesTable where CategoryName in ('Placed Candidate',
'Employee',
'Undesired applicant',
'Applicant',
'IT Applicant',
'Inactive Applicant'
)

select * from ContactCategoriesTable where CategoryName in ('Former undesired employee',
'Competitor',
'Personal',
'Former employee',
'Inactive Client',
'Client',
'Source of Candidates',
'urgent',
'Restraint of Trade',
'Supplier',
'Reference')

select * from ContactCategoriesTable where CategoryName is NULL

select * from ProjectExportView

select * from Attachments

select * from ContactAttachments
--order by ContactServiceID
where ContactServiceID = 33793
left join Attachments A on CA.ContactServiceID = A.

select count(*) from ActivityAttachments

select * from ActivityContacts where ContactID = 33793

307653
307656
307705

select * from ActivityAttachments

select top 100 * from ActivitiesTable where ActivityID in (307653,307656,307705) and ActivityType = 2
where FolderType = 6
--where ActivityType = 2
--Subject like 'CV_Mohan%'

select top 1000 * from ActivitiesTable where Subject like '%.%'

 Subject like '%.pdf' or Subject like '%.doc%' or Subject like '%.xls%' or Subject like '%.doc%' or Subject like '%.rtf%'

select distinct FolderType from ActivitiesTable where ItemType = 22

select * from ContactMainTable where Type = 3 and IsDeletedLocally = 0

select * from OpportunityProductsTable