with
--DOCUMENT / ACTIVITY ATTACHMENTS
 ContactAttachment as (select distinct ac.ContactID, replace(replace(act.Subject,'.txt','.doc'),',','') as FileName, max(act.ModifiedOn) as ModifiedOn
from ActivityContacts ac --> Remove comma in the file name
left join ActivitiesTable act on act.ActivityID = ac.ActivityID
where ac.ContactType = 1 and act.ActivityType = 2 --> ContactType 2 for Company, 1 for Contact, 3 for Job | ActivityType 2 is for File
and (act.Subject like '%.pdf' or act.Subject like '%.doc%' or act.Subject like '%.xls%' or act.Subject like '%.rtf' or act.Subject like '%.html' or act.Subject like '%.txt')
group by ac.ContactID, act.Subject)

, ContactFiles as (SELECT
     ContactID,
     STUFF(
         (SELECT ',' + FileName
          from  ContactAttachment
          WHERE ContactID = a.ContactID
		  order by ModifiedOn desc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS ContactFiles
FROM ContactAttachment as a
GROUP BY a.ContactID)

--COMPANY REFERENCE
, CompanyRef as (select distinct a.ParentContactServiceID, b.FullName from ContactMainTable a
left join ContactMainTable b on a.ParentContactServiceID = b.ContactServiceID
where a.ParentType = 2)

--CANDIDATE DUPLICATE MAIL RECOGNITION
, dup as (SELECT cmt.ContactServiceID, cdt.AddressBookEmailAddress1, ROW_NUMBER() OVER(PARTITION BY cdt.AddressBookEmailAddress1 ORDER BY cmt.ContactServiceID ASC) AS rn 
FROM ContactMainTable cmt
left join ContactDetailsTable cdt on cdt.ContactServiceID = cmt.ContactServiceID
where cmt.Type = 1 and cmt.IsDeletedLocally = 0 and cdt.AddressBookEmailAddress1 is not NULL)

--MAIN SCRIPT
select
concat('RH',cmt.ContactServiceID) as 'candidate-externalId'
, iif(cdt.FirstName = '' or cdt.FirstName is NULL,'Firstname',cdt.FirstName) as 'candidate-firstName'
, iif(cdt.LastName = '' or cdt.LastName is NULL,concat('LastName-',cdt.ContactServiceID),cdt.LastName) as 'candidate-lastName'
, coalesce(cdt.MiddleName,'') as 'candidate-middleName'
, iif(cdt.AssignedTo = '' or cdt.AssignedTo is NULL
	,concat(right(cmt.CreatedBy,len(cmt.CreatedBy)-charindex('\',cmt.CreatedBy)),'@redheads.co.za')
	,concat(right(cdt.AssignedTo,len(cdt.AssignedTo)-charindex('\',cdt.AssignedTo)),'@redheads.co.za')) as 'candidate-owners'
, cdt.AddressBookEmailAddress1
, coalesce(cdt.AddressBookEmailAddress1,concat('candidate-',cmt.ContactServiceID,'@noemail.com')) as 'candidate-email-withduplication'
, iif(cmt.ContactServiceID in (select ContactServiceID from dup where dup.rn > 1)
	, iif(dup.AddressBookEmailAddress1 = '' or dup.AddressBookEmailAddress1 is NULL,concat('candidate-',cmt.ContactServiceID,'@noemail.com'),concat(dup.rn,'_',dup.AddressBookEmailAddress1))
	, iif(cdt.AddressBookEmailAddress1 = '' or cdt.AddressBookEmailAddress1 is null,concat('candidate-',cmt.ContactServiceID,'@noemail.com'),cdt.AddressBookEmailAddress1)) as 'candidate-email' --> check duplicated email
, cdt.AddressBookEmailAddress2 as 'candidate-workEmail'
, cmt.MobilePhoneNum as 'candidate-phone'
, cmt.WorkPhoneNum as 'candidate-workPhone'
, stuff((coalesce(',' + cmt.HomePhoneNum, '') + coalesce(',' + cmt.PhoneNum2, '') 
	+ coalesce(',' + cmt.PhoneNum3, '') + coalesce(',' + cmt.OtherPhoneNum, '')), 1, 1, '') as 'candidate-homePhone'
, stuff((coalesce(',' + cmt.BusinessAddress, '') + coalesce(', ' + cdt.WorkAddressCountry,'')), 1, 1, '') as 'candidate-address'
, cmt.BusinessAddress as '(OriginalAddress)'
, cdt.WorkAddressCity as 'candidate-city'
, cdt.WorkAddressZip as 'candidate-zipCode'
, convert(varchar(10),cdt.Birthday,120) as 'candidate-dob'

--EMPLOYER
, coalesce(cmt.JobTitle,'') as 'candidate-jobTitle1'
, iif(cast(uf.UserField28 as nvarchar(max)) = '' or uf.UserField28 is NULL,'',cast(uf.UserField28 as nvarchar(max))) as 'candidate-employer1'

--EDUCATION
, cast(uf.UserField49 as nvarchar(max)) as 'candidate-schoolName'
, concat(iif(uf.UserField3 = '' or uf.UserField3 is NULL,'',concat('Highest School Level: ', cast(uf.UserField3 as nvarchar(max)), char(10)))
, iif(cast(uf.UserField48 as datetime) = '' or uf.UserField48 is NULL,'',concat('Year passed: ', convert(varchar(10),cast(uf.UserField48 as datetime),120), char(10)))
, iif(uf.UserField4 = '' or uf.UserField4 is NULL,'',concat('Trademanship 1: ', cast(uf.UserField4 as nvarchar(max)), char(10)))
, iif(uf.UserField51 = '' or uf.UserField51 is NULL,'',concat('Company / Training Institution 1: ', cast(uf.UserField51 as nvarchar(max)), char(10)))
, iif(convert(varchar(10),cast(uf.UserField52 as datetime)) = '' or uf.UserField52 is NULL,'',concat('Month / Year passed 1: ', convert(varchar(10),cast(uf.UserField52 as datetime),120), char(10)))
, iif(uf.UserField5 = '' or uf.UserField5 is NULL,'',concat('Field of Study 1: ', cast(uf.UserField5 as nvarchar(max)), char(10)))
, iif(uf.UserField50 = '' or uf.UserField50 is NULL,'',concat('Learning Institute 1: ', cast(uf.UserField50 as nvarchar(max)), char(10)))
, iif(convert(varchar(10),cast(uf.UserField53 as datetime),120) = '' or uf.UserField53 is NULL,'',concat('Start Date Studies 1: ', convert(varchar(10),cast(uf.UserField53 as datetime),120), char(10)))
, iif(convert(varchar(10),cast(uf.UserField54 as datetime),120) = '' or uf.UserField54 is NULL,'',concat('End Date Studies 1: ', convert(varchar(10),cast(uf.UserField54 as datetime),120), char(10)))
, iif(uf.UserField6 = '' or uf.UserField6 is NULL,'',concat('Highiest Level 1: ', cast(uf.UserField6 as nvarchar(max)), char(10)))
, iif(uf.UserField55 = '' or uf.UserField55 is NULL,'',concat('NQF Level 1: ', cast(uf.UserField55 as nvarchar(max)), char(10)))
, iif(uf.UserField76 = '' or uf.UserField76 is NULL,'',concat('Certification 1: ', cast(uf.UserField76 as nvarchar(max)), char(10)))
, iif(uf.UserField77 = '' or uf.UserField77 is NULL,'',concat('Certification 2: ', cast(uf.UserField77 as nvarchar(max)), char(10)))
, iif(uf.UserField78 = '' or uf.UserField78 is NULL,'',concat('Certification 3: ', cast(uf.UserField78 as nvarchar(max))))) as 'candidate-education'

--CANDIDATE SKILLS
, concat(iif(uf.UserField11 = '' or uf.UserField11 is NULL,'',concat('CAD / CAE Systems: ', cast(uf.UserField11 as nvarchar(max)), char(10)))
, iif(uf.UserField12 = '' or uf.UserField12 is NULL,'',concat('Programming Languages: ', cast(uf.UserField12 as nvarchar(max)), char(10)))
, iif(uf.UserField13 = '' or uf.UserField13 is NULL,'',concat('PLC / HMI: ', cast(uf.UserField13 as nvarchar(max)), char(10)))
, iif(uf.UserField14 = '' or uf.UserField14 is NULL,'',concat('Standard Applications: ', cast(uf.UserField14 as nvarchar(max))))) as 'candidate-skills'
, cfi.ContactFiles as 'candidate-resume'

--NOTES
, concat(concat('BCM Contact ID: ',cmt.ContactServiceID,char(10))
, coalesce('Contact title: ' + cdt.Prefix + char(10),'')
, coalesce('Contact suffix: ' + cdt.Suffix + char(10),'')
, concat('Category Name: ',cct.CategoryName,char(10))
, coalesce('Source: ' + cdt.LeadSource + char(10),'')
, concat('Created by: ',right(cmt.CreatedBy,len(cmt.CreatedBy)-charindex('\',cmt.CreatedBy)))
, iif(uf.UserField43 = '' or uf.UserField43 is NULL,'',concat('Employment Status: ', cast(uf.UserField43 as nvarchar(max)), char(10)))
, iif(uf.UserField44 is NULL,'',iif(cast(uf.UserField44 as bit) = 1,concat('Director of Own Company: YES', char(10)),concat('Director of Own Company: NO',char(10))))
, iif(uf.UserField45 = '' or uf.UserField45 is NULL,'',concat('Nationality: ', cast(uf.UserField45 as nvarchar(max)), char(10)))
, iif(uf.UserField58 = '' or uf.UserField58 is NULL,'',concat('Work Permit Type Expiry Date: ', cast(uf.UserField58 as nvarchar(max)), char(10)))
, iif(uf.UserField46 = '' or uf.UserField46 is NULL,'',concat('EE Status: ', cast(uf.UserField46 as nvarchar(max)), char(10)))
, iif(uf.UserField123 is NULL,'Disabled: n/a',iif(cast(uf.UserField44 as bit) = 1,concat('Disabled: YES',char(10)),concat('Disabled: NO',char(10))))
, iif(uf.UserField32 = '' or uf.UserField32 is NULL,'',concat('Known As: ', cast(uf.UserField32 as nvarchar(max)), char(10)))
, iif(uf.UserField41 = '' or uf.UserField41 is NULL,'',concat('Pref Language: ', cast(uf.UserField41 as nvarchar(max)), char(10)))
, iif(cdt.PrefContactMethod = '' or cdt.PrefContactMethod is NULL,'',concat('Preferred Method: ', cdt.PrefContactMethod, char(10)))
, iif(uf.UserField34 = '' or uf.UserField34 is NULL,'',concat('Employed Since: ', cast(uf.UserField34 as nvarchar(max)), char(10)))
, iif(cdt.Spouse = '' or cdt.Spouse is NULL,'',concat('Spouse / Partner: ', cdt.Spouse, char(10)))
, iif(cdt.Children = '' or cdt.Children is NULL,'',concat('Children: ', cdt.Children, char(10)))

, iif(cast(uf.UserField1 as datetime) = '' or uf.UserField1 is NULL,'',concat('Date of Application: ', convert(varchar(10),cast(uf.UserField1 as datetime),120), char(10)))
, iif(uf.UserField2 = '' or uf.UserField2 is NULL,'',concat('Position applying for: ', cast(uf.UserField2 as nvarchar(max)), char(10)))
, iif(uf.UserField36 = '' or uf.UserField36 is NULL,'',concat('Suitable positions: ', cast(uf.UserField36 as nvarchar(max)), char(10)))
, iif(uf.UserField40 = '' or uf.UserField40 is NULL,'',concat('Preferred Location 1: ', cast(uf.UserField40 as nvarchar(max)), char(10)))
, iif(uf.UserField41 = '' or uf.UserField41 is NULL,'',concat('Preferred Location 2: ', cast(uf.UserField41 as nvarchar(max)), char(10)))
, iif(uf.UserField33 = '' or uf.UserField33 is NULL,'',concat('Salary Expectation: ', cast(uf.UserField33 as nvarchar(max)), char(10)))
, iif(uf.UserField35 = '' or uf.UserField35 is NULL,'',concat('Notice Period: ', cast(uf.UserField35 as nvarchar(max)), char(10)))
, iif(uf.UserField42 is NULL,'Wants perm only: n/a',iif(cast(uf.UserField42 as bit) = 1,concat('Wants perm only: YES',char(10)),concat('Wants perm only: NO',char(10))))
, iif(uf.UserField39 = '' or uf.UserField39 is NULL,'',concat('Memberships: ', cast(uf.UserField39 as nvarchar(max)), char(10)))

, iif(uf.UserField15 = '' or uf.UserField15 is NULL,'',concat('Language 1: ', cast(uf.UserField15 as nvarchar(max)), char(10)))
, iif(uf.UserField75 = '' or uf.UserField75  is NULL,'',concat('Level 1: ', cast(uf.UserField75 as nvarchar(max)), char(10)))
, iif(uf.UserField74 = '' or uf.UserField74 is NULL,'',concat('Language 2: ', cast(uf.UserField74 as nvarchar(max)), char(10)))
, iif(uf.UserField73 = '' or uf.UserField73 is NULL,'',concat('Level 2: ', cast(uf.UserField73 as nvarchar(max)), char(10)))
, iif(uf.UserField72 = '' or uf.UserField72 is NULL,'',concat('Language 3: ', cast(uf.UserField72 as nvarchar(max)), char(10)))
, iif(uf.UserField71 = '' or uf.UserField71 is NULL,'',concat('Level 3: ', cast(uf.UserField71 as nvarchar(max)), char(10)))
, iif(uf.UserField70 = '' or uf.UserField70 is NULL,'',concat('Language 4: ', cast(uf.UserField70 as nvarchar(max)), char(10)))
, iif(uf.UserField69 = '' or uf.UserField69 is NULL,'',concat('Level 4: ', cast(uf.UserField69 as nvarchar(max)), char(10)))
, iif(uf.UserField68 = '' or uf.UserField68 is NULL,'',concat('Language 5: ', cast(uf.UserField68 as nvarchar(max)), char(10)))
, iif(uf.UserField67 = '' or uf.UserField67 is NULL,'',concat('Level 5: ', cast(uf.UserField67 as nvarchar(max)), char(10)))
, iif(cmt.Department = '' or cmt.Department is NULL,'',concat('Department: ', cmt.Department, char(10)))
, iif(cdt.ManagerName = '' or cdt.ManagerName is NULL,'',concat('Manager''s name: ', cdt.ManagerName, char(10)))
, iif(cdt.AssistantName = '' or cdt.AssistantName is NULL,'',concat('Assistant''s name: ', cdt.AssistantName, char(10)))
, iif(uf.UserField124 = '' or uf.UserField124 is NULL,'',concat('Qualifications: ', cast(uf.UserField124 as nvarchar(max)), char(10)))
, iif(cmt.LeadScore = '' or cmt.LeadScore is NULL,'',concat('Lead score: ', cmt.LeadScore, char(10)))
, iif(cdt.AreaOfInterest = '' or cdt.AreaOfInterest is NULL,'',concat('Area of interest: ', cdt.AreaOfInterest, char(10)))
, iif(cmt.Rating = '' or cmt.Rating is NULL,'',concat('Contact rating: ', cmt.Rating, char(10)))
, iif(cast(cdt.ContactNotes as nvarchar(max)) = '' or cdt.ContactNotes is NULL,'',concat('Contact Notes: ',cdt.ContactNotes))) as 'candidate-note'
, concat(iif(cbn.ContactBusinessNote = '' or cbn.ContactBusinessNote is NULL,'',concat('***Business notes: ',char(10),cbn.ContactBusinessNote,'<hr>'))
		, iif(ccn.ContactCommunicationNote = '' or ccn.ContactCommunicationNote is NULL,'',concat('***Communication notes: ',char(10),ccn.ContactCommunicationNote))) as 'candidate-comments'
from ContactMainTable cmt
left join (select ContactServiceID, max(CategoryName) as CategoryName from ContactCategoriesTable
	where CategoryName in ('Former undesired employee', 'Placed Candidate', 'Former employee', 'Employee', 'Undesired applicant', 'Applicant', 'IT Applicant','Inactive Applicant')
	group by ContactServiceID) cct on cct.ContactServiceID = cmt.ContactServiceID -- conditions to filter Contact/Candidate based on mapping | 1 contact may have multiple statuses
left join ContactDetailsTable cdt on cdt.ContactServiceID = cmt.ContactServiceID --> candidate additional info
left join UserFields uf on uf.ContactServiceID = cmt.ContactServiceID --> contact addition info from UserField
left join ContactFiles cfi on cfi.ContactID = cmt.ContactServiceID --> candidate files
left join ContactBusinessNote cbn on cbn.ContactID = cmt.ContactServiceID --> contact business notes | ActivityType = 14
left join ContactCommunicationNote ccn on ccn.ContactID = cmt.ContactServiceID --> contact communication notes | ActivityType = 15, 3
left join CompanyRef cf on cf.ParentContactServiceID = cmt.ParentContactServiceID --> Company full name for candidate
left join dup on dup.ContactServiceID = cmt.ContactServiceID
where cmt.Type = 1 and cmt.IsDeletedLocally = 0
and cct.CategoryName in ('Former undesired employee', 'Placed Candidate', 'Former employee', 'Employee', 'Undesired applicant', 'Applicant', 'IT Applicant','Inactive Applicant') --> conditional mapping for contact type