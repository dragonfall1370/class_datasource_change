select concat('GP',jen.Candidate_Unique_ID) as CanExternalId, -10 as userId
		, jen.Creation_Date
		, case 
				when jen.Creation_Date = '00/00/0000 00:00:00' then getdate()
				when jen.Creation_Date like '%/%' then convert(datetime,jen.Creation_Date,103)
				else convert(datetime,concat(left(jen.Creation_Date,4),'/',substring(jen.Creation_Date,9,2),'/',substring(jen.Creation_Date,6,2),' ',right(jen.Creation_Date,8)),120) end as InsertTimeStamp
		, -10 as AssignedUserId, 'comment' as category, 'candidate' as type
		--,jen.Contact_Unique_ID,con.Contact_Unique_ID, jen.Candidate_Unique_ID,jen.Site_Unique_ID, comp.Site_Unique_ID, comp.Main_Contact_Unique, con.Main_Site_Unique
		, concat('--MIGRATED FROM JournalEntryNotes--',
				iif(jen.Subject in ('',' '),'', concat(char(10),'Subject: ',jen.Subject)),
				iif(jen.Contact_Type in ('',' '), '', concat(char(10),'Contact Type: ',jen.Contact_Type)),
				iif(jen.Candidate_Unique_ID in ('',0), '', concat(char(10),'Relate to Candidate: ',can.Forename,' ',can.Surname)),
				iif(jen.Contact_Unique_ID in ('',0) or con.Contact_Unique_ID is null, '', concat(char(10),'Relate to Contact: ',con.Forename,' ',con.Surname)),
				iif(jen.Vacancy_Unique_ID in ('',0), '', coalesce(char(10) + 'Relate to Job: ' + nullif(j.Role_Description,''),'')),
				iif(jen.Site_Unique_ID in ('',0) or comp.Site_Unique_ID is null, '', coalesce(char(10) + 'Relate to Company: ' + nullif(comp.Organisation,''),'')),
				iif(jen.Importance = '', '', concat(char(10),'Importance: ',jen.Importance)),
				iif(jen.Emailed_YN = '', '', concat(char(10),'Email Y/N: ',jen.Emailed_YN)),
				iif(jen.Direction_IO = '', '', concat(char(10),'Direction I/O: ',jen.Direction_IO)),
				iif(jen.Draft_YN = '', '', concat(char(10),'Draft Y/N: ',jen.Draft_YN)),
				iif(jen.Call_Type = '', '', concat(char(10),'Call Type: ',jen.Call_Type)),
				iif(jen.Call_Objective = '', '', concat(char(10),'Call Objective: ',jen.Call_Objective)),
				iif(jen.Call_Duration = 0, '', concat(char(10),'Call Duration: ',jen.Call_Duration)),
				--iif(jen.Call_Recording = '', '', concat(char(10),'Call Recording: ',replace(jen.Call_Recording,'N','No'))),--just N and ? values
				iif(jen.Call_Result = '', '', concat(char(10),'Call Result: ',jen.Call_Result)),
				iif(jen.Ref = '', '', concat(char(10),'Ref: ',jen.Ref)),
				iif(jen.Diary_Entry_Type = '', '', concat(char(10),'Diary Entry Type: ',jen.Diary_Entry_Type)),
				iif(jen.Diary_User = '','', concat(char(10),'Diary User: ',jen.Diary_User)),
				iif(jen.Diary_Date = '','', iif(CHARINDEX(':',jen.Diary_Date)<>0,concat(char(10),'Diary Date Time: ', substring(jen.Diary_Date,6,2),'/',substring(jen.Diary_Date,9,2),'/',left(jen.Diary_Date,4),right(jen.Diary_Time,9)),concat(char(10),'Diary Date Time: ',jen.Diary_Date,right(jen.Diary_Time,9)))),
				iif(jen.Creating_User = '','', concat(char(10),'Creating User: ',jen.Creating_User)),
				iif(jen.Creation_Date = '','', iif(CHARINDEX(':',jen.Creation_Date)<>0,concat(char(10),'Creation Date: ', substring(jen.Creation_Date,6,2),'/',substring(jen.Creation_Date,9,2),'/',left(jen.Creation_Date,4)),concat(char(10),'Creation Date: ',jen.Creation_Date))),
				concat(char(10),'Journal ID: ', jen.Unique_id)
				) as commentContent
from JournalEntryNotes jen --left join TaskType tt on t.TaskTypeId = tt.TaskTypeId
			left join Candidates can on jen.Candidate_Unique_ID = can.Unique_ID
			left join Vacancies j on jen.Vacancy_Unique_ID = j.Unique_ID
			left join ContactManagementContacts con on jen.Contact_Unique_ID = con.Contact_Unique_ID
			left join ContactManagementSites comp on jen.Site_Unique_ID = comp.Site_Unique_Id
where jen.Candidate_Unique_ID <> 0
--where jen.Vacancy_Unique_ID <> 0
order by jen.Unique_ID