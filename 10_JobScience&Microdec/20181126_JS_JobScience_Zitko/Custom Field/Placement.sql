--Placement
select Id, ts2__Job__c,Name from ts2__Placement__c

--Job
select ts2__Job__c from ts2__Placement__c

--Employee
select ts2__Employee__c from ts2__Placement__c

--Candidate Source
select ["Name"] from ts2__Source__c

--Start Date
select id, ts2__Start_Date__c from ts2__Placement__c

--Pay Rate
select Id, ts2__Pay_Rate__c from ts2__Placement__c

--End Date
select id, ts2__End_Date__c from ts2__Placement__c

--Bill Rate
select id, ts2__Bill_Rate__c from ts2__Placement__c

--Total Standard Hours
select id, Total_Standard_Hours__c from ts2__Placement__c

--Gross Margin
select ((( Total_Standard_Hours__c * ts2__Bill_Rate__c ) + ( Total_Overtime_Hours__c * Overtime_Rate_Factor__c * ts2__Bill_Rate__c ))
- (( Total_Standard_Hours__c * ts2__Pay_Rate__c ) + ( Total_Overtime_Hours__c * Overtime_Rate_Factor__c * ts2__Pay_Rate__c ))) as 'Gross Margin' from ts2__Placement__c

--Client
select Id, ts2__Client__c from ts2__Placement__c

--Client Address
select a.Id, a.ts2__Account__c, b.ShippingCity, b.ShippingState from ts2__Job__c a left join Account b on a.ts2__Account__c = b.Id

--Contact Phone
select a.Id, a.ts2__Account__c, b.Phone from ts2__Job__c a left join Account b on a.ts2__Account__c = b.Id

--Contact Email
select a.Id, a.ts2__Account__c, b.Email from ts2__Job__c a left join Contact b on a.ts2__Account__c = b.AccountId

--Primary Recruiter
select Id, ts2__Filled_Pct__c from ts2__Placement__c

--Primary Recruiter Payout
select * from Primary_Recruiter_Payout

--Document
select a.Id, b.Name, b.ContentType from ts2__Placement__c a left join Attachment b on a.Id = b.ParentId
where b.name <> ''