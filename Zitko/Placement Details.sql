------Job
select ts2__Job__c from ts2__Placement__c

------Employee
select ts2__Employee__c from ts2__Placement__c

------Placement Source
select ts2__Employee__c, ts2__Job__c, Placement_Source__c from ts2__Placement__c

------Record Type
select ts2__Employee__c, ts2__Job__c, b.Description from ts2__Placement__c a left join RecordType b on a.RecordTypeId = left(b.id,15)

-------Start Date - offer_personal_info
select ts2__Employee__c, ts2__Job__c, ts2__Start_Date__c from ts2__Placement__c

-------Pay Rate - public.offer
select ts2__Employee__c, ts2__Job__c, ts2__Pay_Rate__c from ts2__Placement__c where ts2__Pay_Rate__c is not null

-------End Date - offer_personal_info

select ts2__Employee__c, ts2__Job__c, ts2__End_Date__c from ts2__Placement__c where ts2__End_Date__c is not null

-------Bill Rate - public.offer - charge rate
select ts2__Employee__c, ts2__Job__c, ts2__Bill_Rate__c from ts2__Placement__c where ts2__Bill_Rate__c is not null

-------Total Standard Hours - offer
select ts2__Employee__c, ts2__Job__c, Total_Standard_Hours__c from ts2__Placement__c where Total_Standard_Hours__c is not null

-------Gross Margin - offer
with test as (select ((( cast(Total_Standard_Hours__c as decimal) * cast(ts2__Bill_Rate__c as decimal) ) + 
( cast(Total_Overtime_Hours__c as decimal) * cast(Overtime_Rate_Factor__c as decimal) * cast(ts2__Bill_Rate__c as decimal) ))  -  
(( cast(Total_Standard_Hours__c as decimal) * cast(ts2__Pay_Rate__c as decimal) ) + 
( cast(Total_Overtime_Hours__c as decimal) * cast(Overtime_Rate_Factor__c as decimal) * cast(ts2__Pay_Rate__c as decimal) ))) as GrossMargin, ts2__Employee__c, ts2__Job__c from ts2__Placement__c )
select * from test where GrossMargin is not null

------Client
select ts2__Employee__c, ts2__Job__c, ts2__Client__c from ts2__Placement__c

------Client Address - offer_personal_info
select ts2__Employee__c, ts2__Job__c, b.Id, c.BillingCity, c.BillingState
from ts2__Placement__c a
left join ts2__Job__c b on a.ts2__Job__c = b.Id
left join Account c on b.ts2__Account__c = c.Id

------Contact Phone - offer_personal_info
select ts2__Employee__c, ts2__Job__c,b.Id, c.Phone
from ts2__Placement__c a
left join ts2__Job__c b on a.ts2__Job__c = b.Id
left join Account c on b.ts2__Account__c = c.Id
where c.Phone is not null

------Contact Email - offer_personal_info

with test as (select ts2__Employee__c, ts2__Job__c, ts2__Client__c, b.ts2__Contact__c, c.Email
from ts2__Placement__c a
left join ts2__Job__c b on a.ts2__Job__c = b.Id
left join contact c on b.ts2__Contact__c = c.Id)

select * from test where email <> ''











------Placement Source
select Placement_Source__c from ts2__Placement__c