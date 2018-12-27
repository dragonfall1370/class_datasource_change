With Fee as (select ts2__Placement__c.ID, ts2__Placement__c.Qty__c,
IIF(RecordType.Name = 'Perm' and RecordType.SobjectType = '01I20000000x6lS', ts2__Salary__c * ts2__Fee_Pct__c + ts2__Salary__c * ts2__Fee2_Pct__c + ts2__Flat_Fee_Internal__c - ts2__Discount__c, ts2__Salary__c * ts2__Fee_Pct__c) as 'ts2__Fee__c'
from ts2__Placement__c
left join RecordType on ts2__Placement__c.RecordTypeId = RecordType.Id)

select (Qty__c * ts2__Fee__c) as 'Amount__c', ID, Qty__c from Fee