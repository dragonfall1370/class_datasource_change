-->>> CHANGE REQUEST
select c.id
, can.id as "candidate_id", can.fullname as "candidate_name" --c.employee_c
, -10 as "user_account_id"
, c.createddate::timestamp as insert_timestamp
, 'comment' as "category"
, 'candidate' as "type"
, concat_ws(chr(10), '[CHANGE REQUEST]'
			, '---Details---'
			, coalesce('Change Request Name: ' || nullif(c."name",''),NULL)
			, coalesce('Change Request Status: ' || nullif(c.change_request_status_c,''),NULL)
			, coalesce('Related Placement: ' || nullif(p.name,''),NULL) --c.related_placement_c
			, coalesce('Employee: ' || nullif(can.fullname, ' '),NULL) --c.employee_c
			, coalesce('Account: ' || nullif(a1.name,''),NULL) --c.account_c
			, chr(10), '---Info to complete---'
			, coalesce('Effective Date: ' || nullif(c.effective_date_c,''),NULL)
			, coalesce('End Date: ' || nullif(c.end_date_c,''),NULL)
			, coalesce('Bill Rate: ' || c.currencyisocode || ' ' || nullif(c.bill_rate_c,''),NULL)
			, coalesce('Pay Rate: ' || c.currencyisocode || ' ' || nullif(c.pay_rate_c, ' '),NULL)
			, coalesce('Contact Manager: ' || nullif(con.fullname,''),NULL) --c.contact_manager_c
			, coalesce('Contact Email: ' || nullif(con.email,''),NULL) --c.contact_manager_c
			, coalesce('Timecard Approver: ' || nullif(con4.fullname,''),NULL) --c.timecardapprover_c
			, coalesce('Status: ' || nullif(c.status_c,''),NULL)
			, chr(10), '---Accounts--'
			, coalesce('Accounts Receivable: ' || nullif(u3.fullname,''),NULL) --c.accounts_receivable_u_c
			, coalesce('Accounts Payable: ' || nullif(con2.fullname,''),NULL) --c.accounts_payable_c
			, coalesce('Currency: ' || nullif(c.currencyisocode,''),NULL)
			, chr(10), '---Paperwork--'
			, coalesce('Employee Name Formula: ' || nullif(can.fullname,''),NULL) --c.employee_c
			, coalesce('Send Extension Contracts: ' || case c.send_extension_contracts_c when '1' then 'YES' else 'NO' end,NULL)
			, coalesce('Date Extension Documents Sent: ' || nullif(c.date_extension_documents_sent_c,''),NULL)
			, coalesce('Contractor Extension Form Completed: ' || case c.contractor_extension_form_completed_c when '1' then 'YES' else 'NO' end,NULL)
			, coalesce('Date Contractor Extension Form Completed: ' || nullif(c.date_contractor_extension_form_completed_c,''),NULL)
			, coalesce('Umbrella Contact: ' || nullif(con2.fullname ,''),NULL) --c.umbrella_contact_c
			, coalesce('Send Client Extension Contracts: ' || case c.send_client_extension_contracts_c when '1' then 'YES' else 'NO' end, NULL)
			, coalesce('Date Client Extension Documents Sent: ' || nullif(c.date_client_extension_documents_sent_c,''),NULL)
			, coalesce('Client Extension Form Completed: ' || case c.client_extension_form_completed_c when '1' then 'YES' else 'NO' end,NULL)
			, coalesce('Date Client Extension Form Completed: ' || nullif(c.date_client_extension_form_completed_c,''),NULL)
			, chr(10), '---Change request fields--'
			, coalesce('Employee Name Formula: ' || nullif(c.description_c,''),NULL)
			, chr(10), '---Status--'
			, coalesce('Processed Status: ' || nullif(c.processed_status_c,''),NULL)
			, coalesce('Processed Datetime: ' || nullif(c.processed_datetime_c,''),NULL)
			, coalesce('Processed Description: ' || nullif(c.processed_description_c ,''),NULL)
			, coalesce('Owner: ' || nullif(u4.fullname,''),NULL)
			, coalesce('Created By: ' || nullif(u.fullname,''),NULL)
			, coalesce('Created: ' || nullif(c.createddate,''),NULL)
			, coalesce('Last Modified By: ' || nullif(u2.fullname,''),NULL)
			, coalesce('Last Modified: ' || nullif(c.lastmodifieddate,''),NULL)
            ) as content
/*
--DETAILS
, c."name" --change request name
, c.change_request_status_c --change request status
, c.related_placement_c --Related Placement
, c.employee_c --Employee
, can.fullname
, c.account_c --Account
, a1.name

--Info to complete
, c.effective_date_c
, c.end_date_c
, c.bill_rate_c
, c.pay_rate_c
, c.currencyisocode
, c.contact_manager_c
, con.fullname
, con.email
, c.timecardapprover_c
, con4.fullname
, c.status_c

--Accounts
, c.accounts_receivable_u_c --User
, u3.fullname
, c.accounts_payable_c --Contact
, con2.fullname
, c.currencyisocode

--Paperwork
, can.fullname --Employee Name Formula
, c.send_extension_contracts_c--Send Extension Contracts
, c.date_extension_documents_sent_c --Date Extension Documents Sent
, c.contractor_extension_form_completed_c --Contractor Extension Form Completed
, c.date_contractor_extension_form_completed_c --Date Contractor Extension Form Completed
, con2.fullname --Umbrella Contact
, c.send_client_extension_contracts_c--Send Client Extension Contracts
, c.date_client_extension_documents_sent_c --Date Client Extension Documents Sent
, c.client_extension_form_completed_c --Client Extension Form Completed
, c.date_client_extension_form_completed_c --Date Client Extension Form Completed
*/
--Change request fields
, c.description_c
/*
--Status
, c.processed_status_c --Processed Status
, c.processed_datetime_c --Processed Datetime
, c.processed_description_c --Processed Description
, c.ownerid
, c.createdbyid
, c.createddate
, c.lastmodifiedbyid
, c.lastmodifieddate
*/
from change_request_c c
left join (select id, concat(firstname,' ',lastname) as fullname, email from "user") u on u.id = c.createdbyid
left join (select id, concat(firstname,' ',lastname) as fullname, email from "user") u2 on u2.id = c.lastmodifiedbyid
left join (select id, concat(firstname,' ',lastname) as fullname, email from "user") u3 on u3.id = c.accounts_receivable_u_c
left join (select id, concat(firstname,' ',lastname) as fullname, email from "user") u4 on u4.id = c.ownerid
left join ts2_placement_c p on p.id = c.related_placement_c
left join (select id, concat(firstname,' ',lastname) as fullname, email from contact 
			where recordtypeid in ('0120Y0000013O5d')) con on con.id = c.contact_manager_c --CONTACT (manager)
left join (select id, concat(firstname,' ',lastname) as fullname, email from contact) con2 on con2.id = c.accounts_payable_c --CONTACT (payable)
left join (select id, concat(firstname,' ',lastname) as fullname, email from contact 
			where recordtypeid in ('0120Y0000013O5d')) con3 on con3.id = c.umbrella_contact_c --CONTACT (umbrella contact)
left join (select id, concat(firstname,' ',lastname) as fullname, email from contact 
			where recordtypeid in ('0120Y0000013O5d')) con4 on con4.id = c.timecardapprover_c --CONTACT (timecard approver)
left join (select id, concat(firstname,' ',lastname) as fullname, email from contact
			where recordtypeid in ('0120Y0000013O5c','0120Y000000RZZV')) can on can.id = c.employee_c --CANDIDATE
left join (select id, name from Account) a1 on a1.id = c.account_c --COMPANY
--where c.id = 'a3Z0J000000I1BJUA0' --538
where can.id is not NULL --442