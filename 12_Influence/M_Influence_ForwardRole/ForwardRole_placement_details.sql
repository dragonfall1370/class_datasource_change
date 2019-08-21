--PLACEMENT DETAILS REFERENCES
--Table-----Column
--offer_personal_info
		-----invoice_message
		-----start_date
		-----end_date
		-----client_invoice_name
		-----invoice_no
		-----invoice_date
		-----invoice_due_date
		-----client_purchase_order
		-----placed_date
		-----client_contact_phone
		-----client_contact_email
		
-----MAIN SCRIPT
select concat('FR',m.VacancyUniqID) as Forward_JobExtID
, concat('FR',m.CandidateUniqID) as Forward_CandidateExtID
, case when p.StartDate is not NULL or p.StartDate <> '' then p.StartDate
	else getdate() end as Forward_start_date
, case when p.EndDate is not NULL or p.EndDate <> '' then p.EndDate
	else getdate() end as Forward_end_date
, case when p.PlacementDate is not NULL or p.PlacementDate <> '' then p.PlacementDate
	else getdate() end as Forward_placed_date
, concat_ws(char(10)
	, coalesce('Creating user: ' + case p.CreatingUser
	when 'ADM' then concat(p.CreatingUser, ' - ', 'Admin',' - ','No email adress - admin login')
	when 'ADMT' then concat(p.CreatingUser, ' - ', 'Staff Training Login')
	when 'AH' then concat(p.CreatingUser, ' - ', 'Alison Hart',' - ','alison@forwardrolerecruitment.com')
	when 'AM' then concat(p.CreatingUser, ' - ', 'Adam Miller',' - ','adam@forwardrolerecruitment.com')
	when 'ARAL' then concat(p.CreatingUser, ' - ', 'Arif Ali',' - ','arif@forwardrole.com')
	when 'BJ' then concat(p.CreatingUser, ' - ', 'Brian Johnson',' - ','brian@forwardrole.com')
	when 'BRTO' then concat(p.CreatingUser, ' - ', 'Brad Thomas',' - ','brad@forwardrole.com')
	when 'BS' then concat(p.CreatingUser, ' - ', 'Becky Smith',' - ','becky@forwardrole.com')
	when 'CP' then concat(p.CreatingUser, ' - ', 'Camilla Purdy',' - ','camilla@forwardrole.com (ADMIN) ')
	when 'CT' then concat(p.CreatingUser, ' - ', 'Chris Thomason',' - ','chris@forwardrolerecruitment.com')
	when 'DAHA' then concat(p.CreatingUser, ' - ', 'Danielle Harvey',' - ','danielle@forwardrolerecruitment.com')
	when 'DAHY' then concat(p.CreatingUser, ' - ', 'Dan Haydon',' - ','danh@forwardrole.com')
	when 'DANO' then concat(p.CreatingUser, ' - ', 'David Nottage',' - ','david@forwardrolerecruitment.com')
	when 'DETO' then concat(p.CreatingUser, ' - ', 'Desislava Torodova',' - ','desi@forwardrole.com')
	when 'DM' then concat(p.CreatingUser, ' - ', 'Dan Middlebrook',' - ','dan@forwardrole.com')
	when 'DS' then concat(p.CreatingUser, ' - ', 'Dominic Scales',' - ','dominic@forwardrolerecruitment.com')
	when 'EMME' then concat(p.CreatingUser, ' - ', 'Emma Melling',' - ','emma@forwardrole.com')
	when 'GRBO' then concat(p.CreatingUser, ' - ', 'Grant Bodie',' - ','grant@forwardrole.com')
	when 'GW' then concat(p.CreatingUser, ' - ', 'Guy Walker',' - ','guy@forwardrole.com')
	when 'HB' then concat(p.CreatingUser, ' - ', 'Henna Baig',' - ','henna@forwardrolerecruitment.com')
	when 'HC' then concat(p.CreatingUser, ' - ', 'Helen Colley',' - ','helen@forwardrole.com')
	when 'IL' then concat(p.CreatingUser, ' - ', 'Ian Lenahan',' - ','ian@forwardrolerecruitment.com')
	when 'IZKH' then concat(p.CreatingUser, ' - ', 'Izzy Khan',' - ','izzy@forwardrole.com')
	when 'JH' then concat(p.CreatingUser, ' - ', 'Jack Harrison',' - ','jack@forwardrolerecruitment.com')
	when 'JOPE' then concat(p.CreatingUser, ' - ', 'Josh Pepper',' - ','josh@forwardrole.com')
	when 'JS' then concat(p.CreatingUser, ' - ', 'Jon Saxon',' - ','jon@forwardrolerecruitment.com')
	when 'KM' then concat(p.CreatingUser, ' - ', 'Katrina McCafferty',' - ','katrina@forwardrolerecruitment.com')
	when 'LK' then concat(p.CreatingUser, ' - ', 'Lucy Ketley',' - ','lucy@forwardrolerecruitment.com')
	when 'MABO' then concat(p.CreatingUser, ' - ', 'Matthew Borthwick',' - ','mattb@forwardrole.com')
	when 'MD' then concat(p.CreatingUser, ' - ', 'Matt Darwell',' - ','matt@forwardrole.com')
	when 'MIRH' then concat(p.CreatingUser, ' - ', 'Mike Rhodes',' - ','mike@forwardrole.com')
	when 'NAYO' then concat(p.CreatingUser, ' - ', 'Nathan Young',' - ','nathan@forwardrole.com')
	when 'PAMC' then concat(p.CreatingUser, ' - ', 'Patrick McMahon',' - ','patrick@forwardrole.com')
	when 'PAWE' then concat(p.CreatingUser, ' - ', 'Paddy Wells',' - ','paddy@forwardrole.com')
	when 'PHST' then concat(p.CreatingUser, ' - ', 'Phill Stott',' - ','phill@forwardrole.com')
	when 'RADA' then concat(p.CreatingUser, ' - ', 'Rachel Davies',' - ','rachel@forwardrole.com')
	when 'RAWH' then concat(p.CreatingUser, ' - ', 'Rachel Wheeler',' - ','rachelw@forwardrole.com')
	when 'RF' then concat(p.CreatingUser, ' - ', 'Ricardo Facchin')
	when 'RYDO' then concat(p.CreatingUser, ' - ', 'Ryan Dolan',' - ','ryan@forwardrole.com')
	when 'SASH' then concat(p.CreatingUser, ' - ', 'Sam Shinners',' - ','sam@forwardrolere.com')
	when 'SOPA' then concat(p.CreatingUser, ' - ', 'Sophie Page',' - ','sophie@forwardrole.com')
	when 'ST' then concat(p.CreatingUser, ' - ', 'Steve Thompson',' - ','steve@forwardrole.com')
	when 'TOBY' then concat(p.CreatingUser, ' - ', 'Tom Byrne',' - ','tom@forwardrole.com')
	when 'TP' then concat(p.CreatingUser, ' - ', 'Thea Parry',' - ','thea@forwardrolerecruitment.com')
	when 'WIVE' then concat(p.CreatingUser, ' - ', 'Will Velios',' - ','will@forwardrolerecruitment.com')
	else p.CreatingUser end,'')
	, coalesce('Creation Date: ' + convert(varchar(20),p.CreationDate,120),'')
	, coalesce('Pay Rate: ' + nullif(convert(varchar(max),p.PayToCandidate),''),'')
	, coalesce('Charge Rate: ' + nullif(convert(varchar(max),p.BillToClient),''),'')
	, coalesce('Hiring Manager: ' + nullif(p.Manager,''),'')
	, coalesce('Annual Salary: ' + nullif(convert(varchar(max),p.FirstYearSalary),''),'')
	, coalesce('Pro Rata Salary: ' + nullif(convert(varchar(max),p.GuaranteedSalary),''),'')
	, coalesce('Average Salary: ' + nullif(convert(varchar(max),p.AverageSalary),''),'')
	, coalesce('Fee Percentage: ' + nullif(convert(varchar(max),p.FeePercentage),''),'')
	, coalesce('Discount Percentage: ' + nullif(convert(varchar(max),p.DiscountPercentage),'0'),'')
	, coalesce('FeeDueValue: ' + nullif(convert(varchar(max),p.FeeDueValue),''),'')
	, coalesce('TotalDue: ' + nullif(convert(varchar(max),p.TotalDue),''),'')
	, coalesce('Contract Length: ' + nullif(convert(varchar(max),p.ContractLength),''),'')
	, coalesce('Length Period Type: ' + nullif(convert(varchar(max),p.LengthPeriodType),''),'')
	, coalesce('Manager Contact UID: ' + nullif(convert(varchar(max),p.ManagerContactUID),''),'')
	, coalesce('Account Contact UID: ' + nullif(convert(varchar(max),p.AccountContactUID),''),'')
	, coalesce('Timesheet Frequency: ' + nullif(p.TimesheetFrequency,''),'')
	) as Forward_placement_notes
, concat_ws(char(10)
	, coalesce('Agreed Payment Terms: ' + nullif(convert(varchar(max),p.AgreedPaymentTerms),0),'')
	, coalesce('Rebate Period: ' + nullif(convert(varchar(max),p.RebatePeriod),0),'')
	) as Forward_invoice_description
	, 0 as Forward_tax_rate
	, 'other' as Forward_export_data_to
	, 0 as Forward_net_total
	, 0 as Forward_other_invoice_items_total
	, 0 as Forward_invoice_total
from Placements p
left join Matches m on m.UniqueID = p.InterviewUniqueID
where m.VacancyUniqID > 0

--total: 2054