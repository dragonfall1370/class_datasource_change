select * from VCContacts
where [contact-externalId] = '00320000004d9LZAAY'

select * from VCContacts
where [contact-email] like '%james@enterprisesp.co.uk%' or [contact-email] like '%egersdorff@hotmail.co.uk%'

select * from Contact x
where Email like '%mike.rogan@udgroup.co.uk%' or Email like '%mike@mosaicpe.com%'
or Email like '%james@enterprisesp.co.uk%' or Email like '%egersdorff@hotmail.co.uk%'
or Email like '%steven.skakel@nextiraone.co.uk%' or Email like '%hilary.hart@nextiraone.co.uk%'
or Email like '%helen.artlett-coe@crimson.co.uk%' or Email like '%charteris.cv@crimson.co.uk%'
or Email like '%donmclau@cisco.com%' or Email like '%linelson@cisco.com%'
or Email like '%martin.doherty@ingenico.com%' or Email like '%jackie.houston@ingenico.com%'
or Email like '%rawcliffer@aol.com%' or Email like '%richard.rawcliffe@tunstall.co.uk%'
or Email like '%francesca.haines@qas.com%' or Email like '%careers@qas.com%'
or Email like '%mmclean@intrinsic.co.uk%' or Email like '%mark.mclean@outlook.com%'
or Email like '%brian.bannatyne@response-uk.co.uk%' or Email like '%michelle.mcdermott@response-uk.co.uk%'
or Email like '%dave.charlton@logica.com%' or Email like '%david.charlton913@btinternet.com%'
or Email like '%jemma.coppleston@atos.net%' or Email like '%jcopleston@thesjbgroup.com%'
or Email like '%davidjanson24@gmail.com%' or Email like '%dijanson@hotmail.com%'
or Email like '%martin.slowey@arnlea.com%' or Email like '%martin.j.slowey@gmail.com%'
or Email like '%scott@parkersoftware.com%' or Email like '%sbarnsley@live.co.uk%'
or Email like '%ashish.devalekar@capgemini.com%' or Email like '%judith.lobo@capgemini.com%'
or Email like '%jessie.graham@ingenico.com%' or Email like '%reception@ingenico.com%'
or Email like '%smoor@checkpoint.com%' or Email like '%ehomewood@checkpoint.com%'
order by x.CreatedDate


select * from [User]
where Id in (
'00520000000jgDcAAI'
, '00520000000voDKAAY'
)

--select * from Account where Id = '000000000000000AAA'

select * from VCContacts

select concat('abc', 1)

select x.* from
VCCanIdxs cis -- 17476
left join Contact x on cis.Id = x.Id

select
Company_Name__c
, Company_Name_3__c
, Company_4__c
, Company_5_Name_Position_Dates_Basic__c
, Type_of_company__c
, Compamy_Name__c
, Compamy_6_Name_Dates_Position_Basic__c

from Contact
where Id = '0030O00001w2huPQAQ'

select count(*) from Opportunity

select * from Opportunity

select * from [VCCandidates]

select
c.Id
--, Date_Joined__c
--, Date_Employment_ended__c
--, Date_Joined_2__c
--, Date_employment_ended_2__c
, Date_joined_3__c
--, Date_employment_ended_3__c
from VCCanIdxs cis
left join Contact c on cis.Id = c.Id

BEGIN TRY  
    -- Generate divide-by-zero error.  
    SELECT 1/0;  
END TRY  
BEGIN CATCH  
    -- Execute error retrieval routine.  
    EXECUTE usp_GetErrorInfo;  
END CATCH;   

select
c.Id
, trim(isnull(convert(varchar(50), cast(Date_Joined__c as datetime), 111), ''))
, trim(isnull(convert(varchar(50), cast(Date_Employment_ended__c as datetime), 111), ''))
, trim(isnull(convert(varchar(50), cast(Date_Joined_2__c as datetime), 111), ''))
, trim(isnull(convert(varchar(50), cast(Date_employment_ended_2__c as datetime), 111), ''))
, iif(ISDATE(Date_joined_3__c) = 1, trim(isnull(convert(varchar(50), cast(Date_joined_3__c as datetime), 111), '')), '')
, trim(isnull(convert(varchar(50), cast(Date_employment_ended_3__c as datetime), 111), ''))
from VCCanIdxs cis
left join Contact c on cis.Id = c.Id



--Company_Name__c
--Aspect Software

--Company_Name_3__c
--Sugar CRM

--Compamy_Name__c
--Gartner

select
Id
, FirstName
, LastName
, Reports_to_1__c
, Reports_to_2__c
, Who_report_to_3__c
from Contact
where len(trim(isnull(Reports_to_1__c, ''))) > 0
or len(trim(isnull(Reports_to_2__c, ''))) > 0
or len(trim(isnull(Who_report_to_3__c, ''))) > 0


select * from VCJobs

select * from VCCandidates

select * from Opportunity
where Id in (
'0062000000SgcEtAAJ'
, '0062000000ZOIt0AAH'
)

select * from OpportunityContactRole
where OpportunityId in (
'0062000000SgcEtAAJ'
, '0062000000ZOIt0AAH'
)


select * from Account where Id = '001200000035uJOAAY'

select * from Contact where AccountId = '001200000035uJOAAY'

select * from VCJobs where len(isnull([position-contactId], '')) > 0

select [candidate-citizenship] from VCCandidates

select * from VCCandidates

select [candidate-externalId], [candidate-email] from VCCandidates
where [candidate-externalId] = '0032000000zw4gQAAQ' or [candidate-email] = 'NoEmail-1447665@noemail.com'

select distinct [candidate-email] from VCCandidates

select distinct [candidate-externalId] from VCCandidates

select [dbo].[ufn_GetHashCode]('0032000000zw4gQAAQ')
select [dbo].[ufn_GetHashCode]('0030O000026bHYxQAM')
select [dbo].[ufn_GetHashCode]('0032000000YfLvNAAV')
select [dbo].[ufn_GetHashCode]('00320000015fXznAAE')

select [dbo].[ufn_GetHashCode](HashBytes('MD5', '0032000000zw4gQAAQ'))
select [dbo].[ufn_GetHashCode](HashBytes('MD5', '0030O000026bHYxQAM'))
select [dbo].[ufn_GetHashCode](HashBytes('MD5', '0032000000YfLvNAAV'))
select [dbo].[ufn_GetHashCode](HashBytes('MD5', '00320000015fXznAAE'))

select CHECKSUM('0032000000zw4gQAAQ')
select CHECKSUM('0030O000026bHYxQAM')
select CHECKSUM('0032000000YfLvNAAV')
select CHECKSUM('00320000015fXznAAE')
