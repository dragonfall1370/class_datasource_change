select PL.jobPostingID as jobPostingID
	--, PL.candidateid as Sirius_CandExtID
	, convert(datetime,PL.dateBegin,121) as dateBegin
	, case when PL.dateEnd is not NULL or PL.dateEnd <> '' then convert(datetime,PL.dateEnd,121) else NULL end as dateEnd
		, Stuff(
	            Coalesce('JobID: ' + cast(PL.jobPostingID as varchar(max)) + char(10), '')
	        + Coalesce('Date Added: ' + cast(PL.dateAdded as varchar(max)) + char(10), '')
	        + Coalesce('Employment Type: ' + NULLIF(PL.employmentType, '') + char(10), '')
	        + Coalesce('Status: ' + NULLIF(PL.status, '') + char(10), '')
	        + Coalesce('Base Pay Rate: ' + NULLIF(cast(PL.payRate as varchar(max)), '') + char(10), '')
	        + Coalesce('Charge Rate: ' + NULLIF(cast(PL.clientBillRate as varchar(max)), '') + char(10), '')
	        + Coalesce('Custom Text 3: ' + NULLIF(cast(PL.customText3 as varchar(max)), '') + char(10), '')
	        + Coalesce('Report to: ' + NULLIF(cast(PL.reportTo as varchar(max)), '') + char(10), '')
	        + Coalesce('Work Start Date: ' + NULLIF(cast(PL.dateBegin as varchar(max)), '') + char(10), '')
	        + Coalesce('Work End Date: ' + NULLIF(cast(PL.dateEnd as varchar(max)), '') + char(10), '')
	        + Coalesce('Comments: ' + NULLIF(cast(PL.comments as varchar(max)), '') + char(10), '')
	        , 1, 0, '') as note
       , a.clientUserID as '#UserID', uc.firstname as '#ContactFirstName', uc.lastname as '#ContactLastName', CC.clientCorporationID as '#CompanyID', cc.name as '#CompanyName'
from bullhorn1.BH_JobPosting a --where a.jobPostingID in (544,843,725,964,1109,1225,1323,1409,1444,1471,1540) --(76938, 100453, 120112)
left join bullhorn1.BH_Client b on a.clientUserID = b.userID
left JOIN bullhorn1.BH_ClientCorporation CC ON b.clientCorporationID = CC.clientCorporationID
left join bullhorn1.BH_UserContact UC on a.clientUserID = UC.userID
left join bullhorn1.BH_Placement PL on PL.jobPostingID = a.jobPostingID
--left join mail5 ON a.userID = mail5.ID
--where b.isPrimaryOwner = 1 --> add isPrimaryOwner = 1 to remove 1 userID having more than 1 clientID
where cc.name like '%AppDynamics%'
and PL.jobPostingID is not null --and a.jobPostingID in (1,164,178,36)
--order by a.jobPostingID asc
