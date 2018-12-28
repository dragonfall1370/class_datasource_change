select * from bullhorn1.Candidate C where c.userID in (680,563)

select count(*) from bullhorn1.BH_Candidate C where C.isPrimaryOwner = 1
select comments,* from bullhorn1.BH_CandidateHistory;
select comments,* from bullhorn1.Candidate where convert(varchar(max),comments) <> '';
select dateLastComment,* from bullhorn1.Candidate;
select comments,* from bullhorn1.View_Candidate;
select commenterUserName,* from bullhorn1.View_CandidateComment;
select comments,* from bullhorn1.View_CandidateComment;
select userCommentID,* from bullhorn1.View_UserCommentCandidate;


with 
-- MAIL
  mail1 (ID,email) as (select userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(ltrim(rtrim(email)),',',ltrim(rtrim(email2)),',',ltrim(rtrim(email3))),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' ') as email from bullhorn1.BH_UserContact )
, mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail5 (ID,email) as (SELECT ID, STUFF((SELECT DISTINCT ', ' + email from mail3 WHERE ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '')  AS email FROM mail3 as a GROUP BY a.ID)
--select * from mail5 where id in (39188,14248,30223)

--JOB DUPLICATION REGCONITION
, job (jobPostingID,clientID,title,starDate,rn) as (
	SELECT  a.jobPostingID as jobPostingID
		, b.clientID as clientID
		, a.title as title
		, CONVERT(VARCHAR(10),a.startDate,120) as starDate
		, ROW_NUMBER() OVER(PARTITION BY a.clientUserID,a.title,CONVERT(VARCHAR(10),a.startDate,120) ORDER BY a.jobPostingID) AS rn 
	from bullhorn1.BH_JobPosting a
	left join bullhorn1.BH_Client b on a.clientUserID = b.userID
	where b.isPrimaryOwner = 1) --> add isPrimaryOwner = 1 to remove 1 userID having more than 1 clientID
--select count(*) from job where title = ''

-- NOTE
, note as (
        select JP.jobPostingID
	, Stuff(  Coalesce('BH Job ID: ' + NULLIF(cast(JP.jobPostingID as varchar(max)), '') + char(10), '')
+ Coalesce('Assigned to: ' + NULLIF(cast(ass.name as varchar(max)), '') + char(10), '')
+ Coalesce('Client Charge Rate: ' + NULLIF(cast(JP.clientBillRate as varchar(max)), '') + char(10), '')
+ Coalesce('Perm Fee (%): ' + NULLIF(cast(JP.feeArrangement as varchar(max)), '') + char(10), '')
+ Coalesce('Pay Rate: ' + NULLIF(cast(JP.payRate as varchar(max)), '') + char(10), '')
+ Coalesce('Reason Closed: ' + NULLIF(cast(JP.reasonClosed as varchar(max)), '') + char(10), '')
+ Coalesce('Reports To: ' + NULLIF(cast(JP.reportToUserID as varchar(max)), '') + char(10), '')
+ Coalesce('Status: ' + NULLIF(cast(JP.status as varchar(max)), '') + char(10), '')
+ Coalesce('Priority: ' + NULLIF(cast(JP.type as varchar(max)), '') + char(10), '')
                --+ Coalesce('Address: ' + NULLIF(cast(JP.address as varchar(max)), '') + char(10), '')
                --+ Coalesce('Address: ' + NULLIF(cast(JP.fullAddress as varchar(max)), '') + char(10), '')
	        + Coalesce('Address: ' + NULLIF(cast(JP.address as varchar(max)), '') + char(10), '')
	        + Coalesce('City: ' + NULLIF(cast(JP.city as varchar(max)), '') + char(10), '')
	        + Coalesce('County: ' + NULLIF(cast(JP.state as varchar(max)), '') + char(10), '')
	        + Coalesce('Zip: ' + NULLIF(cast(JP.zip as varchar(max)), '') + char(10), '')
	        + Coalesce('Country: ' + NULLIF(cast(tmp_country.COUNTRY as varchar(max)) + char(10), ''), '')
	        /*+ Coalesce('Status: ' + NULLIF(JP.Status, '') + char(10), '')
	        + Coalesce('Position type: ' + NULLIF(cast(JP.type as varchar(max)), '') + char(10), '')
	        + Coalesce('Employment Type: ' + NULLIF(cast(JP.employmentType as varchar(max)), '') + char(10), '')
	        --+ Coalesce('Priority: ' + NULLIF(cast(JP.type as varchar(max)), '') + char(10), '')
	        + Coalesce('Salary: ' + NULLIF(cast(JP.salary as varchar(max)), '') + char(10), '')
	        + Coalesce('Fee arrangement: ' + NULLIF(cast(JP.feeArrangement as varchar(max)), '') + char(10), '')
	        --+ Coalesce('Publish Category: ' + NULLIF(cast(JP.publishedCategoryID as varchar(max)), '') + char(10), '')
	        --+ Coalesce('Required skills: ' + NULLIF(cast(JP.skills as varchar(max)), '') + char(10), '')
	        + Coalesce('Years required: ' + NULLIF(cast(JP.yearsRequired as varchar(max)), '') + char(10), '')
	        + Coalesce('Start Date: ' + NULLIF(convert(varchar(10),JP.startdate,120), '') + char(10), '')
	        + Coalesce('Company address: ' + NULLIF(CC.address1, '') + char(10), '')
	        --+ Coalesce('Client Corporation ID: ' + NULLIF(cast(CC.clientCorporationID as varchar(max)), '') + char(10), '')
	        + Coalesce('isOpen: ' + NULLIF(cast(JP.isOpen as varchar(max)), '') + char(10), '')
	        + Coalesce('SSOC code: ' + NULLIF(cast(JP.customInt1 as varchar(max)), '') + char(10), '')
	        --+ Coalesce('Skills / Experience: ' + NULLIF(cast(JP.skillsInfoHeader as varchar(max)), '') + char(10), '')
	        + Coalesce('Keyword: ' + NULLIF(cast(JP.skills as varchar(max)), '') + char(10), '')
	        + Coalesce('Degree Requirements: ' + NULLIF(cast(JP.degreeList as varchar(max)), '') + char(10), '')
	        + Coalesce('Certifications: ' + NULLIF(cast(JP.certifications as varchar(max)), '') + char(10), '')
	        + Coalesce('Benefits: ' + NULLIF(cast(JP.Benefits as varchar(max)), '') + char(10), '')
	        --+ Coalesce('Job Location: ' + NULLIF(cast(JP.locationInfoHeader as varchar(max)), '') + char(10), '')
	        + Coalesce('MaximumSalary: ' + NULLIF(cast(JP.customFloat1 as varchar(max)), '') + char(10), '')
	        + Coalesce('Salary Notes: ' + NULLIF(cast(JP.customTextBlock1 as varchar(max)), '') + char(10), '')
	        + Coalesce('Benefits: ' + NULLIF(cast(JP.customTextBlock3 as varchar(max)), '') + char(10), '')
	        + Coalesce('NumOpenings: ' + NULLIF(cast(JP.numOpenings as varchar(max)), '') + char(10), '')
	        + Coalesce('Kick-Off Date: ' + NULLIF(cast(JP.customDate3 as varchar(max)), '') + char(10), '')
	        + Coalesce('RC Comment: ' + NULLIF(cast(JP.customTextBlock2 as varchar(max)), '') + char(10), '')
	        + Coalesce('Social Media Snippet: ' + NULLIF(cast(JP.customTextBlock4 as varchar(max)), '') + char(10), '')
	        + Coalesce('Exclusive?: ' + NULLIF(cast(JP.customText16 as varchar(max)), '') + char(10), '')
  */
	        , 1, 0, '') as note
        -- select count(*) -- select top 50 *
        from bullhorn1.BH_JobPosting JP --where cast(skills as varchar(max)) <> ''
        left join bullhorn1.BH_CategoryList CL on JP.publishedCategoryID = CL.categoryID
        left join bullhorn1.BH_ClientCorporation CC on JP.clientCorporationID = CC.clientCorporationID
        left join tmp_country on JP.countryID = tmp_country.CODE
        left join ( SELECT jobPostingID, STUFF((SELECT ',' + uc.name from bullhorn1.BH_JobAssignment ja left join bullhorn1.BH_UserContact UC on UC.userID = ja.userID where ja.jobPostingID = a.jobPostingID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS name FROM ( select ja.jobPostingID,uc.name  from bullhorn1.BH_JobAssignment ja left join bullhorn1.BH_UserContact UC on UC.userID = ja.userID ) AS a GROUP BY a.jobPostingID ) ass on ass.jobPostingID = JP.jobPostingID
        )
--select count(*) from note --918 > 1103
select * from note