

with
-- EMAIL
  mail1 (ID,email) as (select UC.userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(UC.email,',',UC.email2,',',UC.email3),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),CHAR(9),' ') as mail from bullhorn1.BH_UserContact UC left join bullhorn1.Candidate C on C.userID = UC.UserID where UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%' and C.isPrimaryOwner = 1 )
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, e1 (ID,email) as (select ID, email from mail4 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 (ID,email) as (select ID, email from mail4 where rn = 2)
, e3 (ID,email) as (select ID, email from mail4 where rn = 3)
, e4 as (select ID, email from mail4 where rn = 4)
--select * from ed


-- NOTE
, note as (
	SELECT CA.candidateID
		 , Stuff( Coalesce('User ID: ' + NULLIF(cast(CA.userID as varchar(max)), '') + char(10), '') 
+ Coalesce('Other email: ' + NULLIF(cast(e3.email as varchar(max)), '') + char(10), '')
+ Coalesce('General Comments: ' + NULLIF(convert(varchar(max),CA.comments), '') + char(10), '')
+ Coalesce('Pay Type: ' + NULLIF(cast(ca.customText10 as varchar(max)), '') + char(10), '')
+ Coalesce('Date Available: ' + NULLIF(cast(ca.dateAvailable as varchar(max)), '') + char(10), '')
+ Coalesce('Desired Locations: ' + NULLIF(cast(ca.desiredLocations as varchar(max)), '') + char(10), '')
+ Coalesce('Employee Type: ' + NULLIF(cast(ca.employeeType as varchar(max)), '') + char(10), '')
+ Coalesce('Employment Preference: ' + NULLIF(cast(ca.employmentPreference as varchar(max)), '') + char(10), '')
+ Coalesce('Fax: ' + NULLIF(cast(ca.fax as varchar(max)), '') + char(10), '')
+ Coalesce('Desired Pay Rate: ' + NULLIF(cast(ca.hourlyRate as varchar(max)), '') + char(10), '')
+ Coalesce('Current Pay Rate: ' + NULLIF(cast(ca.hourlyRateLow as varchar(max)), '') + char(10), '')
--+ Coalesce('Latest Comment: ' + NULLIF(cast(ca.latestComment as varchar(max)), '') + char(10), '')
+ Coalesce('Referred By (Other): ' + NULLIF(cast(ca.referredBy as varchar(max)), '') + char(10), '')
+ Coalesce('Referred By: ' + NULLIF(convert(varchar(max),CA.referredByUserID), '') + ' - ' + UC.firstname + ' ' + UC.lastname + char(10), '') ---+ Coalesce('Referred By: ' + NULLIF(cast(ca.referredByUserID as varchar(max)), '') + char(10), '')
+ Coalesce('Additonal Skills (CV parser): ' + NULLIF(cast(ca.skillSet as varchar(max)), '') + char(10), '')
+ Coalesce('Status: ' + NULLIF(cast(ca.status as varchar(max)), '') + char(10), '')
+ Coalesce('Willing to Relocate: ' + NULLIF(cast(ca.willRelocate as varchar(max)), '') + char(10), '')
-- Candidate Tax Info
+ Coalesce('I9 Expiration Date: ' + NULLIF(cast(ca.dateI9Expiration as varchar(max)), '') + char(10), '')
+ Coalesce('Federal Additional Withholdings Amount: ' + NULLIF(cast(ca.federalAddtionalWitholdingsAmount as varchar(max)), '') + char(10), '')
+ Coalesce('Federal Exemptions: ' + NULLIF(cast(ca.federalExemptions as varchar(max)), '') + char(10), '')
+ Coalesce('Federal Filing Status: ' + NULLIF(cast(ca.federalFilingStatus as varchar(max)), '') + char(10), '')
+ Coalesce('I9 On File: ' + NULLIF(cast(ca.I9OnFile as varchar(max)), '') + char(10), '')
+ Coalesce('Local Additional Withholdings Amount: ' + NULLIF(cast(ca.localAddtionalWitholdingsAmount as varchar(max)), '') + char(10), '')
+ Coalesce('Local Exemptions: ' + NULLIF(cast(ca.localExemptions as varchar(max)), '') + char(10), '')
+ Coalesce('Local Filing Status: ' + NULLIF(cast(ca.localFilingStatus as varchar(max)), '') + char(10), '')
+ Coalesce('Local Tax Code: ' + NULLIF(cast(ca.localTaxCode as varchar(max)), '') + char(10), '')
+ Coalesce('SSN: ' + NULLIF(cast(ca.ssn as varchar(max)), '') + char(10), '')
+ Coalesce('State Additional Withholdings Amount: ' + NULLIF(cast(ca.stateAddtionalWitholdingsAmount as varchar(max)), '') + char(10), '')
+ Coalesce('State Exemptions: ' + NULLIF(cast(ca.stateExemptions as varchar(max)), '') + char(10), '')
+ Coalesce('State Filing Status: ' + NULLIF(cast(ca.stateFilingStatus as varchar(max)), '') + char(10), '')
+ Coalesce('Tax ID: ' + NULLIF(cast(ca.taxID as varchar(max)), '') + char(10), '')
+ Coalesce('Tax State: ' + NULLIF(cast(ca.taxState as varchar(max)), '') + char(10), '')      
+ Coalesce('CV: ' + NULLIF(UW.description, '') , '') --+ Coalesce('CV: ' + NULLIF(cast(ca.description as varchar(max)), '') + char(10), '')                  
-- CandidateCertification (EMPTY)
-- Reference (EMPTY)
                        , 1, 0, '') as note
	-- select top 10 * -- select count(*) -- select UW.description , [dbo].[udf_StripHTML](UW.description) as test --referredBy, referredByUserID
	from bullhorn1.Candidate CA --where CA.isPrimaryOwner = 1 --where convert(varchar(max),CA.comments) <> ''
       left join e3 on CA.userID = e3.ID
	left join ( select userid, firstname, lastname from bullhorn1.BH_UserContact )UC ON UC.userID = CA.referredByUserID
	--left join tmp_country on cast(CA.desiredLocations as varchar(2)) = tmp_country.ABBREVIATION
	--left join SkillName SN on CA.userID = SN.userId
	--left join BusinessSector BS on CA.userID = BS.userId
        --left join bullhorn1.BH_BusinessSectorList BSL on cast(CA.businessSectorIDList as varchar(max)) = cast(BSL.businessSectorID as varchar(max))
        --left join admission AD on CA.userID = AD.Userid
        --left join CName on CA.userID = CName.Userid
        --left join SpeName on CA.userID = SpeName.Userid
        --left join mail5 on CA.userID = mail5.ID
        --left join summary on CA.userID = summary.CandidateID
        --left join (select userid, status from bullhorn1.BH_Placement ) pm on pm.userid = ca.userid
        --left join owner2c on owner2c.userid = CA.userid
        --left join wr1 on wr1.userid = CA.userid
        --left join (select * from lc where rn = 1) lc on lc.userid = CA.userid
        left join (SELECT userid, STUFF((
                        SELECT char(10) + NULLIF(description_truong, '') + char(10) + '--------------------------------------------------' + char(10)
                        from bullhorn1.BH_UserWork where userid = a.userid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS description 
                        FROM (   select userid, description_truong
                                        from bullhorn1.BH_UserWork) AS a GROUP BY a.userid 
                        ) uw on uw.userid = ca.userid                
	where CA.isPrimaryOwner = 1 )


select count(*) from note --8545
--select * from note --where AddedNote like '%Business Sector%'
--select top 100 * from note



-- select * from bullhorn1.BH_UserCertification where licenseNumber is not null;
-- select referenceTitle,* from bullhorn1.BH_UserReference where referenceTitle is not null;