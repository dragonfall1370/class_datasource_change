
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
--select * from ed

-- Latest Comment
, lc (userid,comments,dateAdded,rn) as ( SELECT userid, comments, dateAdded, r1 = ROW_NUMBER() OVER (PARTITION BY userid ORDER BY dateAdded desc) FROM bullhorn1.BH_UserComment )

-- NOTE
, note as (
	SELECT 
	          CA.userID
		 , Stuff( Coalesce('BH Candidate ID: ' + NULLIF(cast(CA.userID as varchar(max)), '') + char(10), '')  
                        + Coalesce('Email 2: ' + NULLIF(cast(e2.email as varchar(max)), '') + char(10), '')
                        + Coalesce('Email 3: ' + NULLIF(cast(e3.email as varchar(max)), '') + char(10), '')
                        + Coalesce('Languages: ' + NULLIF(convert(varchar(max),CA.customText1), '') + char(10), '')
                        + Coalesce('Job Type: ' + NULLIF(cast(CA.employeeType as varchar(max)), '') + char(10), '')
                        + Coalesce('Desired Job Type: ' + NULLIF(cast(CA.employmentPreference as varchar(max)), '') + char(10), '')
                        + Coalesce('Status: ' + NULLIF(convert(varchar(max),CA.Status), '') + char(10), '')
                        + coalesce('Latest Comment: ' + NULLIF([dbo].[udf_StripHTML](lc.comments), '') + char(10), '')
                        + Coalesce('General Comments: ' + NULLIF(convert(varchar(max),CA.comments), '') + char(10), '')
                        --+ coalesce('CV: ' + NULLIF([dbo].[fn_ConvertHTMLToText](UC1.description), '') + char(10), '')
                        + Coalesce('CV: ' + NULLIF([dbo].[udf_StripHTML](UW.description), '') , '') --cast(UW.description as varchar(max))
                        , 1, 0, '') as note
	-- select top 10 * -- select referredBy, referredByUserID -- select top 10 CA.userID, UW.description
	 from bullhorn1.Candidate CA --where convert(varchar(max),CA.comments) <> ''
        --left join e2 on CA.userID = e2.ID
        --left join e3 on CA.userID = e3.ID
        --left join (select * from lc where rn = 1) lc on lc.userid = CA.userid
        left join (SELECT userid, STUFF((
                        SELECT char(10) + description_truong + char(10) + '--------------------------------------------------' + char(10)
                        from bullhorn1.BH_UserWork where userid = a.userid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS description 
                        FROM (   select userid, description_truong
                                        from bullhorn1.BH_UserWork) AS a GROUP BY a.userid 
                        ) uw on uw.userid = ca.userid                
	where CA.isPrimaryOwner = 1 )--and UW.description is not null and ca.userid in (9,11,13) )
--select count(*) from note --63875
--select * from note --where AddedNote like '%Business Sector%'
--select top 100 * from note


	select --top 200
		 convert(varchar(max),C.candidateID) as 'candidate-externalId'
		, note.note as 'candidate-note' --<<
	-- select count (*) -- select distinct employeeType --employmentPreference -- select skillset, skillIDlist, customTextBlock1 --select top 10 *
	from bullhorn1.Candidate C --where C.isPrimaryOwner = 1 --8545
	left join note on C.userID = note.Userid --<<
	where C.isPrimaryOwner = 1