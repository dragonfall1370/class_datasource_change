
-- COMMENT

--select C.candidateID, C.FirstName,C.LastName from bullhorn1.Candidate C where FirstName = 'Mohammad' and lastname = 'Abusalah' --C.isPrimaryOwner = 1 and C.candidateID = 33

/*
with
  comment(candidateID, comment) as (
        SELECT    C.candidateID --,UC.Userid
                , STUFF( Coalesce('Date Added: ' + NULLIF(convert(varchar(10), UC.dateAdded, 120), '') + char(10), '') + Coalesce('Action: ' + NULLIF(cast(UC.comments as varchar(max)), '') + char(10), ''), 1, 0, '') as note
        from bullhorn1.BH_UserComment UC left join bullhorn1.Candidate C on C.userID = UC.Userid )
        --select count(*) from comment --35.508 37.583
        --select top 1000 * from comment

, summary(candidateID,summary) as (
        SELECT    candidateID
                , STUFF( Coalesce('Date Added: ' + NULLIF(convert(varchar(10), dateAdded, 120), '') + char(10), '') + coalesce('Candidate History: ' + NULLIF(convert(varchar(max),comments), ''), ''), 1, 0, '' ) as summary
        from bullhorn1.BH_CandidateHistory )
        --select count(*) from summary --105.793
        --select top 1000 * from summary
select count(*) from comment c,summary s where c.candidateID = s.candidateID --7833
*/

-----
with comments as (
        SELECT --top 1000
                C.candidateID, concat(C.FirstName,' ',C.LastName) as fullname --,UC.Userid
                , UC.dateAdded
                , coalesce('Date Added: ' + convert(varchar(10), UC.dateAdded, 120) + char(10) + 'Action: ' + NULLIF(cast(UC.comments as varchar(max)), ''), '') as note
        --select count(*) -- select top 1000 *
        from bullhorn1.BH_UserComment UC
        left join bullhorn1.Candidate C on C.userID = UC.Userid 
        where C.candidateID is not null and cast(UC.comments as varchar(max)) <> ''
UNION ALL
        SELECT --top 1000
                ch.candidateID ,C.fullname
                , ch.dateAdded
                , coalesce('Date Added: ' + convert(varchar(10), ch.dateAdded, 120) + char(10) + 'Candidate History: ' + NULLIF(convert(varchar(max),ch.comments), ''), '') as summary
        --select count(*) --35735-- select top 100 *
        from bullhorn1.BH_CandidateHistory CH
        left join ( select candidateID, concat(FirstName,' ',LastName) as fullname from bullhorn1.Candidate) C on C.candidateID = CH.candidateID 
        where ch.comments is not null and cast(ch.comments as varchar(max)) <> '' 
)

--select count(*) from comments where note <> '' --40135
--select * from comments where note <> '' and fullname like '%Philip%Levinson%'
select top 100
                   candidateID
                  , cast('-10' as int) as 'user_account_id'
                  --, cast('4' as int) as contact_method
                  --, cast('1' as int) as related_status
                  , dateAdded as insert_timestamp
                  , note as content
from comments where note <> '' --and candidateID = 33



---------------------- View_Appointment
with comments as (
	SELECT --top 20 
	C.candidateID --, a.candidateuserID, C.userID
	, a.dateAdded
		 , Stuff(  
                                   --Coalesce('Date Added: ' + NULLIF(cast(a.dateAdded as varchar(max)), '') + char(10), '')
                                   Coalesce('Date Begin: ' + NULLIF(cast(a.dateBegin as varchar(max)), '') + char(10), '')
                                + coalesce('Date End: ' + NULLIF(convert(varchar(max),a.dateEnd), '') + char(10), '')
                                + coalesce('Communication Method: ' + NULLIF(convert(varchar(max),a.communicationMethod), '') + char(10), '')
                                + coalesce('Type: ' + NULLIF(convert(varchar(max),a.type), '') + char(10), '') --CA.activePlacements
                                + coalesce('Subject: ' + NULLIF(convert(varchar(max),a.subject), '') + char(10), '')
                                + coalesce('Location: ' + NULLIF(convert(varchar(max),a.location), '') + char(10), '')
                                + coalesce('Description: ' + NULLIF( [dbo].[fn_ConvertHTMLToText](a.description), '') + char(10), '')
                                + coalesce('File Name: ' + NULLIF(convert(varchar(max),af.name), '') + char(10), '')
                        , 1, 0, '') as note 
        from bullhorn1.View_Appointment a
        left join bullhorn1.View_AppointmentFile af on af.appointmentID = a.appointmentID
        left join bullhorn1.Candidate C on C.userID = a.candidateuserID 
        where C.candidateID is not null 
)
--select count(*) from comments where note <> '' --40135
--select top 100 * from comments where note <> ''
select --top 100
                   candidateID
                  , cast('-10' as int) as 'user_account_id'
                  , 'comment' as category
                  , 'candidate' as type
                  , dateAdded as insert_timestamp
                  , note as content
from comments where note <> '' and candidateID = 2802


---------------------- View_Task
with comments as (
	SELECT --top 20 
	C.candidateID --, a.candidateuserID, C.userID
	, a.dateAdded
		 , Stuff(  
                                   --Coalesce('Date Added: ' + NULLIF(cast(a.dateAdded as varchar(max)), '') + char(10), '')
                                   Coalesce('Date Begin: ' + NULLIF(cast(a.dateBegin as varchar(max)), '') + char(10), '')
                                + coalesce('Date End: ' + NULLIF(convert(varchar(max),a.dateEnd), '') + char(10), '')
                                + coalesce('Communication Method: ' + NULLIF(convert(varchar(max),a.communicationMethod), '') + char(10), '')
                                + coalesce('Type: ' + NULLIF(convert(varchar(max),a.type), '') + char(10), '') --CA.activePlacements
                                + coalesce('Subject: ' + NULLIF(convert(varchar(max),a.subject), '') + char(10), '')
                                + coalesce('Location: ' + NULLIF(convert(varchar(max),a.location), '') + char(10), '')
                                + coalesce('Description: ' + NULLIF( [dbo].[fn_ConvertHTMLToText](a.description), '') + char(10), '')
                        , 1, 0, '') as note 
        from bullhorn1.View_Task a
        left join bullhorn1.Candidate C on C.userID = a.candidateuserID 
        where C.candidateID is not null 
)
select count(*) from comments where note <> '' --40135
--select top 100 * from comments where note <> ''
select --top 100
                   candidateID as 'candidate_ID'
                  , cast('-10' as int) as 'user_account_id'
                  , 'comment' as category
                  , 'candidate' as type
                  , dateAdded as insert_timestamp
                  , note as content
from comments where note <> '' and candidateID <= 100