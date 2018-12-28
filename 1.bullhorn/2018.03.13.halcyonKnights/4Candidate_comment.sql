
--select C.candidateID, C.FirstName,C.LastName from bullhorn1.Candidate C where FirstName = 'Mohammad' and lastname = 'Abusalah' --C.isPrimaryOwner = 1 and C.candidateID = 33


with comments as (
        SELECT --top 1000
                  C.candidateID, C.fullname --,UC.Userid
                , UC.dateAdded
                , coalesce('COMMENT: ' + char(10)
		                  + 'Date Added: ' + convert(varchar(10), UC.dateAdded, 120) + char(10) 
		                  + 'Comments: ' + NULLIF(cast(UC.comments as varchar(max)), ''), '') 
		                  as note 
        -- select count(*) --12292
        -- select top 100 *
        from bullhorn1.BH_UserComment UC
        left join ( select candidateID, concat(FirstName,' ',LastName) as fullname, userid, isPrimaryOwner from bullhorn1.Candidate) C on C.userID = UC.Userid 
        where C.isPrimaryOwner = 1 and C.candidateID is not null and cast(UC.comments as varchar(max)) <> ''
UNION 
        SELECT --top 1000
                  ch.candidateID ,C.fullname --,UC.Userid
                , ch.dateAdded
                , coalesce('HISTORY: ' + char(10)
		                  + 'Date Added: ' + convert(varchar(10), ch.dateAdded, 120) + char(10) 
		                  + 'Candidate History: ' + NULLIF(convert(varchar(max),ch.comments), ''), '') 
		                  as summary
        -- select count(*) --138
        -- select top 100 *
        from bullhorn1.BH_CandidateHistory CH
        left join ( select candidateID, concat(FirstName,' ',LastName) as fullname, userid, isPrimaryOwner from bullhorn1.Candidate) C on C.candidateID = CH.candidateID 
        where C.isPrimaryOwner = 1 and C.candidateID is not null and ch.comments is not null and cast(ch.comments as varchar(max)) <> '' 
UNION
	SELECT --top 20 
	C.candidateID ,C.fullname  --, a.candidateuserID, C.userID
	, a.dateAdded
		 , Stuff(       'APPOINTMENT: ' + char(10)
		                  + Coalesce('Date Begin: ' + NULLIF(cast(a.dateBegin as varchar(max)), '') + char(10), '')
                                + coalesce('Date End: ' + NULLIF(convert(varchar(max),a.dateEnd), '') + char(10), '')
                                + coalesce('Communication Method: ' + NULLIF(convert(varchar(max),a.communicationMethod), '') + char(10), '')
                                + coalesce('Type: ' + NULLIF(convert(varchar(max),a.type), '') + char(10), '') --CA.activePlacements
                                + coalesce('Subject: ' + NULLIF(convert(varchar(max),a.subject), '') + char(10), '')
                                + coalesce('Location: ' + NULLIF(convert(varchar(max),a.location), '') + char(10), '')
                                + coalesce('Description: ' + NULLIF( [dbo].[fn_ConvertHTMLToText](a.description), '') + char(10), '')
                                + coalesce('File Name: ' + NULLIF(convert(varchar(max),af.name), '') + char(10), '')
                        , 1, 0, '') as note 
        -- select count(*) --2062
        -- select top 100 *
        from bullhorn1.View_Appointment a
        left join bullhorn1.View_AppointmentFile af on af.appointmentID = a.appointmentID
        left join ( select candidateID, concat(FirstName,' ',LastName) as fullname, userid, isPrimaryOwner from bullhorn1.Candidate) C on C.userID = a.candidateuserID 
        where C.isPrimaryOwner = 1 and C.candidateID is not null 
UNION
	SELECT --top 20 
	C.candidateID,C.fullname  --, a.candidateuserID, C.userID
	, a.dateAdded
		 , Stuff(       'TASK: ' + char(10)
		                  + Coalesce('Date Begin: ' + NULLIF(cast(a.dateBegin as varchar(max)), '') + char(10), '')
                                + coalesce('Date End: ' + NULLIF(convert(varchar(max),a.dateEnd), '') + char(10), '')
                                + coalesce('Communication Method: ' + NULLIF(convert(varchar(max),a.communicationMethod), '') + char(10), '')
                                + coalesce('Type: ' + NULLIF(convert(varchar(max),a.type), '') + char(10), '') --CA.activePlacements
                                + coalesce('Subject: ' + NULLIF(convert(varchar(max),a.subject), '') + char(10), '')
                                + coalesce('Location: ' + NULLIF(convert(varchar(max),a.location), '') + char(10), '')
                                + coalesce('Description: ' + NULLIF( [dbo].[fn_ConvertHTMLToText](a.description), '') + char(10), '')
                        , 1, 0, '') as note 
        -- select count(*) --24
        -- select top 100 *        
        from bullhorn1.View_Task a
        left join ( select candidateID, concat(FirstName,' ',LastName) as fullname, userid, isPrimaryOwner from bullhorn1.Candidate) C on C.userID = a.candidateuserID 
        where C.isPrimaryOwner = 1 and C.candidateID is not null 
)


select count(*) from comments where note <> '' --12428
select --top 100
                   candidateID
                  , cast('-10' as int) as 'user_account_id'
                  , 'comment' as 'category'
                  , 'candidate' as 'type'
                  , dateAdded as 'insert_timestamp'
                  , note as 'content'
from comments where note <> '' 
--and candidateID = 2988 or fullname like '%Philip%'

