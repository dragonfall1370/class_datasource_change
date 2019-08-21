

with comments as (
	SELECT --top 20
                 jobPostingID
              , dateAdded
	      , Stuff( Coalesce('Date Added: ' + NULLIF(cast(PL.dateAdded as varchar(max)), '') + char(10), '') + Coalesce('Comments: ' + NULLIF(cast(PL.comments as varchar(max)), '') + char(10), '')
                       , 1, 0, '') as content 
	-- select *
        from bullhorn1.BH_Placement PL where cast(PL.comments as varchar(max)) <> ''
UNION
	SELECT --top 20 
	         j.jobPostingID
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
                        , 1, 0, '') as content 
        -- select *
        from bullhorn1.View_Task a
        left join bullhorn1.BH_JobPosting j on j.jobPostingID = a.jobPostingID 
        where j.jobPostingID is not null 
)


--select count(*) from comments where content <> '' --13
--select top 100 * from comments where note <> ''
select --top 100
                   jobPostingID as 'jobPostingID'
                  , cast('-10' as int) as 'user_account_id'
                  , 'comment' as category
                  , 'job' as type
                  , dateAdded as insert_timestamp
                  , content as content
from comments where content <> ''


