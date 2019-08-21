
-- COMMENT

/*
select C.candidateID, concat(C.FirstName,' ',C.LastName) from bullhorn1.Candidate C 
where C.isPrimaryOwner = 1
--and (FirstName = 'Mohammad' and lastname = 'Abusalah')
and C.candidateID = 213

*/

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
                C.candidateID ,UC.Userid
                , UC.dateAdded
                , coalesce('Date Added: ' + convert(varchar(10), UC.dateAdded, 120) + char(10) + 'Action: ' + NULLIF(cast(UC.comments as varchar(max)), ''), '') as note
        --select count(*) -- select top 1000 UC.comments 
        from bullhorn1.BH_UserComment UC
        left join bullhorn1.Candidate C on C.userID = UC.Userid where C.candidateID is not null and cast(UC.comments as varchar(max)) <> '' and (UC.userid = 111505  or  C.candidateID = 63685 )
UNION ALL
        SELECT --top 1000
                candidateID
                , dateAdded
                , coalesce('Date Added: ' + convert(varchar(10), dateAdded, 120) + char(10) + 'Candidate History: ' + NULLIF(convert(varchar(max),comments), ''), '') as summary
        --select count(*) --select top 1000 comments
        from bullhorn1.BH_CandidateHistory where comments is not null and cast(comments as varchar(max)) <> '' and  candidateID = 63685 )

--select count(*) from comments where note <> '' --544534
select top 10
                   candidateID
                  , cast('-10' as int) as user_account_id
                  , cast('4' as int) as contact_method
                  , cast('1' as int) as related_status
                  , dateAdded as feedback_timestamp_insert_timestamp
                  , note as comment_body
from comments where note <> '' and candidateID = 63685



	select --top 50
                  C.userID as '#userID'
		, case C.gender when 'M' then 'MR' when 'F' then 'MISS'	else '' end as 'candidate-title'
		, case C.gender when 'M' then 'MALE' when 'F' then 'FEMALE' else '' end as 'candidate-gender'
		, C.candidateID as 'candidate-externalId'
	from bullhorn1.Candidate C
	where C.isPrimaryOwner = 1 and userid = 111505
        
select top 200 UC.comments 
from bullhorn1.BH_UserComment UC
where UC.comments like '%[cc:%'
