
with 
-------------------------------------------------------------CALL BACKS
 -- select top 10 * from dCandidateCallBack
 tempCallBack as (
	select cb.intCandidateId, cb.dtInserted, intCandidateCallBackId, cb.vchCallBackDetail, cb.bitActive, cb.datCallBackDate, cbt.vchCallBackTypeName
						, ROW_NUMBER() OVER(PARTITION BY cb.intCandidateId ORDER BY intCandidateCallBackId ASC) AS rn
	from dCandidateCallBack cb 
	left join dCandidate c on cb.intCandidateId = c.intCandidateId
       left join refCallBackType cbt on cb.tintCallBackTypeId = cbt.tintCallBackTypeId
	where cb.bitActive = 1 
	and c.intCandidateId  is not null 
	--and c.intCandidateId = 8588
	)
--select * from  tempCallBack

, tempCallBack1 as (
select intCandidateId, dtInserted,
       STUFF(
                iif(datCallBackDate is NULL,'',concat('Call Back Date: ',datCallBackDate,char(10)))
              + iif(vchCallBackDetail = '' or vchCallBackDetail is NULL,'',concat('Detail: ',vchCallBackDetail,char(10)))
              + iif(vchCallBackTypeName = '' or vchCallBackTypeName is NULL,'',concat('Type: ',vchCallBackTypeName,char(10)))
              --, iif(bitActive = '' or bitActive is NULL,'',concat('  Flag (Active): ',bitActive,char(10)))
              --, concat('Entity: Contact', char(10))
	 ,1,0, '')  AS callBackInfo
from tempCallBack
--where intCandidateId = 8588
)
--select * from tempCallBack1

/*, CandiddateCallBack as (SELECT intCandidateId,
     STUFF(
         (SELECT char(10) + callBackInfo
          from  tempCallBack1
          WHERE intCandidateId =cb1.intCandidateId
    order by intCandidateId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,1, '')  AS callBackInfo
FROM tempCallBack1 as cb1
GROUP BY cb1.intCandidateId)
select * from CandiddateCallBack
*/

-------------------------------------------------------------MAIN SCRIPT
--insert into ImportCandidate
select
         c.intCandidateId as external_Id -- p.intPersonId  as PersonId --just for reference afterward
--       , iif(rtrim(ltrim(p.vchForename)) = '' or rtrim(ltrim(p.vchForename)) is null, 'No Firstname', rtrim(ltrim(p.vchForename))) as 'candidate-firstName'
--       , iif(rtrim(ltrim(p.vchSurName)) = '' or rtrim(ltrim(p.vchSurName)) is null, concat('Lastname-', c.intCandidateId), rtrim(ltrim(p.vchSurName))) as 'candidate-Lastname'
       , cast('-10' as int) as 'user_account_id'
       , 'comment' as 'category'
       , 'candidate' as 'type'
       , ccb.dtInserted as 'insert_timestamp'       
       , ccb.CallBackInfo as 'content'
-- select count(*) --22568-- select distinct rn.vchNationalityName
from dCandidate c
left join dPerson p on c.intPersonId = p.intPersonId
left join tempCallBack1 ccb on c.intCandidateId = ccb.intCandidateId
where ccb.CallBackInfo <> ''

