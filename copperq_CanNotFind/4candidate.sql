
select    ca.CanID as 'candidate-externalId(origin)'
        , concat(ca.CanID,"copperq") as 'candidate-externalId'
	#, ca.Title as 'candidate-title'
        , case when (ca.Title like '%MR' OR ca.Title like '%MRS' OR ca.Title like '%MS' OR ca.Title like '%MISS' OR ca.Title like '%DR') THEN upper(ca.Title) ELSE '' END as 'candidate-title'
	, case when ca.Sex='-1' then 'MALE'
	       when ca.Sex='0' then 'FEMALE'
          END as 'candidate-gender'
	#, ca.FirstName as 'candidate-firstName'
        , case when (ca.FirstName = '' OR ca.FirstName is NULL) THEN 'No Firstname' ELSE ca.FirstName END as 'contact-firstName'
	#, ca.Surname as 'candidate-Lastname'
        , case when (ca.Surname = '' OR ca.Surname is NULL) THEN 'No Lastname' ELSE ca.Surname END as 'contact-lastName'

	, ca.MiddleName as 'candidate-middleName'
	, left(ca.DOB,10) as 'candidate-dob'
	, 'ZA' as 'candidate-citizenship'
	#, ca.email as 'candidate-email'
        , case when (ca.email is not null and ca.email <> '' and ca.email like '%@%') then ca.email else ''	end as 'candidate-email'
	, ca.Mobile as 'candidate-mobile'
	, ca.Tel as 'candidate-phone'	
	, ca.Work as 'candidate-workPhone'
	, concat(ca.Address
		, case when (ca.Add2 = '' OR ca.Add2 is NULL) THEN '' ELSE concat(', ',ca.Add2) END
		, case when (ca.Add3 = '' OR ca.Add3 is NULL) THEN '' ELSE concat(', ',ca.Add3) END
		, case when (ca.Add4 = '' OR ca.Add4 is NULL) THEN '' ELSE concat(', ',ca.Add4) END
		, case when (ca.Add5 = '' OR ca.Add5 is NULL) THEN '' ELSE concat(', ',ca.Add5) END
		, case when (ca.PostCode = '' OR ca.PostCode is NULL) THEN '' ELSE concat(', PostCode: ',ca.PostCode) END
		) as 'candidate-address'
    	, ca.add3 as 'candidate-city'
	, ca.add5 as 'candidate-state'
	, ca.postcode as 'candidate-zipCode'
	, 'ZA' as 'candidate-Country'
	#, ca.minsal as 'candidate-currentSalary'
	#, as 'candidate-desiredSalary'
	
	#, case when (c.employeeType = '' OR c.employeeType is NULL) THEN '' ELSE concat('Employment Type / Job Type: ',c.employeeType,char(10)) END as 'candidate-employeeType'
	, case when ca.tempperm=1 then 'TEMPORARY_TO_PERMANENT'
               when ca.tempperm=2 then 'PERMANENT'
               when ca.tempperm=3 then 'TEMPORARY'
               end as 'candidate-jobtype1' -- This field only accepts: PERMANENT,INTERIM_PROJECT_CONSULTING,TEMPORARY,CONTRACT, TEMPORARY_TO_PERMANENT
        #, UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(tmp_EP.employmentPreference,'Permanent','FULL_TIME'),'Freelance','PART_TIME'),'Part-time','PART_TIME'),'Contract','CASUAL'),'Remote','CASUAL'),'Owner/Partner','CASUAL')) as 'candidate-employmentPreference'

	#, skill.name as 'skills'
	#, as 'candidate-schoolName'
	#, as 'candidate-graduationDate'
	, concat(ca.education,' ',ca.OtherInfo) as 'candidate-degreeName'
	#, as 'candidate-gpa'
	, u.udf1 as 'candidate-owners'
	
	#, cf.filename as 'candidate-resume-filename' #filename #NO
	, tr.name as 'candidate-resume' #IDname #NO
	, pb.FileName as 'candidate-photo'
	
	, ca.fax
        , ca.url as 'candidate-profile'

        , et1.JobTitle as 'candidate-employer1'
        , et1.JobTitle as 'candidate-jobtitle'
        , et1.JobTitle as 'candidate-company1'
	, left(et1.StartDate,10) as 'candidate-startdate1'
	, left(et1.EndDate,10) as 'candidate-enddate1'
	
	, replace(ca.notes,'>','') as 'candidate-note'
	, ca.openstat as 'candidate-comments'
#select count(*) #374
from copperq.candidate ca
left join copperq.tmp_document tr ON ca.CanID = tr.canID
left join copperq.users u ON ca.owner = u.userID
left join copperq.cantempperm ctp ON ca.tempperm = ctp.ID
left join copperq.photoblob pb ON ca.canid = pb.canID
left join (select canid,Employer,JobTitle,StartDate,EndDate,Note from copperq.tmp where num = 1) et1 ON ca.canid = et1.canid

#left join (select canid,Employer from copperq.tmp where num = 1) et1 ON ca.canid = et1.canid
#left join (select canid,JobTitle from copperq.tmp where num = 1) jt1 ON ca.canid = jt1.canid
#left join (select canid,Note from copperq.tmp where num = 1) nt1 ON ca.canid = nt1.canid
#left join (select canid,StartDate from copperq.tmp where num = 1) st1 ON ca.canid = st1.canid
#left join (select canid,EndDate from copperq.tmp where num = 1) et01 ON ca.canid = et01.canid


#left join copperq.canemployer ce ON ca.CanID = ce.canID
#left join copperq.cv cv ON ca.CanID = cv.canID
#left join copperq.acv acv ON ca.CanID = acv.canID


#select ca.canID, ca.FirstName, ca.surName, FileName, 'resume' as document_type from copperq.candidate ca
#left join (SELECT canID, concat(id,'.',filename)as FileName FROM CVblob) cli on cli.canID = ca.CanID where ca.CanID = 81239317
#select * FROM CVblob