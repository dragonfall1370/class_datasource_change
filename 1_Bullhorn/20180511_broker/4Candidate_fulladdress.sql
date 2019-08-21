
-- INSERT TO VINCERE common_location table

	select --top 200
		  concat('candidate',convert(varchar(max),C.candidateID)) as 'externalId'
		, Coalesce(NULLIF(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
              , Coalesce(NULLIF(replace(C.LastName,'?',''), ''), concat('Lastname-',C.candidateID)) as 'contact-lastName'

		, C.address1 as 'address'
		, C.city as 'city'
		, C.state as 'state'
		, C.zip as 'post_code'
                ,c.countryID, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation in ('NULL','ZR') ) THEN null ELSE tc.abbreviation END as 'Country'
                , Stuff( Coalesce(' ' + NULLIF(C.city, ''), '') 
                        + Coalesce(', ' + NULLIF(C.state, ''), '') 
                        + Coalesce(', ' + NULLIF(C.zip, ''), '')
                        + Coalesce(', ' + NULLIF(tc.abbreviation, ''), '')
                , 1, 1, '') as 'location_name' 
	from bullhorn1.Candidate C 
	left join tmp_country tc ON c.countryID = tc.code 
	where C.isPrimaryOwner = 1 --8545
	and c.countryID = 2245
	
	