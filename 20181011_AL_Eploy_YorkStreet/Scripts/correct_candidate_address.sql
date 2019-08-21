select * from Activity
where type =
--'company'
'candidate'

-- update Activity
-- set type = 'candidate'
-- where
-- 	user_account_id = -10
-- 	and type = 'company'

select * from candidate c
join candidate_extension ce on c.id = ce.candidate_id
limit 5

-- 30365
-- 30004
-- 30362
-- 29874
-- 30250


select * from common_location
where id in (
	select current_location_id from candidate c
	where c.external_id is not null
		and c.external_id <> '0'
)


select
address
, district
, city
, state
, post_code
, country
, trim(both ',' from CONCAT(
			trim(coalesce(address, ''))
			, case length(trim(coalesce(district, ''))) > 0 when true then ', ' || trim(coalesce(district, '')) else '' end
			, case length(trim(coalesce(city, ''))) > 0 when true then ', ' || trim(coalesce(city, '')) else '' end
			, case length(trim(coalesce(state, ''))) > 0 when true then ', ' || trim(coalesce(state, '')) else '' end
			, case length(trim(coalesce(post_code, ''))) > 0 when true then ', ' || trim(coalesce(post_code, '')) else '' end
			, case length(trim(coalesce(country, ''))) > 0 when true then ', ' || trim(coalesce(country, '')) else
				case length(trim(coalesce(country_code, ''))) > 0 when true then ', ' || trim(coalesce(country_code, '')) else '' end
			end
			)
		) as address_sum
from common_location
where id in (
30365
,30004
,30362
,29874
,30250
)

-- update common_location
-- set
-- 	address =
-- 		trim(both ',' from concat(
-- 			trim(coalesce(address, ''))
-- 			, case length(trim(coalesce(district, ''))) > 0 when true then ', ' || trim(coalesce(district, '')) else '' end
-- 			, case length(trim(coalesce(city, ''))) > 0 when true then ', ' || trim(coalesce(city, '')) else '' end
-- 			, case length(trim(coalesce(state, ''))) > 0 when true then ', ' || trim(coalesce(state, '')) else '' end
-- 			, case length(trim(coalesce(post_code, ''))) > 0 when true then ', ' || trim(coalesce(post_code, '')) else '' end
-- 			, case length(trim(coalesce(country, ''))) > 0 when true then ', ' || trim(coalesce(country, '')) else
-- 				case length(trim(coalesce(country_code, ''))) > 0 when true then ', ' || trim(coalesce(country_code, '')) else '' end
-- 			end
-- 			)
-- 		),
-- 	location_name =
-- 		trim(both ',' from concat(
-- 			''
-- 			, case length(trim(coalesce(state, ''))) > 0 when true then ', ' || trim(coalesce(state, '')) else '' end
-- 			, case length(trim(coalesce(country, ''))) > 0 when true then ', ' || trim(coalesce(country, '')) else '' end
-- 			)
-- 		)
-- where id in (
-- 	select current_location_id from candidate c
-- 	where c.external_id is not null
-- 		and c.external_id <> '0'
-- )

-- update common_location
-- set
-- 	address =
-- 		trim(both ',' from concat(
-- 			trim(coalesce(address, ''))
-- 			, case length(trim(coalesce(district, ''))) > 0 when true then ', ' || trim(coalesce(district, '')) else '' end
-- 			, case length(trim(coalesce(city, ''))) > 0 when true then ', ' || trim(coalesce(city, '')) else '' end
-- 			, case length(trim(coalesce(state, ''))) > 0 when true then ', ' || trim(coalesce(state, '')) else
-- 				case length(trim(coalesce(state_code, ''))) > 0 when true then ', ' || trim(coalesce(state_code, '')) else '' end
-- 			end
-- 			, case length(trim(coalesce(post_code, ''))) > 0 when true then ', ' || trim(coalesce(post_code, '')) else '' end
-- 			, case length(trim(coalesce(country, ''))) > 0 when true then ', ' || trim(coalesce(country, '')) else
-- 				case length(trim(coalesce(country_code, ''))) > 0 when true then ', ' || trim(coalesce(country_code, '')) else '' end
-- 			end
-- 			)
-- 		),
-- 	location_name =
-- 		trim(both ',' from concat(
-- 			''
-- 			, case length(trim(coalesce(state, ''))) > 0 when true then trim(coalesce(state, '')) else
-- 				case length(trim(coalesce(state_code, ''))) > 0 when true then trim(coalesce(state_code, '')) else '' end
-- 			end
-- 			, case length(trim(coalesce(country, ''))) > 0 when true then ' ' || trim(coalesce(country, '')) else
-- 				case length(trim(coalesce(country_code, ''))) > 0 when true then ' ' || trim(coalesce(country_code, '')) else '' end
-- 			end
-- 			)
-- 		)
-- where id in (
-- 	select current_location_id from candidate c
-- 	where c.external_id is not null
-- 		and c.external_id <> '0'
-- )


-- update common_location
-- set address =
-- left(address,
-- 	length(address)
-- 	- case length(trim(coalesce(country, ''))) > 0 when true then length(', ' || trim(coalesce(country, ''))) else 0 end
-- 	- case length(trim(coalesce(post_code, ''))) > 0 when true then length(', ' || trim(coalesce(post_code, ''))) else 0 end
-- 	- case length(trim(coalesce(state, ''))) > 0 when true then length(', ' || trim(coalesce(state, ''))) else 0 end
-- 	- case length(trim(coalesce(city, ''))) > 0 when true then length(', ' || trim(coalesce(city, ''))) else 0 end
-- 	- case length(trim(coalesce(district, ''))) > 0 when true then length(', ' || trim(coalesce(district, ''))) else 0 end
-- )
-- where id in (
-- 	select current_location_id from candidate c
-- 	where c.external_id is not null
-- 		and c.external_id <> '0'
-- )
 

select left(address,
	length(address)
	- case length(trim(coalesce(country, ''))) > 0 when true then length(', ' || trim(coalesce(country, ''))) else 0 end
	- case length(trim(coalesce(post_code, ''))) > 0 when true then length(', ' || trim(coalesce(post_code, ''))) else 0 end
	- case length(trim(coalesce(state, ''))) > 0 when true then length(', ' || trim(coalesce(state, ''))) else 0 end
	- case length(trim(coalesce(city, ''))) > 0 when true then length(', ' || trim(coalesce(city, ''))) else 0 end
	- case length(trim(coalesce(district, ''))) > 0 when true then length(', ' || trim(coalesce(district, ''))) else 0 end
) as address2
from common_location
where id in (
30365
,30004
,30362
,29874
,30250
,30362
)

--30362;"London";"";"Wallington";"";"sm68hf";"";"45 Rookwood Avenue";;;"2018-08-17 11:52:39.503443";"";"";"";"";;;;64686;"2018-08-17 11:52:39.503443";;

select * from local_currency_mapping 

select * from local_currency_mapping_id_seq 

select * from location_currency 

select * from location_currency_id_seq 

select * from location