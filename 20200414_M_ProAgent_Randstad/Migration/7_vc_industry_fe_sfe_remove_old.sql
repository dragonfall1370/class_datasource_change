---BACKUP ALL INDUSTRY / FE / SFE AND DETAIL LANGUAGE
select *
into mike_bkup_vertical_20200717
from vertical


select *
into mike_bkup_vertical_detail_language_20200717
from vertical_detail_language


select *
into mike_bkup_functional_expertise_20200717
from functional_expertise


select *
into mike_bkup_functional_expertise_detail_language_20200717
from functional_expertise_detail_language


select *
into mike_bkup_sub_functional_expertise_20200717
from sub_functional_expertise


select *
into mike_bkup_sub_functional_expertise_detail_language_20200717
from sub_functional_expertise_detail_language


select *
into mike_bkup_user_account_vertical_20200717
from user_account_vertical


select *
into mike_bkup_user_account_functional_expertise_20200717
from user_account_functional_expertise


select *
into mike_bkup_position_description_vertical_29018_20200717
from position_description
where vertical_id < 29018

--> INDUSTRY <--
select *
from vertical
where id < 29018 --neutral industry ID
order by id --176 old industries

select *
from vertical_detail_language
where vertical_id < 29018 --174 rows

select *
from user_account_vertical
where vertical_id < 29018 --25701 rows

select *
from position_description
where vertical_id < 29018 --172

--MAIN SCRIPT
delete
from user_account_vertical
where vertical_id < 29018


update position_description
set vertical_id = NULL
where vertical_id < 29018 --Industry=Please select


delete from vertical_detail_language
where vertical_id < 29018 --delete following industries

delete from vertical
where id < 29018


--> SFE <--
	select *
	from sub_functional_expertise
	where functional_expertise_id <= 3043
	order by id --347 rows
	
	
	select *
	from sub_functional_expertise_detail_language
	where sub_functional_expertise_id in (select id from sub_functional_expertise where functional_expertise_id <= 3043)
	order by sub_functional_expertise_id --277 rows
	
	
	--MAIN SCRIPT
	delete from sub_functional_expertise_detail_language
	where sub_functional_expertise_id in (select id from sub_functional_expertise where functional_expertise_id <= 3043)
	
	
	delete from sub_functional_expertise
	where functional_expertise_id <= 3043


--> FE <--
	select *
	from functional_expertise
	where id <= 3043
	order by id --14 rows
	
	
	select *
	from functional_expertise_detail_language
	where functional_expertise_id <= 3043
	order by id --13 rows
	
	select *
	from user_account_functional_expertise
	where functional_expertise_id <= 3043 --2125 rows
	
	
	--MAIN SCRIPT
	delete from user_account_functional_expertise
	where functional_expertise_id <= 3043
	
	
	delete from functional_expertise_detail_language
	where functional_expertise_id <= 3043
	
	delete from functional_expertise
	where id <= 3043
