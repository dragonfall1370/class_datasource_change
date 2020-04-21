-->> Campaign - Marketing Campaign Reference <<--
--Sample
select * from campaign where campaignno = '1004782'


select distinct c.campaigntitle
from campaignassociate ca
left join campaign c on ca.idcampaign = c.idcampaign
order by c.campaigntitle


--Campaign REFERENCES
select distinct idcampaign
from campaigncontact
where idcampaigncontactstatus is not NULL --2637

select distinct campaigntitle
from campaign --4915
where isdeleted = '0' --4427

select distinct idcampaignstatus
from campaign --4915
where isdeleted = '0' --4427

select *
from campaign --4915
where isdeleted = '0' --4427
and idcampaignstatus = '66c540a1-d0f0-49fe-9332-eb984773adaf' --182

select idcampaign, idperson
from campaigncontact
where idcampaign in (select idcampaign from campaign where isdeleted = '0') --1332843 rows

select --cc.idcampaign, cc.idperson, c.campaigntitle
distinct campaigntitle --3851 rows
from campaigncontact cc
join (select idcampaign, campaigntitle from campaign where isdeleted = '0') c on c.idcampaign = cc.idcampaign

--Within 5 years and counts
select --cc.idcampaign, cc.idperson, c.campaigntitle
distinct campaigntitle, count(*) --3851 rows
from campaigncontact cc
join (select idcampaign, campaigntitle from campaign where isdeleted = '0' and createdon::timestamp >= now() - interval '5 years') c on c.idcampaign = cc.idcampaign
where cc.createdon::timestamp >= now() - interval '5 years'
group by campaigntitle

--Within 5 years
select --cc.idcampaign, cc.idperson, c.campaigntitle
distinct campaigntitle --784 rows
from campaigncontact cc
join (select idcampaign, campaigntitle from campaign 
	  where isdeleted = '0' and createdon::timestamp >= now() - interval '5 years') c on c.idcampaign = cc.idcampaign

select count(*)
from campaigncontact
where createdon::timestamp >= now() - interval '5 years' --1454841 rows

select count(*)
from campaign
where createdon::timestamp >= now() - interval '5 years' --927 rows