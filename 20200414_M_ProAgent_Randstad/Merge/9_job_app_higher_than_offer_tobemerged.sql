--BACKUP OFFER / OFFER_PERSONAL_INFO / INVOICE
select *
into mike_offer_bkup_20200609
from offer

select *
into mike_offer_personal_info_bkup_20200609
from offer_personal_info

select *
into mike_invoice_bkup_20200609
from invoice

select *
from mike_invoice_bkup_20200609

select *
from offer_revenue_split

--REFERENCE TMP TABLE
select *
from mike_jobapp_tobemerged
where status = 200 --Offer: 118

select *
from mike_jobapp_tobemerged
where status >= 300 --Placed: 401


--->> RUNNING SCRIPT <<---
with appfilter as (select pc.id, pc.candidate_id, m.vc_candidate_id, pc.position_description_id, pc.status
	from position_candidate pc
	join mike_tmp_candidate_dup_check m on m.vc_pa_candidate_id = pc.candidate_id --job app links with dup candidate from PA
	--where exists (select 1 from mike_tmp_candidate_dup_check where m.vc_pa_candidate_id = pc.candidate_id)
	and pc.status => 200 --offered stage
	)

--MAIN SCRIPT TO UPDATE
update position_candidate pc
set candidate_id = af.vc_candidate_id
from appfilter af
where pc.id = af.id