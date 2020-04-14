--DEFAULT CONTACT FOR JOBS
select distinct case when [企業 PANO ] not in (select [PANO ] from csv_recf) then 'CPY9999999'
			else [企業 PANO ] end as [contact-companyId]
		, [採用担当者ID] as con_ext_id
		, concat('DEF', [企業 PANO ]) as [contact-externalId]
		, 'Default contact' as [contact-lastName]
		from csv_job
		where concat('REC-',採用担当者ID) not in (select 採用担当者ID from csv_rec)