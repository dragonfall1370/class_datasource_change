
# NOTE:
# SET group_concat_max_len = 2147483647;
# select distinct postype from api_jobs a
# select sno, type, name from manage where type = 'jotype' 
#select sno, type, name from manage 
# select * from TRUONG_companyowneremail

select
        a.posid as 'position-externalId'
       ,a.company as 'company External ID' ,com.cname as 'company name'
       ,a.contact as 'position-contactId' , con.fname, con.mname, con.lname
       ,o.email as 'position-owners' #,a.owner
       ,ind.name as 'INDUSTRY' #,a.industryid --<<
       ,case when jobtitle.postitle_rank = 1 then jobtitle.postitle else concat(jobtitle.postitle,' ',jobtitle.postitle_rank) end as 'position-title' #,a.postitle
       ,case a.posworkhr
              when 'fulltime' then 'FULL_TIME'
              when 'parttime' then 'PART_TIME'
        end as 'position-employmenttype'
       ,case type.name #a.postype
              when 'Temp/Contract' then 'CONTRACT'
              when 'Direct' then 'PERMANENT'
              when 'Temp/Contract to Direct' then 'TEMPORARY_TO_PERMANENT'
              when 'Internal Temp/Contract' then 'CONTRACT'
              when 'Internal Direct' then 'PERMANENT'
        end as 'position-type'
       ,a.no_of_pos 'position-headcount'
       ,date(a.posted_date) as 'position-startDate' #,a.posted_date
       ,date(a.remove_date) as 'position-endDate' #,a.remove_date
       ,id.note as 'position-internalDescription'
       ,pd.note as 'position-publicDescription'
       ,note.note as 'position-note'
       #as 'position-document'
       #notes as 'ACTIVITIES COMMENTS'
       #notes, tasklist as 'ACTIVITIES COMMENTS'
	#as 'DOCUMENTS'
# select postitle # select count(*)
from posdesc a
left join staffoppr_cinfo com on com.sno = a.company
left join staffoppr_contact con on con.sno = a.contact
left join TRUONG_companyowneremail o on o.owner = a.owner
left join truong_jobdocument d on d.sno = a.posid
left join (select sno, type, name from manage where type = 'jotype') type on type.sno = a.postype
left join (select sno, type, name from manage where type = 'joindustry') ind on ind.sno = a.industryid
left join (
              SELECT
                                @postitle_rank := IF(@postitle = case postitle when '' then 'No JobTitle' else postitle end and @company = company, @postitle_rank + 1, 1) AS postitle_rank,
                                @company := company as company,
                                @postitle := case postitle when '' then 'No JobTitle' else postitle end as postitle,
                                posted_date,
                                posid
              FROM posdesc
              ORDER BY postitle, company DESC
       ) jobtitle on jobtitle.posid = a.posid
left join (
              -- JOB INTERNAL DESCRIPTION
              select a.posid,
                                  group_concat(
                                           case when a.pamount is NULL THEN '' ELSE concat('Rates: Rate: ',a.pamount,' / ',a.pperiod,' / ',a.pcurrency,' / ',case a.job_loc_tax when 'N' then ' Billable' else 'Non-Billable' end,char(13)) END
                                          
                                          ,case when (a.prateopen is NULL) THEN '' ELSE concat('Regular Pay Rate: Open: ',case a.prateopen when 'N' then '(No)' when 'Y' then '(Yes)' end,' ',a.prateopen_amt,char(13)) END
                                          #,case when (a.prateopen_amt is NULL) THEN '' ELSE concat('Regular Pay Rate > Open: ',a.prateopen_amt,char(13)) END
                                          
                                          ,case when (a.burden is NULL) THEN '' ELSE concat('Pay Burden: Zero Pay Burden ',a.burden,' %',char(13)) END
                                          
                                          ,case when (a.otrate is NULL) THEN '' ELSE concat('Regular Bill Rate: Rate ',a.bamount,' / ',a.bcurrency,' / ',a.bperiod,' / ',case a.brateopen when 'N' then 'Taxable' else 'Non-Taxable' end,char(13)) END
                                          
                                          ,case when (a.bill_burden is NULL) THEN '' ELSE concat('Bill Burden: Zero Bill Burden ',a.bill_burden,' %',char(13)) END
                                          ,case when (a.margin is NULL) THEN '' ELSE concat('Margin: ',a.margin,' %',char(13)) END
                                          ,case when (a.markup is NULL) THEN '' ELSE concat('Markup: ',a.markup,' %',char(13)) END
                                          
                                          ,case when (a.Salary is NULL) THEN '' ELSE concat('Salary: Amount ',a.Salary,' / ',a.salary_currency,' / ',a.salary_period,char(13)) END
                                          ,case when (a.otrate is NULL) THEN '' ELSE concat('Overtime Pay Rate: ',a.otrate,' / ',a.ot_period,' / ',a.ot_currency,' / ',case a.diem_billable when 'N' then ' Billable' else 'Non-Billable' end,char(13)) END
                                          #,case when (a.ot_period = '' OR a.ot_period = '0' OR a.ot_period is NULL) THEN '' ELSE concat('Overtime Pay Rate > Per Hour: ',a.ot_period,char(13)) END
                                          #,case when (a.ot_currency = '' OR a.ot_currency = '0' OR a.ot_currency is NULL) THEN '' ELSE concat('Overtime Pay Rate > Currency : ',a.ot_currency,char(13)) END
                                          #,case when (a.diem_billable  = '' OR a.diem_billable = '0' OR a.diem_billable is NULL) THEN '' ELSE concat('Overtime Pay Rate > Billable / Non Billable: ',a.diem_billable ,char(13)) END
                                          
                                          ,case when (a.otbrate_amt is NULL) THEN '' ELSE concat('Overtime Bill Rate: ',a.otbrate_amt,' / ',a.otbrate_period,' / ',a.otbrate_curr,' / ',case a.diem_taxable when 'N' then ' Taxable' else 'Non-Taxable' end,char(13)) END
                                          #,case when (a.otbrate_period = '' OR a.otbrate_period = '0' OR a.otbrate_period is NULL) THEN '' ELSE concat('Overtime Bill Rate > Per Hour: ',a.otbrate_period,char(13)) END
                                          #,case when (a.otbrate_curr = '' OR a.otbrate_curr = '0' OR a.otbrate_curr is NULL) THEN '' ELSE concat('Overtime Bill Rate > Currency: ',a.otbrate_curr,char(13)) END
                                          #,case when (a.diem_taxable = '' OR a.diem_taxable  = '0' OR a.diem_taxable  is NULL) THEN '' ELSE concat('Overtime Bill Rate > Taxable / Non Taxable: ',a.diem_taxable ,char(13)) END
                                          
                                          ,case when (a.double_prate_amt is NULL) THEN '' ELSE concat('Double Time Pay Rate: ',a.double_prate_amt,' / ',a.double_prate_period,' / ',a.double_prate_curr,' / ',case a.diem_billable when 'N' then ' Billable' else 'Non-Billable' end,char(13)) END
                                          #,case when (a.double_prate_period = '' OR a.double_prate_period = '0' OR a.double_prate_period is NULL) THEN '' ELSE concat('Double Time Pay Rate > Per Hour: ',a.double_prate_period,char(13)) END
                                          #,case when (a.double_prate_curr = '' OR a.double_prate_curr = '0' OR a.double_prate_curr is NULL) THEN '' ELSE concat('Double Time Pay Rate > Currency: ',a.double_prate_curr,char(13)) END
                                          #,case when (a.diem_billable = '' OR a.diem_billable  = '0' OR a.diem_billable  is NULL) THEN '' ELSE concat('Double Time Pay Rate > Billable / Non Billable: ',a.diem_billable ,char(13)) END
                                          
                                          ,case when (a.double_brate_amt is NULL) THEN '' ELSE concat('Double Time Bill Rate: ',a.double_brate_amt,' / ',a.double_brate_period,' / ',a.double_brate_curr,' / ',case a.diem_taxable when 'N' then ' Taxable' else 'Non-Taxable' end,char(13)) END
                                          #,case when (a.double_brate_period = '' OR a.double_brate_period = '0' OR a.double_brate_period is NULL) THEN '' ELSE concat('Double Time Bill Rate > Per Hour: ',a.double_brate_period,char(13)) END
                                          #,case when (a.double_brate_curr = '' OR a.double_brate_curr = '0' OR a.double_brate_curr is NULL) THEN '' ELSE concat('Double Time Bill Rate > Currency: ',a.double_brate_curr,char(13)) END
                                          #,case when (a.diem_taxable  = '' OR a.diem_taxable  = '0' OR a.diem_taxable  is NULL) THEN '' ELSE concat('Double Time Bill Rate > Taxable / Non Taxable: ',a.diem_taxable ,char(13)) END
                                          
                                          ,case when (a.placement_fee is NULL) THEN '' ELSE concat('Placement Fee: ',a.placement_fee,' / ',a.placement_curr,char(13)) END
                                          #,case when (a.placement_curr = '' OR a.placement_curr = '0' OR a.placement_curr is NULL) THEN '' ELSE concat('Placement Fee > Currency: ',a.placement_curr,char(13)) END
                                          
                                          ,case when a.distributename in ('',NULL) THEN '' ELSE concat('Commission / Splits : ',a.distributename,char(13)) END
                                          ,case when (a.payrollpid = '' OR a.payrollpid = '0' OR a.payrollpid is NULL) THEN '' ELSE concat('Payroll Provider ID: ',a.payrollpid,char(13)) END
                                          ,case when (a1.wcomp_code = '' OR a1.wcomp_code = '0' OR a1.wcomp_code is NULL) THEN '' ELSE concat('Workers Compensation Code: ',a1.wcomp_code,char(13)) END #--EMPTY(posdesc)
                                          ,case when a.pterms in ('',NULL) THEN '' ELSE concat('Payment Terms: ',a.pterms,char(13)) END
                                          
                                          ,case when (a.tsapp is NULL) THEN '' ELSE concat('Timesheet Approval: ',a.tsapp,char(13)) END
                                          ,case when (a1.meta_keywords is NULL) THEN '' ELSE concat('Meta Keywords (for SEO): ',a1.meta_keywords,char(13)) END #--(posdesc)
                                          ,case when (a1.meta_desc is NULL) THEN '' ELSE concat('Meta Description (for SEO): ',a1.meta_desc,char(13)) END #--(posdesc)
                                          #,case when (a.skill_name = '' OR a.skill_name = '0' OR a.skill_name is NULL) THEN '' ELSE concat('Skill Name: ',a.skill_name,char(13)) END
                                          #,case when (a.last_used = '' OR a.last_used = '0' OR a.last_used is NULL) THEN '' ELSE concat('Last Used: ',a.last_used,char(13)) END
                                          #,case when (a.skill_level = '' OR a.skill_level = '0' OR a.skill_level is NULL) THEN '' ELSE concat('Skill Level: ',a.skill_level,char(13)) END
                                          #,case when (a.expe = '' OR a.expe = '0' OR a.expe is NULL) THEN '' ELSE concat('Years of Experience: ',a.expe,char(13)) END  --(posdesc)
                                          ,case when (a0.note is NULL) THEN '' ELSE concat(a0.note,char(13)) END
                                          ,case when a.city in ('',NULL) THEN '' ELSE concat('Relocation City: ',a.city,char(13)) END
                                          ,case when a.state in ('',NULL) THEN '' ELSE concat('Relocation State ',a.state,char(13)) END
                                          ,case when a.country in ('',NULL) THEN '' ELSE concat('Relocation Country ',countries.country,char(13)) END #a.country
                            SEPARATOR '\n\n') as 'note'
              # select *
              from req_pref a
              left join posdesc a1 on a1.posid = a.posid
              left join countries on countries.sno = a.country
              left join (select a2.jonumber
                            , group_concat(
                                           case when a3.skill_name in ('', NULL) THEN '' ELSE concat('Skill Name: ',a3.skill_name,char(13)) END
                                          ,case when a3.last_used in ('', NULL) THEN '' ELSE concat('Last Used: ',a3.last_used,char(13)) END
                                          ,case when a3.skill_level in ('', NULL) THEN '' ELSE concat('Skill Level: ',a3.skill_level,char(13)) END
                                          ,case when a3.expe in ('', NULL) THEN '' ELSE concat('Years of Experience: ',a3.expe,char(13)) END
                              SEPARATOR '\n\n') as 'note'                            
                              from api_jobs_skills a3
                              left join (select jonumber, sno from api_jobs) a2 on a2.sno = a3.rid
                              group by a3.rid 
                              ) a0 on a0.jonumber = a.posid
              #where a.posid = 831
              group by a.posid
       ) id on id.posid = a.posid
left join (
              -- JOB PUBLIC DESCRIPTION
              select posid
                     ,group_concat(
                             case when (a.posdesc = '' or a.posdesc is NULL) THEN '' ELSE concat('Description: ',a.posdesc,char(13),char(13)) END
                            ,case when (a.requirements = '' or a.requirements is NULL) THEN '' ELSE concat('Requirements: ',a.requirements,char(13),char(13)) END
                            ,case when (a.education = '' or a.education is NULL) THEN '' ELSE concat('Education: ',a.education,char(13),char(13)) END
                            ,case when (a.experience = '' or a.experience is NULL) THEN '' ELSE concat('Years of Experience: ',a.experience,char(13),char(13)) END
                     SEPARATOR '\n\n') as 'note'
              from posdesc a
              #where a.posid = 2553
              group by posid
       ) pd on pd.posid = a.posid
left join (
              -- NOTE
              select a.posid,
                     group_concat(
                             case when (status.name = '' OR status.name = '0' OR status.name is NULL) THEN '' ELSE concat('Status: ',status.name,char(13)) END #, a.posstatus
                            ,case when (jostage.name is null) THEN '' ELSE concat('Stage: ',jostage.name,char(13)) END #, a.jostage
                            ,case when (o.name is NULL) THEN '' when a.accessto = 'all' then concat('Share: all',char(13)) ELSE concat('Share: ',o.name,char(13)) END  #a.accessto
                            ,case when (source.name is NULL) THEN '' ELSE concat('Source Type: ',source.name,char(13)) END #,a.sourcetype
                            #Job Note > Reason
                            #Job Note > Punch Method
                            #Supervisor Email
                            ,case when (a.con_det = '' OR a.con_det = '0' OR a.con_det is NULL) THEN '' ELSE concat('Numero Contacto Empleado: ',a.con_det,'\n') END
                            ,case when (dept.deptname is NULL) THEN '' ELSE concat('HRM Department: ',dept.deptname,char(13)) END #a.con_det
                            ,case when (cat.name is NULL) THEN '' ELSE concat('Category: ',cat.name,char(13)) END #,a.catid
                            ,case when (a.refcode = '' OR a.refcode = '0' OR a.refcode is NULL) THEN '' ELSE concat('Ref. Code: ',a.refcode,char(13)) END
                            ,case when (a.closepos = '' OR a.closepos is NULL) THEN '' ELSE concat('Filled: ',a.closepos,'\n') END
                            ,case when (a.joblocation = '' OR a.joblocation = '0' OR a.joblocation is NULL) THEN '' ELSE concat('Job Location: ',a.joblocation,char(13)) END #staffoppr_location table
                            ,case when report.fullname is NULL THEN '' ELSE concat('Reports To: ',report.fullname,'\n') END #a.posreportto
                            #,case when (a.po_num = '' OR a.po_num = '0' OR a.po_num is NULL) THEN '' ELSE concat('PO Number: ',a.po_num,char(13)) END
                            ,case when (dept.deptname is NULL) THEN '' ELSE concat('Department: ',dept.deptname,char(13)) END #,a.department
                            ,case when (aj.joblocation is NULL) THEN '' ELSE concat('Billing Address: ',aj.joblocation,'\n') END #a.bill_address
                            ,case when bt.fullname is NULL THEN '' ELSE concat('Billing Contact: ',bt.fullname,'\n') END #a.billingto
                            ,case when aj.billpay_code is null THEN '' ELSE concat('Billing Terms: ',aj.billpay_code,'\n') END #a.billingto
                            ,case when (a.service_terms = '' OR a.service_terms = '0' OR a.service_terms is NULL) THEN '' ELSE concat('Service Terms: ',a.service_terms,char(13)) END
                                   #,case when (a.starthour = '' OR a.starthour = '0' OR a.starthour is NULL) THEN '' ELSE concat('Start Hours: ',a.starthour,char(13)) END
                                   #,case when (a.endhour = '' OR a.endhour = '0' OR a.endhour is NULL) THEN '' ELSE concat('End Hours: ',a.endhour,char(13)) END
                                   #Job Note > Shift(s) / Scheduling
                            ,case when (a.conmethod = '' OR a.conmethod = '0' OR a.conmethod is NULL or a.conmethod = '---') THEN '' ELSE concat('Contact Method: ',a.conmethod,char(13)) END
                            ,case when (a.posjo = '' OR a.posjo = '0' OR a.posjo is NULL or a.posjo = '-------') THEN '' ELSE concat('Requirements: ',a.posjo,char(13)) END
                    SEPARATOR '\n\n') as 'note'
              # select * # select count(*) # select conmethod, bill_req
              from posdesc a
              left join (select sno, type, name from manage where type = 'jostage' ) jostage on jostage.sno = a.jostage
              left join (select sno, type, name from manage where type = 'jostatus' ) status on status.sno = a.posstatus
              left join (select sno, type, name from manage where type = 'josourcetype' ) source on source.sno = a.sourcetype
              left join (select sno, type, name from manage where type = 'jocategory' ) cat on cat.sno = a.catid
              left join (select sno, deptname from department ) dept on dept.sno = a.deptid
              left join TRUONG_companyowneremail o on o.owner = a.accessto
              left join (select sno,group_concat(fname,mname,lname SEPARATOR ' ') as fullname from staffoppr_contact group by sno) report on report.sno = a.posreportto
              left join (select sno,group_concat(fname,mname,lname SEPARATOR ' ') as fullname from staffoppr_contact where fname <> '' group by sno) bt on bt.sno = a.billingto
              left join (select ajs.jonumber, ajs.joblocation, t.billpay_code from api_jobs ajs left join bill_pay_terms t on t.billpay_termsid = ajs.bill_req where t.billpay_status = 'active' ) aj on aj.jonumber = a.posid
              #where a.posid in (1633,1638,1639,1640,1642,1644,1645,1651,1652,1653,1654,1659,1661,1662,1663,1665,1666,1667,1672)
              group by a.posid
       ) note on note.posid = a.posid
#where a.posid in (5457) #or a.postitle like '%Finance Manager%'
#where a.company = 774