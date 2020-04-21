with test as (select * from lookup where code_type = 123)

,test2 as (select b.description as codename from event a
left join test b on a.z_last_type = b.code)

select case when codename = 'Holiday'
when codename = 'Telephone Interview'
when codename = 'Int 1 with client'
when codename = 'Mailshot'
when codename = 'Client Matching Call'
when codename = 'Planned Call'
when codename = '***Referral Call***'
when codename = 'Shortlist presented'
when codename = 'Client Process Call'
when codename = 'Incoming Client Enquiry'
when codename = 'Online Enquiry'
when codename = 'Candidate Prep Call'
when codename = 'Applied for Role'
when codename = 'Meeting with candidate'
when codename = 'Submitted to KAM'
when codename = 'Held Call'
when codename = 'Int 3 with client'
when codename = 'Candidate Check In Call'
when codename = 'Letter sent'
when codename ='Meeting with client'
when codename ='Notes'
when codename ='Email CV sent'
when codename ='Flip Marketing Call'
when codename ='Reference request'
when codename ='Data Lead'
when codename ='Sales Call'
when codename ='Email sent'
when codename ='Permanent offer'
when codename ='Write-in'
when codename ='Business Intelligence'
when codename ='Email Marketing'
when codename ='Temp Finished'
when codename ='Temp Start'
when codename ='NULL'
when codename ='Candidate Lead'
when codename ='Assigned to Vacancy'
when codename ='Added from Search'
when codename ='Resourcer Search 2'
when codename ='Target'
when codename ='Telephone call'
when codename ='CV received'
when codename ='Offer Call'
when codename ='***Confirm Details Call***'
when codename ='Letter,Fax,Email'
when codename ='Canvass call'
when codename ='Process CV from email'
when codename ='Quick text'
when codename ='Social media capture'
when codename ='Marketing Reply'
when codename ='***Market Knowledge Call***'
when codename ='Client Check In Call'
when codename ='Sales lead'
when codename ='Process vacancy from email'
when codename ='Int other'
when codename ='Chase Lead Call'
when codename ='Added from Watchdog'
when codename ='Email shortlist'
when codename ='Source'
when codename ='Candidate Further Info Call'
when codename ='Int with consultant'
when codename ='Contact text sent'
when codename ='Activity'
when codename ='Candidate Reg Call'
when codename ='Placement'
when codename ='Resourcer Search 1'
when codename ='Resourcer Search 3'
when codename ='Candidate Offer Call'
when codename ='Invoice'
when codename ='Ad Chase'
when codename ='Not Held Call'
when codename ='Meeting'
when codename ='CV sent'
when codename ='Email Received'
when codename ='Client Lead'
when codename ='CV Formatting'
when codename ='Job advert response (BB)'
when codename ='***Candidate Marketing Call***'
when codename ='Candidate Feedback Call'
when codename ='Prep Call'
when codename ='Unsuccessful'
when codename ='CV update'
when codename ='Temp to Perm'
when codename ='Vac advert authorisation (BB)'
when codename ='Client Service Call'
when codename ='Candidate text sent'
when codename ='Cross Sell'
when codename ='Resourcer Search 4'
when codename ='Work history request to candidate'
when codename ='Int 2 with client'
when codename ='Client Referral'
when codename ='Email proposal'
when codename ='Contract offer'


from test2