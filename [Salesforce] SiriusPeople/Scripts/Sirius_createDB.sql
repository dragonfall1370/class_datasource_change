CREATE TABLE [dbo].[Candidate] (
	ID varchar(100),
	ACCOUNTID varchar(100),
	LASTNAME nvarchar(max),
	FIRSTNAME nvarchar(max),
	SALUTATION nvarchar(max),
	RECORDTYPEID varchar(100),
	MAILINGSTREET nvarchar(max),
	MAILINGCITY nvarchar(max),
	MAILINGSTATE nvarchar(max),
	MAILINGPOSTALCODE nvarchar(max),
	MAILINGCOUNTRY nvarchar(max),
	PHONE nvarchar(max),
	MOBILEPHONE nvarchar(max),
	HOMEPHONE nvarchar(max),
	EMAIL nvarchar(max),
	TITLE nvarchar(max),
	BIRTHDATE varchar(max),
	OWNERID varchar(100),
	DONOTCALL nvarchar(max),
	CREATEDDATE nvarchar(max),
	PEOPLECLOUD1__GENDER__C nvarchar(max),
	PEOPLECLOUD1__STATUS__C nvarchar(max),
	PEOPLECLOUD1__HOME_EMAIL__C nvarchar(max),
	PEOPLECLOUD1__WORK_EMAIL__C nvarchar(max),
	CURRENT_EMPLOYER__C nvarchar(max),
	IDEAL_POSITION__C nvarchar(max),
	CURRENT_DAILY_RATE__C nvarchar(max),
	EMERGENCY_CONTACT_NAME__C nvarchar(max),
	VISA_TYPE_DEL__C nvarchar(max),
	WHS_COMPLETION_DATE__C nvarchar(max),
	PREFERRED_NAME__C nvarchar(max),
	LINKEDIN_PROFILE__C nvarchar(max),
	CANDIDATE_DESCRIPTION__C nvarchar(max),
	DIVISION__C nvarchar(max),
	EMERGENCY_CONTACT_PHONE__C nvarchar(max),
	EMERGENCY_CONTACT_RELATIONSHIP__C nvarchar(max),
	WHS_MODULES_COMPLETED__C nvarchar(max),
	WORKPRO_CIN_DEL__C nvarchar(max),
	WORKPRO_PIN_DEL__C nvarchar(max),
	NOTICE_PERIOD__C nvarchar(max),
	DATE_REGISTERED__C nvarchar(max),
	PREFERRED_EMPLOYMENT_TYPE__C nvarchar(max),
	PREFERRED_SHIFT__C nvarchar(max),
	CURRENT_EMPLOYMENT_TYPE__C nvarchar(max),
	DO_YOU_HAVE_A_VALID_DRIVERS_LICENSE__C nvarchar(max),
	DO_YOU_HAVE_A_VALID_WHITECARD__C nvarchar(max),
	ANY_DISABILITIES__C nvarchar(max),
	DO_YOU_HAVE_A_CAR__C nvarchar(max),
	CANDIDATE_RATING_CONSULTANT__C nvarchar(max),
	HOTLIST__C nvarchar(max),
	ANY_MEDICATIONS__C nvarchar(max),
	CRIMINAL_RECORD__C nvarchar(max),
	AUTHORISATION_TO_CONTACT_YOUR_REFERENCES__C nvarchar(max),
	VISA_NOTES__C nvarchar(max),
	VISA_EXPIRY_DEL__C nvarchar(max),
	DO_NOT_CONTACT_REASON__C nvarchar(max),
	REGISTERED_BY_PICKLIST__C nvarchar(max),
	WHS_EXPIRY_DATE__C nvarchar(max),
	DESK__C nvarchar(max),
	DO_YOU_HAVE_SAFETY_BOOTS__C nvarchar(max),
	TYPING__C nvarchar(max),
	NUMERIC__C nvarchar(max),
	CURRENT_ANNUAL_SALARY__C nvarchar(max),
	CURRENT_HOURLY_SALARY__C nvarchar(max),
	DESIRED_ANNUAL_SALARY__C nvarchar(max),
	DESIRED_DAILY_SALARY__C nvarchar(max),
	DESIRED_HOURLY_SALARY__C nvarchar(max),
	IF_YES_TO_THE_ABOVE_PLEASE_EXPLAIN__C nvarchar(max),
	TYPING_ACCURACY__C nvarchar(max),
	NUMERIC_ACCURACY__C nvarchar(max),
	AVAILABILITY_MIRROR__C nvarchar(max),
	CURRENT_CONTRACT_END_DATE__C nvarchar(max),
	CURRENT_TEAM_SIZE__C nvarchar(max),
	FORKLIFT_LICENSE__C nvarchar(max)
)

-----------------
-----------------

CREATE TABLE [dbo].[Contact] (
	ID varchar(100),
	ACCOUNTID varchar(100),
	LASTNAME nvarchar(max),
	FIRSTNAME nvarchar(max),
	SALUTATION nvarchar(max),
	MAILINGSTREET nvarchar(max),
	MAILINGCITY nvarchar(max),
	MAILINGSTATE nvarchar(max),
	MAILINGPOSTALCODE nvarchar(max),
	MAILINGCOUNTRY nvarchar(max),
	MAILINGADDRESS nvarchar(max),
	PHONE nvarchar(max),
	MOBILEPHONE nvarchar(max),
	REPORTSTOID nvarchar(max),
	EMAIL nvarchar(max),
	TITLE nvarchar(max),
	OWNERID nvarchar(max),
	DONOTCALL nvarchar(max),
	PEOPLECLOUD1__HOME_EMAIL__C nvarchar(max),
	PEOPLECLOUD1__WORK_EMAIL__C nvarchar(max),
	CONTACT_STATUS__C nvarchar(max),
	LINKEDIN_PROFILE__C nvarchar(max),
	NO_PERM_STAFF_IN_TEAM__C nvarchar(max),
	NO_CONTRACTORS_IN_TEAM__C nvarchar(max),
	INDUSTRY_SECTORS__C nvarchar(max),
	DIVISION__C nvarchar(max),
	NO_TEMPS_IN_TEAM__C nvarchar(max),
	OWNER__C nvarchar(max),
	DO_NOT_CONTACT_REASON__C nvarchar(max),
	DESK__C nvarchar(max),
	CONTACT_OWNER_SBS__C nvarchar(max),
	CONTACT_OWNERSSM__C nvarchar(max),
	CONTACT_OWNER_STSSTM__C nvarchar(max),
	CONTACT_OWNER_IND__C nvarchar(max),
	CONTACT_OWNER_SAAF__C nvarchar(max),
	ACCOUNTING_FINANCE__C nvarchar(max),
	ACCOUNT_NAME__C nvarchar(max),
	HOW_MANY_PEOPLE_IN_YOUR_TEAM__C nvarchar(max),
	CURRENT_TEAM_SIZE__C nvarchar(max),
	DEVELOPMENT_QUALIFICATION__C nvarchar(max),
	INFRASTRUCTURE_QUALIFICATION__C nvarchar(max),
	BI_DATA_CRM_QUALIFICATION__C nvarchar(max),
	PROJECT_SERVICES_QUALIFICATION__C nvarchar(max),
	SUPPORT__C nvarchar(max),
	TECH_CONTACT_OWNER_CONTRACT__C nvarchar(max),
	TECH_CONTACT_OWNER_PERM__C nvarchar(max),
	INDUSTRIOUS__C nvarchar(max),
	SSM__C nvarchar(max),
	COMPANIES_PACKAGES__C nvarchar(max),
	DIGITAL_QUALIFICATION__C nvarchar(max)
)


-----------
-----------

CREATE TABLE [dbo].[Company] (
	ID varchar(100),
	NAME nvarchar(max),
	PARENTID varchar(100),
	BILLINGSTREET nvarchar(max),
	BILLINGCITY nvarchar(max),
	BILLINGSTATE nvarchar(max),
	BILLINGPOSTALCODE nvarchar(max),
	BILLINGCOUNTRY nvarchar(max),
	PHONE nvarchar(max),
	WEBSITE nvarchar(max),
	DESCRIPTION nvarchar(max),
	OWNERID nvarchar(max),
	LASTACTIVITYDATE date,
	ACCOUNT_MANAGER__C nvarchar(max),
	DIVISION__C nvarchar(max),
	ABN_ACN__C nvarchar(max),
	INDUSTRY_SECTORS__C nvarchar(max),
	MODERN_AWARD__C nvarchar(max),
	SPECIAL_TERMS_CONDITIONS__C nvarchar(max),
	WORKPLACE_OHS_ASSESSMENT_COMPLETED__C nvarchar(max),
	WORKPLACE_OHS_DATE_COMPLETED__C date,
	COMPANY_ID_HIDDEN__C nvarchar(max)
)

-----------
-----------

CREATE TABLE [dbo].[TermBusiness] (
	ID varchar(100),
	NAME nvarchar(max),
	CREATEDDATE nvarchar(max),
	CREATEDBYID varchar(100),
	CLIENT__C nvarchar(max)
)

-----------
-----------

CREATE TABLE [dbo].[CVFloated] (
	ID varchar(100),
	NAME nvarchar(max),
	CREATEDDATE date,
	CREATEDBYID varchar(100),
	CANDIDATE_FLOATED__C varchar(100),
	COMPANY__C varchar(100),
	CONTACT__C varchar(100)
)

-----------
-----------

CREATE TABLE [dbo].[PaymentHistory] (
	ID varchar(100),
	NAME nvarchar(max),
	CREATEDDATE varchar(100),
	CREATEDBYID varchar(100),
	ACCOUNT__C varchar(100),
	NOTES_PAYMENT_INFO__C nvarchar(max)
)

-----------
-----------
CREATE TABLE [dbo].[SMSHistory] (
	ID varchar(100),
	OWNERID varchar(100),
	NAME nvarchar(max),
	CREATEDDATE nvarchar(max),
	SMAGICINTERACT__CONTACT__C varchar(100),
	SMAGICINTERACT__NAME__C nvarchar(max),
	SMAGICINTERACT__SMSTEXT__C nvarchar(max)
)

-----------
-----------
CREATE TABLE [dbo].[Tasks] (
	ID varchar(100),
	WHOID varchar(100),
	WHATID varchar(100),
	SUBJECT nvarchar(max),
	STATUS nvarchar(max),
	OWNERID varchar(100),
	ACCOUNTID varchar(100),
	SHORT_DESCRIPTION__C nvarchar(max),
	COMPLETED_DATE__C varchar(max)
)

-----------
-----------
CREATE TABLE [dbo].[ResumeCompliance] (
	ID varchar(100),
	OWNERID varchar(100),
	NAME nvarchar(max),
	CREATEDDATE date,
	PEOPLECLOUD1__DOCUMENT_RELATED_TO__C varchar(100),
	PEOPLECLOUD1__LINK_TO_FILE_SEC__C nvarchar(max),
	PEOPLECLOUD1__DOCUMENT_TYPE__C nvarchar(max)
)

-----------
-----------
CREATE TABLE [dbo].[CandidateSkills] (
	ID varchar(100),
	NAME nvarchar(max),
	PEOPLECLOUD1__CANDIDATE__C varchar(100),
	PEOPLECLOUD1__SKILL__C nvarchar(max),
	SKILL_GROUP_NAME__C nvarchar(max),
	SKILL_NAME__C nvarchar(max)
)

-----------
-----------
CREATE TABLE [dbo].[Jobs] (
	ID varchar(100),
	OWNERID varchar(100),
	NAME nvarchar(max),
	CREATEDDATE nvarchar(max),
	PEOPLECLOUD1__BASE_SALARY__C nvarchar(max),
	PEOPLECLOUD1__COMPANY__C varchar(100),
	PEOPLECLOUD1__END_DATE__C nvarchar(max),
	PEOPLECLOUD1__PLACED_CANDIDATE__C varchar(100),
	PEOPLECLOUD1__CANDIDATE_CHARGE_RATE__C nvarchar(max),
	PEOPLECLOUD1__CLIENT_CHARGE_RATE__C nvarchar(max),
	PEOPLECLOUD1__FLAT_FEE__C nvarchar(max),
	PEOPLECLOUD1__SUPER__C nvarchar(max),
	PEOPLECLOUD1__TOTAL_PACKAGE__C nvarchar(max),
	DIVISION__C nvarchar(max),
	RESOURCER__C varchar(100),
	HOURS_OF_WORK__C nvarchar(max),
	DAYS__C nvarchar(max),
	NUMBER_OF_POSITIONS__C nvarchar(max),
	VACANCY_STATUS__C nvarchar(max),
	FEE_BASED_ON__C nvarchar(max),
	ESTIMATED_VACANCY_VALUE__C nvarchar(max),
	PRO_RATA_MONTHS__C nvarchar(max),
	ONCOST__C nvarchar(max),
	ONCOST_VALUE__C nvarchar(max),
	MARGIN__C nvarchar(max),
	MARGIN_PERCENTAGE__C nvarchar(max),
	DESK__C nvarchar(max),
	EXPECTED_CLOSE_DATE__C nvarchar(max),
	JOB_NUMBER__C nvarchar(max),
	JOB_TYPE__C nvarchar(max),
	RATE_TYPE__C nvarchar(max),
	HOURS_PER_DAY__C nvarchar(max),
	DAYS_PER_WEEK__C nvarchar(max),
	WEEKLY_MARGIN__C nvarchar(max),
	CLIENT_CONTACT__C varchar(100),
	ESTIMATED_CONTRACT_VALUE__C nvarchar(max),
	ESTIMATED_TEMP_VALUE__C nvarchar(max),
	JOB_PICKED_UP_PASSED__C nvarchar(max),
	TOTAL_PACKAGE_1__C nvarchar(max),
	CALCULATED_TOTAL_PACKAGE__C nvarchar(max),
	CLOSED_DATE__C nvarchar(max),
	CONSULTANT_FORECAST_PERCENTAGE__C nvarchar(max),
	FORECAST_VALUE_CONSULTANT__C nvarchar(max),
	REPLACEMENT_VACANCY__C nvarchar(max),
	PLACEMENT_BEING_REPLACED__C nvarchar(100),
	CONSULTANT__C varchar(100),
	RECORD_TYPE_NAME__C nvarchar(max),
	FORECAST_NOTES__C nvarchar(max),
	CLIENT_CONTACT_ID__C nvarchar(100)
)

-----------
-----------
CREATE TABLE [dbo].[Ads] (
	ID varchar(100),
	OWNERID varchar(100),
	NAME nvarchar(max),
	RECORDTYPEID varchar(100),
	CREATEDDATE date,
	PEOPLECLOUD1__JOB_CONTENT__C nvarchar(max),
	PEOPLECLOUD1__JOB_TITLE__C nvarchar(max),
	PEOPLECLOUD1__VACANCY__C varchar(100),
	CLIENT__C nvarchar(max),
	PEOPLE_CLOUD_REF_ID__C varchar(100)
)

-----------
-----------
CREATE TABLE [dbo].[CandidateManagement] (
	ID varchar(100),
	NAME nvarchar(max),
	RECORDTYPEID varchar(100),
	CREATEDDATE nvarchar(max),
	PEOPLECLOUD1__PLACEMENT__C varchar(100),
	PEOPLECLOUD1__CANDIDATE_SOURCE1__C nvarchar(max),
	PEOPLECLOUD1__CANDIDATE_STATUS__C nvarchar(max),
	PEOPLECLOUD1__CANDIDATE__C varchar(100),
	PEOPLECLOUD1__COMPANY__C nvarchar(max),
	PEOPLECLOUD1__START_DATE__C nvarchar(max),
	PEOPLECLOUD1__STATUS__C nvarchar(max),
	PEOPLECLOUD1__CANDIDATE_FEEDBACK_FIRST__C nvarchar(max),
	PEOPLECLOUD1__CANDIDATE_FEEDBACK_SECOND__C nvarchar(max),
	PEOPLECLOUD1__CLIENT_FEEDBACK_FIRST__C nvarchar(max),
	PEOPLECLOUD1__CLIENT_FEEDBACK_SECOND__C nvarchar(max),
	PEOPLECLOUD1__CONSULTANT__C nvarchar(max),
	PEOPLECLOUD1__CANDIDATE_FIRST_NAME__C nvarchar(max),
	PEOPLECLOUD1__DATE_AND_TIME_FIRST__C nvarchar(max),
	PEOPLECLOUD1__DATE_AND_TIME_SECOND__C nvarchar(max),
	PEOPLECLOUD1__END_DATE__C nvarchar(max),
	FEE_BASED_ON__C nvarchar(max),
	PRO_RATA_MONTHS__C nvarchar(max),
	BASE_SALARY__C nvarchar(max),
	SUPER__C nvarchar(max),
	FEE__C nvarchar(max),
	FLAT_FEE__C nvarchar(max),
	ACTUAL_PLACEMENT_VALUE__C nvarchar(max),
	CAND_RESUME_LINK__C nvarchar(max),
	PAY_RATE_CUSTOM__C nvarchar(max),
	CHARGE_RATE_CUSTOM__C nvarchar(max),
	ONCOST__C nvarchar(max),
	ONCOST_VALUE__C nvarchar(max),
	MARGIN_VALUE__C nvarchar(max),
	MARGIN__C nvarchar(max),
	HOURS__C nvarchar(max),
	CLIENT_CHARGE__C nvarchar(max),
	CANDIDATE_PAY__C nvarchar(max),
	HOURS_OF_WORK__C nvarchar(max),
	ASSIGNMENT_LOCATION__C nvarchar(max),
	GUARANTEE_PERIOD__C nvarchar(max),
	GUARANTEE_DATE__C nvarchar(max),
	PLACEMENT_TYPE__C nvarchar(max),
	PO_NUMBER__C nvarchar(max),
	INVOICE_NUMBER__C nvarchar(max),
	DUE_DATE__C nvarchar(max),
	PLACED_DATE__C nvarchar(max),
	RATE_TYPE__C nvarchar(max),
	JOB_NAME__C nvarchar(max),
	WEEKLY_MARGIN__C nvarchar(max),
	TIMESHEET_APPROVER__C nvarchar(max),
	WEEKLY_MARGIN_SIRIUS__C nvarchar(max),
	CLIENT_SIGNATORY__C nvarchar(max),
	TOTAL_PACKAGE_C__C nvarchar(max),
	DIVISION__C nvarchar(max),
	CREATED_DATE__C nvarchar(max),
	CLIENT_CONTACT__C nvarchar(max),
	INVOICE_MAILING_ADDRESS__C nvarchar(max),
	CANDIDATE_NAME__C nvarchar(max),
	REPORTS_TO__C nvarchar(max),
	TIMESHEET_APPROVER_SECONDARY__C nvarchar(max),
	INVOICE_RECIPIENT__C nvarchar(max),
	WEEKLY_MARGIN_IND_SBS__C nvarchar(max),
	CALCULATED_TOTAL_PACKAGE__C nvarchar(max),
	CANDIDATE_PAYMENT_TYPE1__C nvarchar(max),
	ABN_ACN_COMPANY_ENTERPRISE__C nvarchar(max),
	ADDRESS_COMPANY_ENTERPRISE__C nvarchar(max),
	COMPANY_ENTERPRISE_NAME__C nvarchar(max),
	VACANCY_STATUS__C nvarchar(max),
	PLACEMENT_EXTENSION__C nvarchar(max),
	PAYMENT_TERMS__C nvarchar(max),
	ABN_ACN__C nvarchar(max),
	CLIENT_SIGNATORY_EMAIL__C nvarchar(max),
	COMPANY_ADDRESS__C nvarchar(max),
	NOTICE_PERIOD__C nvarchar(max),
	RESOURCER__C nvarchar(max),
	INVOICE_RECIPIENT_EMAIL_ADDRESS__C nvarchar(max),
	STATUS_CANDIDATE_PROGRESS__C nvarchar(max),
	REPLACEMENT_VACANCY__C nvarchar(max),
	TOTAL_FEE__C nvarchar(max),
	LINK_TO_PREVIOUS_RELATED_PLACEMENT__C nvarchar(max),
	RECORD_TYPE_NAME__C nvarchar(max),
	JOB_NUMBER__C nvarchar(max),
	IS_PLACEMENT_CURRENT_OR_STARTING__C nvarchar(max),
	MONTHLY_DRIP_FEE__C nvarchar(max),
	SALES_VALUE__C nvarchar(max),
	CLIENT_CONTACT_ID__C nvarchar(max),
	INVOICE_PAID__C nvarchar(max)
)

--------------
--------------
CREATE TABLE [dbo].[S3File] (
	S3Filename nvarchar(max)
)

--------------
--------------
CREATE TABLE [dbo].[SiriusUsers] (
	ID varchar(100),
	USERNAME varchar(100),
	LASTNAME varchar(100),
	FIRSTNAME varchar(100),
	NAME varchar(100),
	DIVISION varchar(100),
	EMAIL varchar(100),
	ISACTIVE varchar(100)
)

----------------
----------------
CREATE TABLE [dbo].[Attachments] (
	ID varchar(100),
	PARENTID varchar(100),
	NAME varchar(max),
	CREATEDDATE varchar(100)
)

----------------
----------------
CREATE TABLE [dbo].[SiriusFE] (
	SeqNo int,
	SiriusFE varchar(max),
	SiriusSFE varchar(max),
	VCFE varchar(max),
	VCSFE varchar(max)
)
----------------
----------------
CREATE TABLE [dbo].[SiriusIndustry] (
	SeqNo int,
	VCIndustry varchar(max)
)
----------------
----------------
CREATE TABLE [dbo].[SiriusContactReg] (
	ID varchar(100),
	ACCOUNTID varchar(100),
	LASTNAME varchar(100),
	FIRSTNAME varchar(100),
	CREATEDDATE varchar(100)
)

----------------
----------------
CREATE TABLE [dbo].[SiriusPlacement] (
	ID varchar(100),
	NAME nvarchar(max),
	RECORDTYPEID varchar(100),
	CREATEDDATE nvarchar(max),
	PEOPLECLOUD1__PLACEMENT__C varchar(100),
	PEOPLECLOUD1__CANDIDATE_SOURCE1__C nvarchar(max),
	PEOPLECLOUD1__CANDIDATE_STATUS__C nvarchar(max),
	PEOPLECLOUD1__CANDIDATE__C varchar(100),
	PEOPLECLOUD1__COMPANY__C nvarchar(max),
	PEOPLECLOUD1__START_DATE__C nvarchar(max),
	PEOPLECLOUD1__STATUS__C nvarchar(max),
	PEOPLECLOUD1__CANDIDATE_FEEDBACK_FIRST__C nvarchar(max),
	PEOPLECLOUD1__CANDIDATE_FEEDBACK_SECOND__C nvarchar(max),
	PEOPLECLOUD1__CLIENT_FEEDBACK_FIRST__C nvarchar(max),
	PEOPLECLOUD1__CLIENT_FEEDBACK_SECOND__C nvarchar(max),
	PEOPLECLOUD1__CONSULTANT__C nvarchar(max),
	PEOPLECLOUD1__CANDIDATE_FIRST_NAME__C nvarchar(max),
	PEOPLECLOUD1__DATE_AND_TIME_FIRST__C nvarchar(max),
	PEOPLECLOUD1__DATE_AND_TIME_SECOND__C nvarchar(max),
	PEOPLECLOUD1__END_DATE__C nvarchar(max),
	FEE_BASED_ON__C nvarchar(max),
	PRO_RATA_MONTHS__C nvarchar(max),
	BASE_SALARY__C nvarchar(max),
	SUPER__C nvarchar(max),
	FEE__C nvarchar(max),
	FLAT_FEE__C nvarchar(max),
	ACTUAL_PLACEMENT_VALUE__C nvarchar(max),
	CAND_RESUME_LINK__C nvarchar(max),
	PAY_RATE_CUSTOM__C nvarchar(max),
	CHARGE_RATE_CUSTOM__C nvarchar(max),
	ONCOST__C nvarchar(max),
	ONCOST_VALUE__C nvarchar(max),
	MARGIN_VALUE__C nvarchar(max),
	MARGIN__C nvarchar(max),
	HOURS__C nvarchar(max),
	CLIENT_CHARGE__C nvarchar(max),
	CANDIDATE_PAY__C nvarchar(max),
	HOURS_OF_WORK__C nvarchar(max),
	ASSIGNMENT_LOCATION__C nvarchar(max),
	GUARANTEE_PERIOD__C nvarchar(max),
	GUARANTEE_DATE__C nvarchar(max),
	PLACEMENT_TYPE__C nvarchar(max),
	PO_NUMBER__C nvarchar(max),
	INVOICE_NUMBER__C nvarchar(max),
	DUE_DATE__C nvarchar(max),
	PLACED_DATE__C nvarchar(max),
	RATE_TYPE__C nvarchar(max),
	JOB_NAME__C nvarchar(max),
	WEEKLY_MARGIN__C nvarchar(max),
	TIMESHEET_APPROVER__C nvarchar(max),
	WEEKLY_MARGIN_SIRIUS__C nvarchar(max),
	CLIENT_SIGNATORY__C nvarchar(max),
	TOTAL_PACKAGE_C__C nvarchar(max),
	DIVISION__C nvarchar(max),
	CREATED_DATE__C nvarchar(max),
	CLIENT_CONTACT__C nvarchar(max),
	INVOICE_MAILING_ADDRESS__C nvarchar(max),
	CANDIDATE_NAME__C nvarchar(max),
	REPORTS_TO__C nvarchar(max),
	TIMESHEET_APPROVER_SECONDARY__C nvarchar(max),
	INVOICE_RECIPIENT__C nvarchar(max),
	WEEKLY_MARGIN_IND_SBS__C nvarchar(max),
	CALCULATED_TOTAL_PACKAGE__C nvarchar(max),
	CANDIDATE_PAYMENT_TYPE1__C nvarchar(max),
	ABN_ACN_COMPANY_ENTERPRISE__C nvarchar(max),
	ADDRESS_COMPANY_ENTERPRISE__C nvarchar(max),
	COMPANY_ENTERPRISE_NAME__C nvarchar(max),
	VACANCY_STATUS__C nvarchar(max),
	PLACEMENT_EXTENSION__C nvarchar(max),
	PAYMENT_TERMS__C nvarchar(max),
	ABN_ACN__C nvarchar(max),
	CLIENT_SIGNATORY_EMAIL__C nvarchar(max),
	COMPANY_ADDRESS__C nvarchar(max),
	NOTICE_PERIOD__C nvarchar(max),
	RESOURCER__C nvarchar(max),
	INVOICE_RECIPIENT_EMAIL_ADDRESS__C nvarchar(max),
	STATUS_CANDIDATE_PROGRESS__C nvarchar(max),
	REPLACEMENT_VACANCY__C nvarchar(max),
	TOTAL_FEE__C nvarchar(max),
	LINK_TO_PREVIOUS_RELATED_PLACEMENT__C nvarchar(max),
	RECORD_TYPE_NAME__C nvarchar(max),
	JOB_NUMBER__C nvarchar(max),
	IS_PLACEMENT_CURRENT_OR_STARTING__C nvarchar(max),
	MONTHLY_DRIP_FEE__C nvarchar(max),
	SALES_VALUE__C nvarchar(max),
	CLIENT_CONTACT_ID__C nvarchar(max),
	INVOICE_PAID__C nvarchar(max)
)

----------------
----------------
CREATE TABLE [dbo].[ContactFE] (
	ID varchar(100),
	ACCOUNTID varchar(100),
	LASTNAME nvarchar(max),
	FIRSTNAME nvarchar(max),
	RECORDTYPEID nvarchar(max),
	ACCOUNTING_FINANCE__C nvarchar(max),
	A_F_SUB_COMMUNITY__C nvarchar(max),
	DEVELOPMENT_QUALIFICATION__C nvarchar(max),
	DEVELOPMENT_SKILLSET__C nvarchar(max),
	INFRASTRUCTURE_QUALIFICATION__C nvarchar(max),
	INFRA_SKILLSET__C nvarchar(max),
	BI_DATA_CRM_QUALIFICATION__C nvarchar(max),
	BI_DATA_CRM_SKILLSET__C nvarchar(max),
	PROJECT_SERVICES_QUALIFICATION__C nvarchar(max),
	PROJ_SERVICES_SKILLSET__C nvarchar(max),
	SUPPORT__C nvarchar(max),
	SUPPORT_SUB_COMMUNITIES__C nvarchar(max),
	INDUSTRIOUS__C nvarchar(max),
	INDUSTRIOUS_SUB_COMMUNITIES__C nvarchar(max),
	SSM__C nvarchar(max),
	SSM_SUB_COMMUNITIES__C nvarchar(max),
	COMPANIES_PACKAGES__C nvarchar(max),
	DIGITAL_QUALIFICATION__C nvarchar(max),
	DIGITAL_SKILLS__C nvarchar(max)
)

----------------
----------------
CREATE TABLE [dbo].[CandidateFE] (
	ID varchar(100),
	ACCOUNTID varchar(100),
	LASTNAME nvarchar(max),
	FIRSTNAME nvarchar(max),
	RECORDTYPEID nvarchar(max),
	ACCOUNTING_FINANCE__C nvarchar(max),
	A_F_SUB_COMMUNITY__C nvarchar(max),
	DEVELOPMENT_QUALIFICATION__C nvarchar(max),
	DEVELOPMENT_SKILLSET__C nvarchar(max),
	INFRASTRUCTURE_QUALIFICATION__C nvarchar(max),
	INFRA_SKILLSET__C nvarchar(max),
	BI_DATA_CRM_QUALIFICATION__C nvarchar(max),
	BI_DATA_CRM_SKILLSET__C nvarchar(max),
	PROJECT_SERVICES_QUALIFICATION__C nvarchar(max),
	PROJ_SERVICES_SKILLSET__C nvarchar(max),
	SUPPORT__C nvarchar(max),
	SUPPORT_SUB_COMMUNITIES__C nvarchar(max),
	INDUSTRIOUS__C nvarchar(max),
	INDUSTRIOUS_SUB_COMMUNITIES__C nvarchar(max),
	SSM__C nvarchar(max),
	SSM_SUB_COMMUNITIES__C nvarchar(max),
	COMPANIES_PACKAGES__C nvarchar(max),
	DIGITAL_QUALIFICATION__C nvarchar(max),
	DIGITAL_SKILLS__C nvarchar(max)
)


CREATE TABLE [dbo].[VincereUserAccount] (
	ID INT,
	name nvarchar(max),
	email nvarchar(max)
)

----------------
----------------
CREATE TABLE [dbo].[CandidateOwner] (
	ID varchar(100),
	CandidateOwner nvarchar(max)
)