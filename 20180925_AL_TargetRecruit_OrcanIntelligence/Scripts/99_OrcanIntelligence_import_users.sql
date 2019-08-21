select count (*) from (
select
trim(isnull(u.CommunityNickname, '')) as [name] 
, u.Username as email
, 'b5a482296aa19f41c25821ec3de8245c8fa235014659a200' as encrypted_password
, 1 as super_user
, cast(CreatedDate as datetime) as insert_timestamp
, 1 as internal_staff
, 1 as hr
, 1 as interviewer
, 'en' as [language]
, 'f4af6f325556316b8fa235014659a200' as email_password
, 'STANDARD' as email_api
, 'United Kingdom' as user_location
, 'Europe/London' as timezone
, 1 as requisition_signatory
, 1 as matrix_manager
, 1 as business_partner 
, 0 as system_admin
, 0 as length_calendar_password
, 2 as length_email_password
, 1 as director
, 1 as manager
, 1 as consultant
, 1 as researcher
, 'MICROSOFT_EXCHANGE' as email_support_type
, 0 as exchange_version
, 'pound' as currency_type
, 'dd/MM/yyyy' as [date_format]
, 0 as ssl_tls
, 0 as [authentication]
, '{"value":"5","accountIds":""}' as kpi_permission
, 'SUMMARY' as default_candidate_view
, 1 as default_document_view
, 1 as display_training
, 2 as job_dashboard_default_view
, '1e25dffa766fcffbbb7d8e0aac7aaf11d268ed625bc897d9' as hash_password
, 1000 as hash_iterations
, 1 as display_intro
, 0 as pin_comment
, 1 as new_search
, 0 as view_mode
, 0 as email_connected
, '1,2,3,4,5,6' as position_of_box
, 14004060636 as freshdesk_user_id
, 0 as outgoing_authentication
, 0 as outgoing_encrypt_type
, 'mile' as distance_unit
, 'Not specified' as town_city
, 1 as default_welcome_page_view
, 1 as candidate_quick_view_tab
, 0 as synced_to_cognito
, trim(isnull(u.FirstName, '')) as first_name
, trim(isnull(u.LastName, '')) as last_name
from [User] u
) abc where len(abc.email) <= 255