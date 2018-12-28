SELECT 
  contact.phone, 
  contact.mobile_phone, 
  contact.home_phone, 
  contact.switchboard_phone, 
  contact.switchboard_phone_ext
FROM 
  public.contact
WHERE contact.phone is null or length(trim(contact.phone)) = 0
-- 292 