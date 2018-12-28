SELECT 
  candidate.preferred_work_phone, 
  candidate.preferred_home_phone, 
  candidate.preferred_phone, 
  candidate.preferred_mobile_phone, 
  candidate.emergency_phone, 
  candidate.home_phone, 
  candidate.work_phone, 
  candidate.phone2, 
  candidate.phone
FROM 
  public.candidate
WHERE
  phone is null or length(trim(phone)) = 0

 -- 52


-- UPDATE
--   public.candidate
-- SET
--   phone =
--     CASE WHEN phone2 is not null and length(trim(phone2)) > 0 THEN phone2
--     ELSE phone
--     END
-- WHERE
--   phone is null or length(trim(phone)) = 0