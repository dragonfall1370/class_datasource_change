SELECT 
  position_description.full_description, 
  position_description.public_description, 
  position_description.id, 
  position_description.external_id, 
  position_description.summary
FROM 
  public.position_description;
