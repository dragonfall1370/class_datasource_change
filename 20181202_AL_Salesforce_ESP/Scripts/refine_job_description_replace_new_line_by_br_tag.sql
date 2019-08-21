update position_description
set
public_description = regexp_replace(public_description, e'[\\n]{1}', '<br>', 'g' )
--, full_description = regexp_replace(public_description, e'[\\n]{1}', '<br>', 'g' )
--, summary = regexp_replace(public_description, e'[\\n]+', '<br>', 'g' )
--where id = 35369

-- update position_description
-- set
-- full_description = public_description
--where id = 35369