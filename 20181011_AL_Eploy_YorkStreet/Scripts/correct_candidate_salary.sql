SELECT id, name, annual_salary_from, annual_salary_to
  FROM public.position_description
  where id = 32271

  select * from compensation
  where position_id = 32271

select * from contact
where external_id = '606'