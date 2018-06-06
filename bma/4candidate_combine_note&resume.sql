select * from candidate_note limit 100
create table candidate_note_truong as select * from candidate_note

select * delete from candidate_note_truong limit 100

select * delete from candidate_note  where title = 'Resume' limit 100
alter table candidate add column note_truong text

update candidate set note_truong = note where note <> '' and note is not null

select c.id, t.note, concat(c.note,chr(10),chr(10),chr(10),'------- RESUME -------',chr(10),t.note) as newnote
from candidate c
left join candidate_note_truong t on t.candidate_id = c.id limit 10

update candidate c
set note = concat(c.note,chr(10),chr(10),chr(10),'------- RESUME -------',chr(10),t.note)
from candidate_note_truong t where t.candidate_id = c.id 

