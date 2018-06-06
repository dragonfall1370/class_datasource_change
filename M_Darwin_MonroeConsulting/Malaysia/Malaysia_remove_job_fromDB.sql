1/ compensation

compensation_position_id__fkey -> key id 29832

2/ position_candidate_feedback -> key 29826 >> position_candidate_feedback__position_description_id__fkey

3/ 
update position_candidate_feedback set position_description_id = NULL where position_description_id is not NULL

4/
delete from position_description where external_id is not NULL