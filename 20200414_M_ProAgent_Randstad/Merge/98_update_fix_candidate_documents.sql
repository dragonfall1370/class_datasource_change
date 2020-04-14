with abc as (select m.vc_candidate_id as candidate_id
--, cd.candidate_id origin_cand_id
--, m.external_id
, m.cand_ext_id || '_' || cd.uploaded_filename as uploaded_filename
, cd.saved_filename
, cd.version_no
, cd.document_type
, 0 as primary_document --all merged documents will not be primary
, cd.google_viewer
, cd.temporary
, cd.document_types_id
, cd.customer_portal
, cd.visible
, cd.uploaded_filename_bk
from candidate_document cd
join mike_tmp_candidate_dup_check m on m.vc_pa_candidate_id = cd.candidate_id --19836 rows
where cd.document_type <> 'candidate_photo' --candidate_photo must be checked by the latest date
--and cd.candidate_id in (219653, 207588, 201023)
and vc_candidate_id in (
43899,76120,45951,79486,111148,80417,104085,87417,84016,92408,69890,106322,106705,47315,44208,61752,46939,91575,101443,41218,86158,87819,64933,49968,45048,102084,43730,96871,94893,73900,82792,48491,48767)
)

update candidate_document cd
set saved_filename = abc.saved_filename
from abc
where abc.candidate_id = cd.candidate_id
and abc.uploaded_filename = cd.uploaded_filename
and abc.candidate_id in (
43899,76120,45951,79486,111148,80417,104085,87417,84016,92408,69890,106322,106705,47315,44208,61752,46939,91575,101443,41218,86158,87819,64933,49968,45048,102084,43730,96871,94893,73900,82792,48491,48767)