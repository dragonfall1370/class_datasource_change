select cn_id, iif(cn_english_ability = 0,concat('English Language ','No Data')
	,iif(cn_english_ability = 5,concat('English Language ','Good'),NULL)) from tblCandidate where cn_english_ability is not NULL

select distinct cn_other_language1 from tblCandidate where cn_other_language1 is not NULL -- 15 rows

select distinct cn_other_language2 from tblCandidate where cn_other_language2 is not NULL -- 15 rows

select distinct cn_pers_score from tblCandidate where cn_pers_score is not NULL -- 15 rows

select distinct cn_conf_score from tblCandidate where cn_conf_score is not NULL -- 15 rows

select distinct cn_pr_test from tblCandidate where cn_pr_test is not NULL -- 15 rows

select distinct cn_corresp_test from tblCandidate where cn_corresp_test is not NULL -- 15 rows

select distinct cn_excel_test from tblCandidate where cn_excel_test is not NULL -- 15 rows

select distinct cn_computer_skills from tblCandidate where cn_computer_skills is not NULL -- 0 rows

select distinct cn_typing_en from tblCandidate where cn_typing_en is not NULL -- all 0

select distinct cn_typing_th from tblCandidate where cn_typing_th is not NULL -- all 0

select cn_other_itskills from tblCandidate where cn_other_itskills is not NULL -- all blank

select cn_skill_personality from tblCandidate where cn_skill_personality is not NULL

select cn_skill_confidence from tblCandidate where cn_skill_confidence is not NULL

select cn_skills from tblCandidate where cn_skills is not NULL 

select distinct cn_eng_confidence from tblCandidate where cn_eng_confidence is not NULL -- all 0

select distinct cn_eng_clarity from tblCandidate where cn_eng_clarity is not NULL -- all 0

select distinct cn_eng_grammar from tblCandidate where cn_eng_grammar is not NULL -- all 0

select distinct cn_eng_vocab from tblCandidate where cn_eng_vocab is not NULL -- all 0

select distinct cn_eng_explain from tblCandidate where cn_eng_explain is not NULL -- all 0

select distinct cn_eng_speed from tblCandidate where cn_eng_speed is not NULL -- all 0

select distinct cn_eng_listen from tblCandidate where cn_eng_listen is not NULL
