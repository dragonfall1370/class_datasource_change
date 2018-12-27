with test as (select ROW_NUMBER() over (partition by [candidate-email] order by [candidate-email]) as row_num,* from candidateskip)

select iif([candidate-email] is null or [candidate-email] = '','',iif(row_num = 1,concat('2-',[candidate-email]),concat((row_num+1),'-',[candidate-email]))) as [candidate-email],* from test


