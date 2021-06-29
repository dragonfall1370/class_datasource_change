import pandas as pd

# %% if-then/if-then-else on one column, and assignment to another one or more columns:
df = pd.DataFrame({'AAA': [4, 5, 6, 7],
                   'BBB': [10, 20, 30, 40],
                   'CCC': [100, 50, -30, -50]})

# %% if then on one column
df.loc[df.AAA >= 5, 'DDD'] = -1

# %% An if-then with assignment to 2 columns
df.loc[df.AAA >= 5, ['BBB', 'CCC', ]] = 555

# %% apply’s callable is passed a sub-DataFrame which gives you access to all the columns

df = pd.DataFrame({'animal': 'cat dog cat fish dog cat cat'.split(),
                   'size': list('SSMMMLL'),
                   'weight': [8, 10, 11, 1, 20, 12, 12],
                   'adult': [False] * 5 + [True] * 2})

# # List the size of the animals with the highest weight.
df.groupby('animal').apply(lambda subf: subf['size'][subf['weight'].idxmax()])

# %% group by multi colum, get latest stage and the latest's stage date
df1 = pd.DataFrame(
    {
        'candidate_id': ['cand1', 'cand1', 'cand1', 'cand1', 'cand2', 'cand2', 'cand2', 'cand2', 'cand2', 'cand2'],
        'job_id': ['job_1', 'job_1', 'job_1', 'job_1', 'job_1', 'job_1', 'job_1', 'job_1', 'job_2', 'job_2'],
        'app_stage': [6, 5, 4, 3, 6, 6, 5, 4, 3, 1],
        'date': pd.date_range('2018-01-01', periods=10)
    }
)
df1.groupby(['candidate_id', 'job_id']).apply(lambda subdf: subdf.loc[subdf['app_stage'].idxmax(), ['app_stage', 'date']]).reset_index()

tem = df1.groupby(['candidate_id', 'job_id']).apply(lambda subdf: subdf.sort_values(['app_stage', 'date'], ascending=False))[['app_stage', 'date']]
tem.groupby(['candidate_id', 'job_id']).apply(lambda subdf: subdf.loc[subdf['app_stage'].idxmax(), ['app_stage', 'date']]).reset_index()
