### H3 Create table from your query
Use it for:
    - _01_company_sample
    - _02_contact_sample
    - _03_job_sample
    - _04_candidate_sample

** For Postgres SQL
Put below statement on top of your query:

```DROP TABLE IF EXISTS _01_company_sample;
CREATE TABLE _01_company_sample AS
```

** For SQL Server
Put below statement on top of your query:
```DROP TABLE IF EXISTS _01_company_sample;
```

and put ```INTO _01_company_sample``` right before ```FROM``` in main query

*Example: 
    ```WITH(...)
    SELECT ...
    INTO _01_company_sample
    FROM ...
    ```

### H3 Put sample data to google spreadsheet
Open terminal/command and install below packages:
- pip install pandas
- pip install pygsheets
- pip install pathlib
- pip install psycopg2
- pip install pymysql
- pip install sqlalchemy
- pip install pyodbc

Change directory to sample_data folder
*cd {path}/sample_data/*
run script
*python get_data_to_gsheet.py "{google_spreadsheet_url}" "{region}" "{db_type}" "{db_name}" {limit}*

Params:
    - google_spreadsheet_url(string): url of spreadsheet
    - region(string): fra, us, sing
    - db_type(string): postgres, sql_server
    - db_name(string): database name
    - limit(integer): number of sample data you want to put on spreadsheet

**NOTE:
2 ways to authorize with google spreadsheets:
    - Enable shareable link and change permission to 'Any one with the link can edit'
    - Share spreadsheet with this account: maverick@formidable-feat-241405.iam.gserviceaccount.com