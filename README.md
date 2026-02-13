# Solution Provided 

# Exploratory Data Analysis
Based upon the information provided in the readme.md file following KPIs are identified  
- List of KPIs to be resolved:
  - Whats the number of jobs posting per category (Top 10)? 
  - Whats the salary distribution per job category? 
  - Is there any correlation between the higher degree and the salary?
  - Whats the job posting having the highest salary per agency? 
  - Whats the job positings average salary per agency for the last 2 years? 
  - What are the highest paid skills in the US market? 

Accordingly they are resolved -  complete details on EDA done can be identified at [Exploratory_data_analysis.ipynb](https://github.com/kollurisaikumar/data_engineering_takehome1/blob/feature/sai_develop/jupyter/notebook/Exploratory_data_analysis.ipynb)  


# Data Processing solution
As per the provided details, here are the points covered in the provided solution  
 dedup_by_job_id_keep_latest()        - Deduplication by Job ID  
 convert_salary_to_numeric()          - Salary From/To → Midpoint  
 convert_post_date_to_datetime()      - ISO datetime parsing  
 extract_primary_job_category()       - Regex category extraction  
 normalize_salary_to_annual()         - Salary frequency normalization  
 categorize_job_qualification()       - Feature engineering #1  
 flag_salary_vs_qualification()       - Feature engineering #  
 extract_generalized_skills()         - Feature engineering #3  

Required 3+ Feature engineering techniques in the solution 
following are the feature engineering solutions used 
1. Primary_Job_Category (regex extraction)  
2. Qualification_Category (text classification)  
3. Salary_Qual_Flag (dynamic median bucketing) 
4. top_skills (multi-column skill extraction)  
5. Annual_Salary_Mid (salary normalization)    

# Outputs created by the pipeline
File 1: Skill_Salary_Distribution.csv         → KPI 1,2 (Top 10 categories + salary dist + skills)
File 2: Degree_Salary_Correlation.csv         → KPI 3 (Degree-salary correlation)  
File 3: Agency_insight.csv                    → KPI 4,5 (Agency max/avg salaries)
File 4: Highest_Paid_Skill.csv                → KPI 6 (Highest paid skills in US)     


# How to run 
to run the data pipeline run the main.py file use command

python-m ./code/main.py

to run the test_main.py test cases written for the main.py file use command

python -m pytest .code/tests/ -v -s
