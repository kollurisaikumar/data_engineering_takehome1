"""
Docstring for code.main
main trigger script file to trigger data pipeline 
"""
# Import modules
import os
from nycjobs import datacleaning, datatransfromation, functions
from nycjobs.logging_config import setup_pipeline_logging, get_logger
import findspark
findspark.init()
from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local").appName("NYC-Jobs-Spark-Application").getOrCreate()
if __name__ == "__main__":
    logger = get_logger(__name__)
    logger.info("Initializing spark pipeline")

    ## picking up jsons from the config folder
    config =functions.load_config("D:/Sai/data_engineering_takehome1/code/config/paths.json")

    input_dir = config["input_dir"]
    output_dir = config["output_dir"]
    log_dir = config["log_dir"]

    # Create directories
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)
    setup_pipeline_logging(log_dir)

    try:
        # === STEP 1: Loading Data from input directory as a spark dataframe ===
        logger.info("Loading data from: %s/nyc-jobs.csv",input_dir)
        jobs_df = spark.read.option("header", "true")\
            .option("inferSchema","true")\
            .csv(f"{input_dir}/nyc-jobs.csv")
        ### === STEP 2 : Cleaning data
        jobs_df = datacleaning.dedup_by_job_id_keep_latest(df=jobs_df)
        jobs_df = datacleaning.convert_salary_to_numeric(df=jobs_df)
        jobs_df = datacleaning.convert_post_date_to_datetime(df=jobs_df)
        jobs_df = datacleaning.normalize_salary_to_annual(df=jobs_df)
        jobs_df = datacleaning.extract_primary_job_category(df=jobs_df)


        ## ===STEP 3 === : data transformation functions
        jobs_df = datatransfromation.categorize_job_qualification(df=jobs_df)
        jobs_df = datatransfromation.flag_salary_vs_qualification(df=jobs_df)
        jobs_df = datatransfromation.extract_generalized_skills(df=jobs_df)
        highest_paid_skills_df = datatransfromation.highest_paid_skills(jobs_df)

        # === STEP 4: Generate 3 KPI Files ===
        ## FILE 1: Skills Salary Distribution (KPI 1,2)
        skill_salary_distribution_df = functions.skill_salary_distribution(jobs_df)
        functions.write_single_csv(skill_salary_distribution_df,
                                    "Skill_Salary_Distribution",
                                    output_dir)
        # FILE 2: Degree Salary Correlation (KPI 3)
        degree_salary_correlation_df = functions.degree_salary_correlation(jobs_df)
        functions.write_single_csv(degree_salary_correlation_df,
                                   "Degree_Salary_Correlation",
                                   output_dir)

        # FILE 3: Agency Insights (KPI 4+5)
        agency_insights_df = functions.agency_insights(jobs_df)
        functions.write_single_csv(agency_insights_df,
                                   "Agency_insights",
                                   output_dir)

        # FILE 4: Highest paid skills (KPI 6)
        functions.write_single_csv(highest_paid_skills_df,
                                   "Highest_Paid_Skills",
                                   output_dir)

    finally:
        spark.stop()
        logger.info("Spark session closed - Pipeline complete!")
