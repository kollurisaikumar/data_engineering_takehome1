"""
Docstring for code.nycjobs.functions
contains functions used in main.py reusable components 
"""

import json
import os
import shutil
from datetime import datetime, timedelta
import findspark
findspark.init()
from pyspark.sql.functions import (
    col, count, avg, max as spark_max,
      min as spark_min, desc, expr,
    round as spark_round, countDistinct,to_date
)
from .logging_config import get_logger
logger = get_logger(__name__)


def load_config(config_path):
    """Load configuration from JSON file"""
    with open(config_path, 'r', encoding='UTF-8') as f:
        return json.load(f)

def write_single_csv(spark_df, keyword, output_dir):
    """
    Docstring for write_single_csv
    
    :param spark_df: dataframe to write as csv
    :param keyword: name of the csv file
    :param output_dir: location of file to be saved
    """
    try:
        # Create timestamp folder
        timestamp = datetime.now().strftime("%Y%m%d")
        timestamp_dir = f"{output_dir}/{timestamp}"
        os.makedirs(timestamp_dir, exist_ok=True)
        ## timestamp based directory and file name is constructed here
        final_path=f"{timestamp_dir}/{keyword}.csv"
        temp_dir = f"{timestamp_dir}/_temp_{os.urandom(4).hex()}"
        # Write single partition to temp folder
        spark_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)
        # Find the single part file created
        part_files = [f for f in os.listdir(temp_dir) if f.startswith("part-00000")]
        if not part_files:
            raise FileNotFoundError ("No part file generated")
        part_file_path = os.path.join(temp_dir, part_files[0])
        # Rename part file to final CSV filename
        shutil.move(part_file_path, final_path)
        # Remove temporary folder
        shutil.rmtree(temp_dir)
        logger.info("Saved single CSV: %s",final_path)
    except Exception as e:
        logger.info("Unable to save single csv file : %s : %s",final_path,str(e))
        raise

def skill_salary_distribution(df):
    """
    Docstring for skill_salary_distribution
    
     Group by ACTUAL Primary_Job_Category - REMOVED non-existent top_skills
    """
    try:
        #
        return df.groupBy("Primary_Job_Category").agg(
            count("*").alias("job_count"),
            spark_round(avg("Annual_Salary_Mid"), 0).alias("avg_salary"),
            expr("percentile_approx(Annual_Salary_Mid, 0.5)").alias("median_salary"),
            spark_min("Annual_Salary_Mid").alias("min_salary"),
            spark_max("Annual_Salary_Mid").alias("max_salary")
            # REMOVED: spark_round(avg(size(col("top_skills"))), 1).alias("avg_skills_per_job")
        ).filter(col("job_count") > 1)
    except Exception as e:
        logger.info("Unable to calculate skill salary distribution : %s",str(e))
        raise



def degree_salary_correlation(df):  
    """
    calculated and creates correlation table between Education qualification and salary
    """
    try:
        degree_stats = (df
            .groupBy("Qualification_Category", "Salary_Qual_Flag")
            .agg(
                count("*").alias("job_count"),
                spark_round(avg("Annual_Salary_Mid"), 0).alias("avg_salary")
            )
            .orderBy("Qualification_Category", desc("job_count")))
        logger.info("correlation table between degree and salary is created")
        return degree_stats
    except Exception as e:
        logger.info("unable to create salary degree correlation table : %s",str(e))
        raise

def agency_insights(df, min_jobs=3):
    """
    Agency insights: Last 2 years job count, avg salary, highest job premium, salary range
    Dynamically picks latest date - 2 years for filtering
    """
    try:
        logger.info("Starting agency insights analysis...")
        # Convert Posting_Date to date and find dynamic 2-year window
        df_with_date = df.withColumn("Posting_Date", to_date(col("Posting_Date")))
        max_date = df_with_date.agg(spark_max("Posting_Date")).collect()[0][0]
        if max_date is None:
            logger.warning("No valid dates found, using full dataset")
            recent_df = df
        else:
            cutoff_date = max_date - timedelta(days=730)  # 2 years back
            logger.info("Filtering: %s to %s",cutoff_date,max_date)
            recent_df = df_with_date.filter(col("Posting_Date") >= cutoff_date)
        # Agency stats
        agency_stats = recent_df.groupBy("Agency").agg(
            count("*").alias("job_count_2yr"),
            avg("Annual_Salary_Mid").alias("avg_salary_2yr"),
            spark_max("Annual_Salary_Mid").alias("High_salary_per_agency"),
            spark_min("Annual_Salary_Mid").alias("Min_salary_per_agency"),
            countDistinct("Job ID").alias("unique_postings")
        ).filter(col("job_count_2yr") >= min_jobs)
        # Global avg for premium calculation
        global_avg = recent_df.agg(avg("Annual_Salary_Mid")).collect()[0][0]
        logger.info("Global avg salary (2yr): %s",global_avg)
        # Find highest salary JOB per agency and calculate premium
        highest_salary_jobs = recent_df.groupBy("Agency").agg(
            spark_max("Annual_Salary_Mid").alias("max_salary_job")
        )
        # Join and calculate premiums
        final_stats = agency_stats.join(highest_salary_jobs, "Agency").withColumn(
            "salary_premium", 
            col("max_salary_job") - col("avg_salary_2yr")
        ).withColumn(
            "premium_pct_per_agency", 
        spark_round((col("salary_premium") / col("avg_salary_2yr") * 100), 1)
        ).select(
            "Agency", "job_count_2yr", "avg_salary_2yr", "salary_premium",
            "premium_pct_per_agency", "High_salary_per_agency", "Min_salary_per_agency"
        ).orderBy(col("avg_salary_2yr").desc())
        logger.info("Generated insights for %s agencies",final_stats.count())
        return final_stats
    except Exception as e:
        logger.error("Error in agency_insights_2yr: %s",str(e))
        raise
