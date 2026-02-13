"""
this module contains various functions to be used for data transformation,
pre-processing work is handled here
1. includes qualification categorization and salary benchmarking
"""
from pyspark.sql.functions import (
    col, lower, when, expr, regexp_extract, split,
    concat_ws, coalesce, lit, explode, desc, size,
    regexp_extract, split, slice, udf, countDistinct, count, avg,
    lit, round as spark_round, length, array
    )

from pyspark.sql.types import DoubleType
import re
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException, ParseException
from py4j.protocol import Py4JJavaError
import findspark
findspark.init()
from .logging_config import get_logger

logger = get_logger(__name__)

skills_json = {
  "technical": {
    "engineering": ["cad", "solidworks", "autocad", "matlab", "ansys", "revit"],
    "construction": ["procore", "bluebeam", "primavera", "ms project"],
    "healthcare": ["epic", "cerner", "ehr", "hl7", "emr"],
    "finance": ["sap", "oracle", "peoplesoft", "quickbooks", "salesforce"],
    "legal": ["lexisnexis", "westlaw", "pacers", "case management"]
  },
  "it_data": {
    "programming": ["python", "java", "sql", "javascript", "r", "scala", "c++"],
    "cloud": ["aws", "azure", "gcp", "docker", "kubernetes"],
    "data": ["tableau", "powerbi", "snowflake", "databricks", "spark"]
  },
  "certifications": {
    "project": ["pmp", "agile", "scrum", "six sigma"],
    "finance": ["cpa", "cfa", "cfp", "frs"],
    "health": ["rn", "cna", "lpn", "emt"],
    "safety": ["osha", "hazwoper", "first aid", "cpr"]
  },
  "professional": {
    "leadership": ["supervisory", "management", "team lead", "director"],
    "communication": ["public speaking", "negotiation", "stakeholder"],
    "analysis": ["data analysis", "financial modeling", "risk assessment"]
  },
  "operations": {
    "maintenance": ["hvac", "plumbing", "electrical", "carpentry"],
    "procurement": ["vendor management", "contract negotiation", "rfp"],
    "hr": ["recruiting", "talent acquisition", "onboarding"]
  }
}


def categorize_job_qualification(df, req_col="Minimum Qual Requirements"):
    """Categorize job qualifications from requirements text"""
    try:
        logger.info(f"Categorizing qualifications from column: {req_col}")
        df_result = df.withColumn("Qualification_Category",
            when(lower(col(req_col)).contains("master"), "Masters Degree")
            .when(lower(col(req_col)).contains("baccalaureate"), "Bachelors Degree")
            .when(lower(col(req_col)).contains("diploma"), "Diploma")
            .when(lower(col(req_col)).contains("matriculat"), "Matriculation")
            .otherwise("Experienced")
        )
        logger.info(" Qualification categorization complete")
        return df_result
    except Exception as e:
        logger.error(f"Qualification categorization failed: {str(e)}")
        raise

def flag_salary_vs_qualification(df):
    """
    Flag salary vs qualification median benchmarks (Fixed window syntax)
    Categories: underpaid, below standard, industry standard, above standard, highly paid
    """
    try:
        logger.info("Computing qualification-based salary benchmarks")
        
        # Step 1: Calculate median salaries per qualification (aggregate)
        median_df = (df
            .groupBy("Qualification_Category")
            .agg(expr("percentile_approx(Annual_Salary_Mid, 0.5)").alias("qual_median_salary"))
            .cache())  # Cache for join performance
        
        # Step 2: Join medians back and apply thresholds
        df_result = (df
            .join(median_df, "Qualification_Category", "left")
            .withColumn("Salary_Qual_Flag",
                when(col("Annual_Salary_Mid") < col("qual_median_salary") * 0.85, "underpaid")
                .when(col("Annual_Salary_Mid") < col("qual_median_salary") * 0.95, "below standard")
                .when(col("Annual_Salary_Mid") <= col("qual_median_salary") * 1.105, "industry standard")
                .when(col("Annual_Salary_Mid") <= col("qual_median_salary") * 1.115, "above standard")
                .otherwise("highly paid")
            )
            .drop("qual_median_salary")  # Clean up
            .cache())
        median_df.unpersist()  # Memory cleanup
        logger.info(" Salary vs qualification flagging complete")
        return df_result
        
    except Exception as e:
        logger.error(f"Salary qualification flagging failed: {str(e)}", exc_info=True)
        raise


def extract_generalized_skills(df):
    """Just creates combined_text - NO complex operations"""
    try:
        logger.info("Creating combined text for skills analysis")
        df_text = df.withColumn("combined_text", 
            lower(concat_ws(' ', 
                coalesce(col('Preferred Skills'), lit('')),
                coalesce(col('Job Description'), lit('')),
                coalesce(col('Minimum Qual Requirements'), lit(''))
            ))
        )
        logger.info("  combined_text created successfully")
        return df_text
    except Exception as e:
        logger.error(f"Text extraction failed: {str(e)}")
        raise

def highest_paid_skills(df, min_jobs=3):
    try:
        logger.info("Starting highest paid skills analysis...")
        
        # Use existing Annual_Salary_Mid (created by datacleaning)
        df_ready = df.filter(col("Annual_Salary_Mid").isNotNull())
        
        # Use module-level skills_json
        all_skills = []
        for category, subcats in skills_json.items():
            for subcategory, keywords in subcats.items():
                all_skills.extend(keywords)
        
        skills_pattern = "|".join([re.escape(skill.lower()) for skill in all_skills])
        
        df_with_skills = df_ready.withColumn(
            "extracted_skills",
            expr(f"regexp_extract_all(combined_text, '\\\\b({skills_pattern})\\\\b', 1)")
        )
        
        exploded_df = df_with_skills.select(
            expr("explode(extracted_skills)").alias("skill"),
            "Annual_Salary_Mid", 
            "Job ID"
        ).filter(col("skill").isNotNull() & (length(col("skill")) > 0))
        
        skill_stats = exploded_df.groupBy("skill").agg(
            count("Annual_Salary_Mid").alias("job_count"),
            avg("Annual_Salary_Mid").alias("avg_salary"),
            countDistinct("Job ID").alias("unique_jobs")
        ).filter(col("job_count") >= min_jobs)
        
        global_avg = df_ready.agg(avg("Annual_Salary_Mid")).collect()[0][0]
        final_stats = skill_stats.withColumn(
            "salary_premium", col("avg_salary") - lit(global_avg)
        ).withColumn(
            "premium_pct", spark_round((col("salary_premium") / lit(global_avg)) * 100, 1)
        ).orderBy(col("avg_salary").desc()).limit(20)
        
        logger.info(f"  Generated {final_stats.count()} top skills")
        return final_stats
        
    except Exception as e:
        logger.error(f"Highest paid skills failed: {str(e)}")
        raise
