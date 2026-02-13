"""
this module contains various functions to be used for cleaning the data before processing,
pre-processing work is handled here
1. includes numeric and datetime spark conversions
2. deduplication of data based on job id 
"""
import findspark
findspark.init()
from py4j.protocol import Py4JJavaError

from pyspark.sql.functions import col, row_number, to_date
from pyspark.sql.functions import regexp_replace, regexp_extract, when
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from pyspark.sql.utils import AnalysisException, ParseException
from .logging_config import get_logger


logger = get_logger(__name__)

def dedup_by_job_id_keep_latest(df, job_id_col="Job ID", date_col="Posting Date"):
    """Deduplicate by Job ID, keeping most recent record based on Posting Date"""
    try:
        logger.info("Starting deduplication on data")

        if job_id_col not in df.columns:
            raise ValueError(f"Job ID column '{job_id_col}' not found")
        if "Posting Date" not in df.columns:
            raise ValueError(f"Date column '{date_col}' not found")

        logger.info("Deduplicating by %s using %s (latest first)", job_id_col , date_col)
        window_spec = (Window()
                      .partitionBy(col(job_id_col))
                      .orderBy(col(date_col).desc()))
        df_ranked = df.withColumn("rn", row_number().over(window_spec))
        df_dedup = df_ranked.filter(col("rn") == 1).drop("rn")
        logger.info("Deduplication completed")
        return df_dedup
    except AnalysisException as e:
        logger.error("Spark SQL error: %s",str(e))
        raise
    except ParseException as e:
        logger.error("Spark SQL parse error: %s",str(e))
        raise
    except ValueError as e:
        logger.error("Config error: %s",str(e))
        raise
    except Exception as e:
        logger.error("Unexpected error: %s",str(e), exc_info=True)
        raise

def convert_post_date_to_datetime(df, date_col="Posting Date"):
    """Convert Posting Date column to proper date type - Handles ISO datetime format"""
    try:
        logger.info("Starting date conversion for column: %s",date_col)
        df_clean = df.withColumn("Posting_Date",
                                to_date(col(date_col), "yyyy-MM-dd'T'HH:mm:ss.SSS"))

        logger.info("   Date conversion complete!")
        # Drop original column, keep ONLY clean Posting_Date
        result_df = df_clean.drop(date_col)
        logger.info("Date conversion pipeline completed successfully")
        return result_df

    except Py4JJavaError as e:
        logger.error("PySpark Java error during date conversion: %s",str(e))
        raise
    except Exception as e:
        logger.error("Unexpected error in date conversion: %s",str(e), exc_info=True)
        raise

def convert_salary_to_numeric(df):
    """Convert Salary Range From/To to numeric + create Salary Mid column"""
    try:
        logger.info("Starting salary conversion for NYC Jobs data")
        df_cleaned = (df.withColumn("Annual_Salary_From_clean",
                                   regexp_replace(col("Salary Range From"),
                                                 r'[^\d.]', '').cast(DoubleType()))
                     .withColumn("Annual_Salary_To_clean",
                               regexp_replace(col("Salary Range To"),
                                            r'[^\d.]', '').cast(DoubleType())))
        df_final = df_cleaned.withColumn("Annual_Salary_Mid",
                                        (col("Annual_Salary_From_clean") +
                                         col("Annual_Salary_To_clean")) / 2.0)
        df_final = df_final.drop("Annual_Salary_From_clean", "Annual_Salary_To_clean")
        logger.info("Salary conversion completed successfully")
        return df_final 
    except Exception as e:
        logger.error("Error in salary conversion: %s",str(e))
        raise

def extract_primary_job_category(df, job_category_col="Job Category"):
    """Extract FIRST job category before comma/ampersand/slash"""
    try:
        logger.info("Extracting Primary Job Category from: %s",job_category_col)
        df_result = df.withColumn("Primary_Job_Category",
                                 regexp_extract(col(job_category_col), r"^[^,&/]+", 0))
        logger.info("Primary Job category extraction completed")
        return df_result
    except AnalysisException as e:
        logger.error("Spark SQL analysis error: %s",str(e))
        raise
    except ParseException as e:
        logger.error("Spark SQL parse error: %s",str(e))
        raise
    except Exception as e:
        logger.error("Unexpected error in category extraction: %s",str(e), exc_info=True)
        raise

def normalize_salary_to_annual(df):
    """
    Convert all salary types (Daily/Hourly) to Annual equivalents in Annual_Salary_Mid
    
    Assumes 2080 working hours/year (40hr/week * 52 weeks)
    Assumes 260 working days/year (5 days/week * 52 weeks)
    """
    try:
        logger.info("Normalizing all salaries to Annual basis")
        df_normalized = df.withColumn("Annual_Salary_Mid",
            when(col("Salary Frequency") == "Hourly", col("Annual_Salary_Mid") * 2080)
            .when(col("Salary Frequency") == "Daily", col("Annual_Salary_Mid") * 260)
            .when(col("Salary Frequency") == "Annual", col("Annual_Salary_Mid"))  # Already annual
            .otherwise(col("Annual_Salary_Mid"))  # Default: assume annual
        )
        logger.info("  Salary normalization complete")
        return df_normalized
    except Exception as e:
        logger.error("Salary normalization failed: %s",str(e))
        raise
