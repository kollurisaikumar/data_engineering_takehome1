import pytest
import sys
from pathlib import Path
import findspark
findspark.init()
from pyspark.sql import SparkSession
import os

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['NUMEXPR_MAX_THREADS'] = '1'
os.environ['OMP_NUM_THREADS'] = '1'

sys.path.insert(0, str(Path(__file__).parent.parent))
from nycjobs import datacleaning, datatransfromation

@pytest.fixture(scope="session")
def spark_session():
    spark = (SparkSession.builder
             .master("local[1]")  # SINGLE THREAD ONLY
             .appName("test")
             .config("spark.sql.execution.arrow.pyspark.enabled", "false")
             .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1")
             .config("spark.python.worker.reuse", "false")
             .config("spark.sql.adaptive.enabled", "false")
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             .config("spark.sql.codegen.wholeStage", "false")
             .getOrCreate())
    yield spark
    spark.stop()

@pytest.fixture
def sample_df(spark_session):
    data = [("J001", "DOH", "80000", "120000", "Annual", "IT", "Bachelors"),
            ("J002", "NYPD", "100000", "140000", "Annual", "IT", "Masters"),
            ("J003", "DOT", "60000", "85000", "Annual", "HR", "Bachelors")]
    columns = ["Job ID", "Agency", "Salary Range From", "Salary Range To", 
               "Salary Frequency", "Job Category", "Minimum Qual Requirements"]
    return spark_session.createDataFrame(data, columns)  # NO CACHE!!!

class TestNYCJobsPipeline:
    def test_salary_conversion(self, spark_session, sample_df):
        result = datacleaning.convert_salary_to_numeric(sample_df)
        assert result is not None
        assert "Annual_Salary_Mid" in result.columns

    def test_category_extraction(self, spark_session, sample_df):
        result = datacleaning.extract_primary_job_category(sample_df)
        assert result is not None
        assert "Primary_Job_Category" in result.columns

    def test_qualification_categorization(self, spark_session, sample_df):
        result = datatransfromation.categorize_job_qualification(sample_df)
        assert result is not None
        assert "Qualification_Category" in result.columns

    def test_pipeline_end_to_end(self, spark_session, sample_df):
        df1 = datacleaning.convert_salary_to_numeric(sample_df)
        df2 = datacleaning.extract_primary_job_category(df1)
        df3 = datatransfromation.categorize_job_qualification(df2)
        assert df3 is not None
        assert len(df3.columns) >= 10  # Original 7 + 3 new columns
