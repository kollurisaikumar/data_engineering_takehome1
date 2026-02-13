## Explanadtion of code 
Code folder conists of my code submission for pyspark data pipeline

Created nycjobs package to avoid monolithi file based pipeline, it contains the following files in it:  
├── nycjobs/  
│   ├──__init.py  
│   ├── datacleaning.py  
│   └── datatransfromation.py  
│   └── functions.py  
│   └── loggin_config.py  
├── tests/  
│   └── test_main.py  

datacleaning.py -  contains purpose built data cleaning and wrangling functions  
datatransformations.py - contains transformations reaquired to transform data accordingly to composed KPIs  
functions.py - purpose built functions to support data ahdnling processes and file saving  
logging_config.py prpose built logging module to create logs for data pipelines  
  
  main.py - contains the main triggering code, well composed to handle file reading, data extractions, transformation and loading outputs
  
tests/ - contains pytest codes to test the mail
test_main.py -  contains pytest codes to test the main.py moudle

