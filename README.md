---------------------------------------------------------------------------------
------------- ETL PIPELINE FOR PREPARING COVID-19 DATA IN AWS CLOUD -------------
---------------------------------------------------------------------------------

python version == 3.9.13

STEPS:

    1. CREATE A S3 BUCKET IN AWS.
    
    2. UPDATE THE DATASET (/dataset FOLDER) INTO THE S3 BUCKET.
    
    3. CREATE A NEW BUCKET FOR STORE RESULTS FROM ATHENA QUERYING.
    
    4. CREATE A GLUE CRAWLER FOR EACH DATASET FILE TO ENABLE QUERYING S3 DATA FROM ATHENA.
    
    5. CREATE A REDSHIFT CLUSTER (MUST BE IN PRODUCTION, NOT FREE TRIAL BECAUSE YOU CAN NOT UPLOAD DATA INTO TRIAL CLUSTER IN AWS) FOR UPLOAD TRANSFORMED DATA (THE DATA MODEL USED IS SHOWN IN dimension_model.pdf).
    
    6. CHANGE CONFIG FILE NAME FROM fake_config.json TO CONFIG.JSON AND UPDATE ITS VALUES.
    
    7. EXECUTE main.py
