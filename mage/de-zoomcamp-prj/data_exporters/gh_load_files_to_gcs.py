import os
import shutil
from google.cloud import storage
from datetime import timedelta

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports files to GCS bucket

    Args:
        data: The output from the upstream parent block. Expected the name of the directory with downloaded files.
        args: The output from any additional upstream blocks (if applicable)    
    """
    # Specify your data exporting logic here

    print(f'data= {data}')
    print(f"exec_date = {kwargs['execution_date']}")
    print(f"p_load_date = {kwargs['p_load_date']}")

    #by default loading date is yesterday
    load_date =(kwargs['execution_date'] - timedelta(days=1) ) \
        .strftime("%Y-%m-%d")

    #overwrite load_date if the value passed in p_load_date variable (YYYY-MM-DD)
    p_load_date = kwargs['p_load_date']
    if p_load_date != '':
        load_date = p_load_date         

    print(f'load_date = {load_date}')

    input_dir = f"data_{load_date}"    
    if data != '':
        input_dir = data

    print(input_dir)

    #    Create a GCS client
    client = storage.Client()    
    
    bucket = client.bucket(kwargs['p_gcs_bucket_name'])    
    print ('Bucket name: ',kwargs['p_gcs_bucket_name'])
    raw_prefix = f"{kwargs['p_gcs_raw_path']}{load_date.split('-')[0]}/{load_date.split('-')[1]}/{load_date.split('-')[2]}"
    
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            file_path = os.path.join(root, file)
            blob_path = raw_prefix+'/'+os.path.relpath(file_path, input_dir)            
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(file_path)
            print(f"Uploaded {blob_path} to GCS.")

    # Cleanup output dir
    shutil.rmtree(input_dir)  
     


