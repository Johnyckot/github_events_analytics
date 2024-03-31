if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def start_cluster(cluster_client,project_id,region,cluster_name):
    print('Starting cluster...')
    operation = cluster_client.start_cluster(
    request={"project_id": project_id, "region": region, "cluster_name": cluster_name}
    )        
    operation.result()
    # print(operation.result())

def stop_cluster(cluster_client,project_id,region,cluster_name):
    print('Stopping cluster...')
    operation = cluster_client.stop_cluster(
    request={"project_id": project_id, "region": region, "cluster_name": cluster_name}
    )        
    operation.result()
    # print(operation.result())

def is_running_cluster(cluster_client,project_id,region,cluster_name):
    cluster_request = cluster_client.get_cluster(
    request={"project_id": project_id, "region": region, "cluster_name": cluster_name}
    )
    return cluster_request.status.state == 2    

@custom
def load_data(*args, **kwargs):
    from google.cloud import dataproc_v1 as dataproc   
    import time
    import os
    from datetime import timedelta
    from google.cloud import storage 
 

    # Define your cluster details
    project_id = kwargs['p_project_id']
    region = kwargs['p_region']
    cluster_name = kwargs['p_cluster_name']
    flag_stop_cluster=kwargs['p_flag_stop_cluster'] 
    pyspark_file = kwargs['p_pyspark_file']
    gcs_bucket_name = kwargs['p_gcs_bucket_name']
    stage_path = kwargs['p_gcs_stage_path']
    raw_path = kwargs['p_gcs_raw_path']  
    

    # upload latest version of pyspark file for DataProc job into GCS
    local_pyspark_file='de-zoomcamp-prj/pyspark_etl/githunb_transform_raw_stage.py'
    client = storage.Client()
    bucket = client.bucket(gcs_bucket_name)
    blob = bucket.blob(pyspark_file.replace(f'gs://{gcs_bucket_name}/',''))
    blob.upload_from_filename(local_pyspark_file)
        

    #by default loading date is yesterday
    load_date =(kwargs['execution_date'] - timedelta(days=1) ) \
        .strftime("%Y-%m-%d")

    #overwrite load_date if the value passed in p_load_date variable (YYYY-MM-DD)
    p_load_date = kwargs['p_load_date']
    if p_load_date != '':
        load_date = p_load_date      

    print(load_date)       

    input_path = f"gs://{gcs_bucket_name}/{raw_path}{load_date.split('-')[0]}/{load_date.split('-')[1]}/{load_date.split('-')[2]}/*.json.gz"
    output_path = f"gs://{gcs_bucket_name}/{stage_path}"    

    cluster_client = dataproc.ClusterControllerClient(client_options={
        'api_endpoint': '{}-dataproc.googleapis.com:443'.format(region)
    })


    #Check the state of the Dataproc cluster and Start if not running
    if not is_running_cluster(cluster_client,project_id,region,cluster_name):
        print('Cluster is not in a Running state')
        start_cluster(cluster_client,project_id,region,cluster_name)

        # Initialize a Dataproc instance
    client = dataproc.JobControllerClient(client_options={
        'api_endpoint': '{}-dataproc.googleapis.com:443'.format(region)
    }) 


    # Prepare  pyspark job details
    job_payload = {
        'placement': {
            'cluster_name': cluster_name
        },
        'pyspark_job': {
            'main_python_file_uri': pyspark_file,
            "args":[
                f"--input_path={input_path}",
                f"--output_path={output_path}",
                f"--load_date={load_date}"
            ]
        }
    }

    # Submit the job
    job_response = client.submit_job(project_id=project_id, region=region, job=job_payload)

    # Output a response
    print('Submitted job ID {}'.format(job_response.reference.job_id))
   
    job_id = job_response.reference.job_id
    job_info = client.get_job(project_id=project_id, region=region, job_id = job_id)
    is_done = job_info.done
    state =  job_info.status.state
    while not is_done:
        time.sleep(3)
        job_info = client.get_job(project_id=project_id, region=region, job_id = job_id)
        state =  job_info.status.state
        is_done = job_info.done
        print(f"Job state: {state} , is_done = {is_done}")

    
    #  if the flag is set then stop Dataproc cluster
    if flag_stop_cluster != 'N':
        stop_cluster(cluster_client,project_id,region,cluster_name)
    
    return state




@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert output == 5, 'Job failed, pls check logs in DataProc'

