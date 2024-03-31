if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    from google.cloud import dataproc_v1 as dataproc

    # Initialize a Dataproc instance
    client = dataproc.JobControllerClient(client_options={
        'api_endpoint': '{}-dataproc.googleapis.com:443'.format('europe-west1')
    })

    # Define your cluster details
    project_id = 'de-zoomcamp-shamdzmi'
    region = 'europe-west1'
    cluster_name = 'dpc-zoomcamp'

    # Prepare your pyspark job details
    job_payload = {
        'placement': {
            'cluster_name': cluster_name
        },
        'pyspark_job': {
            'main_python_file_uri': 'gs://de-zoomcamp-shamdzmi-bucket/Code/dataproc_test.py'
        }
    }

    # Submit the job
    job_response = client.submit_job(project_id=project_id, region=region, job=job_payload)

    # Output a response
    print('Submitted job ID {}'.format(job_response.reference.job_id))


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
