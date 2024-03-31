import os
import requests
from datetime import timedelta
import shutil
# from google.cloud import storage

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

    #by default loading date is yesterday
    load_date =(kwargs['execution_date'] - timedelta(days=1) ) \
        .strftime("%Y-%m-%d")

    #overwrite load_date if the value passed in p_load_date variable (YYYY-MM-DD)
    p_load_date = kwargs['p_load_date']
    if p_load_date != '':
        load_date = p_load_date         

    print(f'load_date = {load_date}')

    output_dir = f"data_{load_date}" 

     # Cleanup output dir if it exists
    shutil.rmtree(output_dir,  ignore_errors=True)

    # Create the directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)   

    for h in range(0,24):
        file_name = f"{load_date}-{h}.json.gz"
        url = f"https://data.gharchive.org/{file_name}"
        output_file = file_name
         # Full path of the output file
        output_path = os.path.join(output_dir, output_file)
        print(url)

        # Send a GET request to the URL
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Open a file in binary write mode
            with open(output_path, "wb") as file:
                # Write the content of the response to the file
                file.write(response.content)
            print("File downloaded successfully.")
        else:
            print(f"Error downloading the file. Status code: {response.status_code}")
            
    
    return output_dir  

    


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
