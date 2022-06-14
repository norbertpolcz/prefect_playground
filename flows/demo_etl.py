from prefect import task, Flow
import pandas as pd
import io
from io import BytesIO
import datetime
from dotenv import load_dotenv

from prefect import task, Flow
from prefect.storage import Azure
from prefect.run_configs import DockerRun
from connectors import blob_storage as bs


# set vars
load_dotenv()

# dowload csv from blob storage
@task
def extract():
    blobUtil = bs.BlobUtils(CONN_STR)
    blobUtil.get_client(container_name, file_path_in)
    my_blob = blobUtil.read_file(container_name, file_path_in)
    df = pd.read_csv(io.StringIO(my_blob), sep = ',')

    return df

# add transformation date
@task
def transform(df):
    df["TransformationDate"] = datetime.datetime.utcnow()
    df = df.astype({'TransformationDate':'string'})
    
    return df

# upload json to blob storage
@task
def load(df):
    file = BytesIO()
    df.to_json(file, orient='records')
    file.seek(0)
    blob_client = bs.BlobUtils(CONN_STR)
    blob_client_out = blob_client.get_client(container_name, file_path_out)
    blob_client_out.upload_blob(data = file, overwrite = True)

    return


# define flow from tasks
with Flow("demo_etl") as flow:
    data = extract()
    transformed = transform(data)
    load(transformed)


# set storage
flow.storage = Azure(container="rawzone", connection_string_secret="azureblobstorage", stored_as_script=True, blob_name="23542_prefect_pipelines/flows/demo_etl.py")
# set run config, labels, dependent pip packages
flow.run_config = DockerRun(labels={"axpo"}, env={"EXTRA_PIP_PACKAGES": "pandas sqlalchemy"})
# register flow to orchestration layer
flow.register(project_name="tutorial")
