from azure.storage.blob import BlobServiceClient
from io import BytesIO

class BlobUtils:
    def __init__(self, constr) -> None:
        self.constr = constr

    # get client to communicate with blob storage
    def get_client(self, container, filepath):
        blob_service_client = BlobServiceClient.from_connection_string(self.constr)
        blob_client = blob_service_client.get_blob_client(container = container, blob = filepath)
        
        return blob_client

    # read a specified blob from a given container
    def read_file(self, container, filepath):
        stream = BytesIO()
        blob_client_in = self.get_client(container, filepath)
        streamdownloader = blob_client_in.download_blob()
        streamdownloader.readinto(stream)
        #TODO - decode need to be parametrized and have a default value
        file_reader = stream.getvalue().decode('utf-8-sig')

        return file_reader

    # upload file to a specified container
    def upload_data(self, container, filepath, file):
        blob_client_out = self.get_client(container, filepath)
        blob_client_out.upload_blob(data = file, overwrite = True)
        