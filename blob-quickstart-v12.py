import os, uuid, time
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

try:
    print("Azure Blob Storage v" + __version__ + " - Python quickstart sample")

    # Quick start code goes here

    # Retrieve the connection string for use with the application. The storage
    # connection string is stored in an environment variable on the machine
    # running the application called AZURE_STORAGE_CONNECTION_STRING. If the environment variable is
    # created after the application is launched in a console or with Visual Studio,
    # the shell or application needs to be closed and reloaded to take the
    # environment variable into account.
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # the program and the directory to be uploaded is under the same directory
    # container name is the same as the parent directory name
    container_name = input("Enter the name of the directory you want to upload:")

    ### ADD ERROR CHECKING HERE

    # Create the container
    container_client = blob_service_client.create_container(container_name)

    # for each file found by os.walk, upload it onto cloud
    for (root, dirs, files) in os.walk(container_name):
        for file_name in files:
            upload_file_path = os.path.join("./" + root + "/", file_name)

            # Create a blob client using the file name and path as the name for the blob
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=root[len(container_name) + 1:] + "/" + file_name)

            print("\nUploading to Azure Storage as blob:\n\t" + file_name)
            start = time.time()
            # Upload the created file
            with open(upload_file_path, "rb") as data:
                blob_client.upload_blob(data)
            end = time.time()

            print("finished in " + str(end - start) + " seconds.")




    print("\nDownloading blobs...")

    # Iterate thorugh the blobs in the container
    blob_list = container_client.list_blobs()
    for blob in blob_list:

        # Download the blob to a local file
        
        download_file_path = os.path.join("./" + container_name, str.replace(blob.name ,'.zip', 'DOWNLOAD.zip'))
        print("\nDownloading blob to \n\t" + download_file_path)
        # Create a blob client using the blob name
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob.name)
        start = time.time()
        with open(download_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        end = time.time()

        print("finished in " + str(end - start) + " seconds.")

    # # Clean up
    # print("\nPress the Enter key to begin clean up")
    # input()

    # print("Deleting blob container...")
    # container_client.delete_container()

    # print("Deleting the local source and downloaded files...")
    # os.remove(upload_file_path)
    # os.remove(download_file_path)
    # os.rmdir(local_path)

    print("Done")

except Exception as ex:
    print('Exception:')
    print(ex)
    

# potential ways to optimize: find bottle neck: disk, network, CPU
# see if the download fully utilizes network 