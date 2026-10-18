from google.cloud import storage

def get_line_count_from_gcs(bucket_name, file_name):
    # Create a cloud storage client
    gcs_client = storage.Client()

    #Access the bucket
    bucket_obj = gcs_client.get_bucket(bucket_name)

    # retrive the specific file (blob)
    file_blob = bucket_obj.get_blob(file_name)

    #download the file content as bytes
    file_data = file_blob.download_as_string()

    #calculate newline by newline character
    total_lines = file_data.count(b'\n')+1

    return total_lines

def main():
    #Setting up buket and file name
    bucket_name = 'ibd_bucket-as1'
    file_name = 'input.txt'

    total_lines_count = get_line_count_from_gcs(bucket_name,file_name)

    print(f'The file "{file_name}" in bucket "{bucket_name}" has "{total_lines_count}" lines.')
    print("Task Completed")

if __name__ == "__main__" :
     main()
        
