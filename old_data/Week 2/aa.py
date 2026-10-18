import requests

def count_lines_in_https_file(https_url):
    # Download the file content from HTTPS URL
    response = requests.get(https_url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    
    # Get the content as text
    content = response.text
    
    # Count the lines
    line_count = len(content.splitlines())
    
    return line_count

# Full HTTPS path
https_path = 'https://storage.googleapis.com/test_iitm_bucket_gokul_gcp/Tesla.csv'

line_count = count_lines_in_https_file(https_path)
print(f"Number of lines in {https_path}: {line_count}")
