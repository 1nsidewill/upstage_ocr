from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
import os
import requests
import asyncio
import aiofiles
from typing import List
import time
from PyPDF2 import PdfReader, PdfWriter
from bs4 import BeautifulSoup
import re

app = FastAPI()

# Define Pydantic model for document parse request
class DocumentRequest(BaseModel):
    file_path: str  # Path to the input file

class DocumentResponse(BaseModel):
    file_path: str  # Path to the output file
    status: str  # Status of the parse operation

# Redirect root endpoint to /docs
@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/docs")

# Endpoint to convert HTML files to TXT and group by prefix
@app.post("/convert_html_to_txt")
async def convert_html_to_txt():
    input_folder = "output_data"
    output_folder = "output_text_data"
    
    # Create output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Group files by prefix (e.g., "06-공과대학")
    file_groups = {}
    files = [f for f in os.listdir(input_folder) if f.endswith(".html")]

    for file_name in files:
        prefix = "-".join(file_name.split('-')[:2])  # Extract common prefix like "06-공과대학"
        if prefix not in file_groups:
            file_groups[prefix] = []
        file_groups[prefix].append(file_name)

    # Process each group and convert to a single text file
    for prefix, group_files in file_groups.items():
        # Sort the files in ascending order based on the numeric part of the filename
        def extract_numeric_part(file_name):
            match = re.search(r'(\d+)', file_name.split('-', 2)[-1])
            return int(match.group()) if match else float('inf')  # Return a very large number if no numeric part is found

        group_files.sort(key=extract_numeric_part)

        # Determine output file name without the `.pdf_parsed.html` suffix
        base_prefix = prefix if len(group_files) > 1 else group_files[0].split(".pdf_parsed")[0]

        txt_output_path = os.path.join(output_folder, f"{base_prefix}.txt")
        
        async with aiofiles.open(txt_output_path, 'w') as txt_file:
            for index, file_name in enumerate(group_files):
                input_path = os.path.join(input_folder, file_name)

                # Extract text content from HTML
                async with aiofiles.open(input_path, 'r') as html_file:
                    html_content = await html_file.read()

                # Use BeautifulSoup to parse the HTML
                soup = BeautifulSoup(html_content, "html.parser")

                # Extract 'alt' text from <img> tags and remove the <img> tags
                for img_tag in soup.find_all('img'):
                    if img_tag.has_attr('alt'):
                        alt_text = img_tag['alt']
                        # Insert the alt text directly in place of the img tag
                        img_tag.insert_before(alt_text)
                    img_tag.decompose()  # Remove the img tag completely

                # Extract all the text content from the cleaned HTML
                text_content = soup.get_text(strip=True)

                # Write cleaned text to the TXT file with consistent headers and spacing
                if index > 0:
                    # Add 10 lines of spacing before each new section, except the first
                    await txt_file.write('\n' * 10)

                # Write the header derived from the filename
                suffix = file_name.split('-', 2)[-1].split(".pdf_parsed")[0]
                await txt_file.write(f"######{suffix}######\n")

                # Write the cleaned text content
                await txt_file.write(text_content)

    return {"status": "Conversion completed", "output_directory": output_folder}

# Endpoint to initiate document parsing asynchronously
@app.post("/parse_documents")
async def parse_documents(background_tasks: BackgroundTasks):
    input_folder = "input_data"
    output_folder = "output_data"

    # List all files in the input folder
    files = [f for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]
    
    # Iterate over each file and add to the background task for parsing
    for file_name in files:
        input_path = os.path.join(input_folder, file_name)
        output_path = os.path.join(output_folder, f"{file_name}_parsed")
        
        if os.path.getsize(input_path) > 50 * 1024 * 1024:  # If file size > 50MB, split it
            split_files = split_pdf(input_path)
            for split_file in split_files:
                split_output_path = f"{split_file}_parsed"
                background_tasks.add_task(call_upstage_api, split_file, split_output_path)
        else:
            background_tasks.add_task(call_upstage_api, input_path, output_path)

    return {"status": "Processing initiated"}

# Function to split a large PDF into smaller chunks
def split_pdf(input_file: str) -> List[str]:
    split_files = []
    reader = PdfReader(input_file)
    total_pages = len(reader.pages)

    output_dir = "split_data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Split into chunks of 10 pages or smaller if the file is large
    pages_per_chunk = 10
    for start_page in range(0, total_pages, pages_per_chunk):
        writer = PdfWriter()
        end_page = min(start_page + pages_per_chunk, total_pages)
        
        for page_num in range(start_page, end_page):
            writer.add_page(reader.pages[page_num])

        split_file_path = os.path.join(output_dir, f"{os.path.basename(input_file)}_part_{start_page//pages_per_chunk + 1}.pdf")
        with open(split_file_path, "wb") as output_pdf:
            writer.write(output_pdf)
        
        split_files.append(split_file_path)

    return split_files

# Asynchronous function to call the Upstage API
async def call_upstage_api(input_file: str, output_file: str):
    UPSTAGE_API_URL = os.getenv("UPSTAGE_API_URL")
    UPSTAGE_API_KEY = os.getenv("UPSTAGE_API_KEY")  # Get API key from environment variables
    
    headers = {
        "Authorization": f"Bearer {UPSTAGE_API_KEY}",
    }

    files = {"document": open(input_file, "rb")}
    data = {"ocr": "force"
            ,"coordinates": False
            ,"output_formats": "['html']"
            ,"model" : "document-parse"}

    # Make the request to Upstage API
    response = requests.post(UPSTAGE_API_URL, headers=headers, files=files, data=data)

    # Handle the response
    if response.status_code == 202:
        result_data = response.json()
        request_id = result_data["request_id"]
        print(f"Request ID: {request_id}")

        # Now poll for the result using the request_id
        await poll_for_result(request_id, output_file)
    else:
        print(f"Error processing file {input_file}: {response.text}")

async def poll_for_result(request_id: str, output_file: str):
    UPSTAGE_RESULT_URL = f"https://api.upstage.ai/v1/document-ai/requests/{request_id}"
    UPSTAGE_API_KEY = os.getenv("UPSTAGE_API_KEY")  # Get API key from environment variables
    
    headers = {
        "Authorization": f"Bearer {UPSTAGE_API_KEY}",
    }

    while True:
        response = requests.get(UPSTAGE_RESULT_URL, headers=headers)
        
        if response.status_code == 200:
            result_data = response.json()

            # Check the status of the request
            status = result_data.get("status")
            print(f"Current status: {status}")
            
            if status == "completed":
                print("Processing completed, downloading result...")
                # Download the result using the download URL
                await download_inference_result(result_data["batches"][0]["download_url"], output_file)
                break
            elif status == "failed":
                print(f"Processing failed: {result_data.get('failure_message')}")
                break
            else:
                # Wait for a few seconds before checking the status again
                time.sleep(5)
        else:
            print(f"Error fetching result for request {request_id}: {response.text}")
            break
        
async def download_inference_result(download_url: str, output_file: str):
    response = requests.get(download_url)

    if response.status_code == 200:
        result_data = response.json()

        # Extracting HTML content only once
        clean_html_parts = []

        # Extract html from 'content' key and add it only if not already present
        if 'content' in result_data and 'html' in result_data['content']:
            html_content = result_data['content']['html']
            if html_content not in clean_html_parts:
                clean_html_parts.append(html_content)

        # Join all HTML parts into a single string
        clean_html = "\n".join(clean_html_parts)

        # Ensure the output file has an .html extension
        if not output_file.endswith(".html"):
            output_file += ".html"

        # Save the cleaned HTML content to the output file
        async with aiofiles.open(output_file, 'w') as output:
            await output.write(clean_html)  # Save the cleaned HTML
        print(f"Result saved to {output_file}")

    else:
        print(f"Error downloading result: {response.text}")
        
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
