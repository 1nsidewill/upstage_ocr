from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
import os
import requests
import asyncio
import aiofiles
from typing import List
import time

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
        background_tasks.add_task(call_upstage_api, input_path, output_path)

    return {"status": "Processing initiated"}

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
