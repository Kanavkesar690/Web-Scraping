import os
import requests
import logging
import tempfile
import azure.functions as func
from bs4 import BeautifulSoup
from textwrap import wrap
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from azure.storage.blob import BlobServiceClient
from azure.identity import ManagedIdentityCredential
from dotenv import load_dotenv
 
# Load environment variables

logging.basicConfig(level=logging.INFO)
load_dotenv()
 
# Configurations
storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
container_name = os.getenv("CONTAINERNAME")
font_path = "DejaVuSans.ttf"
pdfmetrics.registerFont(TTFont('DejaVuSans', font_path))
 
def scrape_page(url, file_name, container_name) -> str:
    if not file_name.lower().endswith('.pdf'):
        file_name += '.pdf'
 
    def scrape_data(url):
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"}
        response = requests.get(url, headers=headers)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        scraped_text = []

        for element in soup.find_all(['h1', 'h2', 'h3', 'p', 'a']):
            if element.name == 'a' and element.has_attr('href'):
                href = element['href']
                if href.startswith('https'):
                    text = f"{element.get_text().strip()} ({href})"
                    scraped_text.append(text)

            else:
                text = element.get_text().strip()
                if text:
                    scraped_text.append(' '.join(text.split()))
        return scraped_text
 
    def save_to_pdf(data, filepath):
        c = canvas.Canvas(filepath, pagesize=letter)
        c.setFont("DejaVuSans", 12)
        width, height = letter
        y_position = height - 40
        margin = 40
        line_height = 14
 
        for line in data:
            for part in wrap_text(line, width - margin * 2, c):
                c.drawString(margin, y_position, part)
                y_position -= line_height
                if y_position < margin:
                    c.showPage()
                    c.setFont("DejaVuSans", 12)
                    y_position = height - 40
        c.save()
        return "Success"
 
    def wrap_text(text, max_width, canvas):
        words = text.split(' ')
        lines = []
        current_line = ""
        for word in words:
            test_line = f"{current_line} {word}".strip()
            width = canvas.stringWidth(test_line, "DejaVuSans", 12)
            if width <= max_width:
                current_line = test_line

            else:
                lines.append(current_line)
                current_line = word
 
        if current_line:
            lines.append(current_line)
        return lines
    scraped_data = scrape_data(url)

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
        temp_filepath = temp_file.name
        save_to_pdf(scraped_data, temp_filepath)
    upload_to_blob(temp_filepath, file_name, container_name)
    return f"{file_name} uploaded to Azure Blob Storage and indexed successfully."
 
def upload_to_blob(filepath, file_name, container_name):

    # ManagedIdentityCredential 
    creds = ManagedIdentityCredential(client_id=os.getenv("CLIENT_ID"))
    blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net/", credential=creds)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob=file_name)

    with open(filepath, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
        logging.info(f"Uploaded {file_name} to Azure Blob Storage container '{container_name}'.")
    os.remove(filepath)
    return
 
 
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    url = req.params.get('url')
    file_name = req.params.get('filename')
    container_name = req.params.get('containername')
    if not file_name.endswith('.pdf'):
        file_name += '.pdf'

    if not url:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            url = req_body.get('url')

    if url:
        return func.HttpResponse(scrape_page(url, file_name, container_name))
    else:
        return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a URL in the query string or request body.",
            status_code=200
        )