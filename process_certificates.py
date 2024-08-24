import os
import anthropic
import json
import time
import io
import base64
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize the Anthropic client
client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# Google Drive and Sheets settings
CREDENTIALS_JSON = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
SCOPES = ['https://www.googleapis.com/auth/drive.readonly', 'https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
FOLDER_ID = os.getenv('FOLDER_ID')

def get_google_services():
    try:
        creds = service_account.Credentials.from_service_account_info(
            json.loads(CREDENTIALS_JSON),
            scopes=SCOPES
        )
        drive_service = build('drive', 'v3', credentials=creds)
        sheet_service = build('sheets', 'v4', credentials=creds)
        logging.info("Google services initialized successfully.")
        return drive_service, sheet_service
    except Exception as e:
        logging.error(f"Error initializing Google services: {e}")
        raise

def analyze_certificate(image_data):
    try:
        message = client.messages.create(
            model="claude-3-sonnet-20240229",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": """You are an AI assistant tasked with analyzing and extracting information from certificate images to help build a comprehensive resume/CV warehouse. Your goal is to accurately transcribe, describe, and categorize the information from each certificate provided.

When presented with a certificate image, follow these steps:

1. Analyze the image using your vision capabilities.

2. Extract and present the following information in this JSON format:

{
  "transcription": "Full transcription and description of the certificate",
  "company_name": "Company name",
  "position_held": "Position held",
  "duration": "Month/Year of Start - Month/Year of Finish",
  "location": "City, State",
  "section": "Appropriate section from the list provided"
}

3. To determine the appropriate section, refer to this list and choose the most suitable option:
- Social and Professional Affiliations
- Volunteer and Community/Campus Involvement
- Leadership Positions
- Class or Design Projects
- Research Experiences
- Practicum or Internship Experiences
- Teaching Experiences
- Study/Travel Abroad
- Honors, Awards, and Scholarships
- Certifications or Licensure
- Languages
- Computer/Technical Skills
- Laboratory Skills/Field Processes
- Presentations and Publications
- Special Interests or Hobbies
- Notable Achievements

4. If any information is unclear or missing from the certificate, use "Not specified" as the value.

5. If you're unsure about any aspect of the certificate or its categorization, provide your best estimate based on the available information.

Respond ONLY with the JSON object. Do not include any other text in your response.

Here is the certificate image to analyze:"""
                        },
                        {
                            "type": "image",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{base64.b64encode(image_data).decode('utf-8')}"
                            }
                        }
                    ]
                }
            ]
        )
        return json.loads(message.content[0].text)
    except Exception as e:
        logging.error(f"Error analyzing certificate: {e}")
        raise

def append_to_sheet(sheet_service, spreadsheet_id, data):
    try:
        values = [[
            data.get('company_name', 'Not specified'),
            data.get('position_held', 'Not specified'),
            data.get('duration', 'Not specified'),
            data.get('location', 'Not specified'),
            data.get('section', 'Not specified'),
            data.get('transcription', 'Not specified')
        ]]
        body = {'values': values}
        sheet_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range='Sheet1',  # Adjust this if your sheet has a different name
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()
        logging.info("Data appended to Google Sheet successfully.")
    except Exception as e:
        logging.error(f"Error appending data to sheet: {e}")
        raise

def process_certificate(drive_service, sheet_service, spreadsheet_id, file):
    try:
        request = drive_service.files().get_media(fileId=file['id'])
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()

        result = analyze_certificate(fh.getvalue())
        append_to_sheet(sheet_service, spreadsheet_id, result)

        drive_service.files().delete(fileId=file['id']).execute()
        logging.info(f"Processed and deleted certificate: {file['name']}")

        return True
    except Exception as e:
        logging.error(f"Error processing certificate {file['name']}: {e}")
        return False

@functions_framework.http
def process_certificates(request):
    try:
        drive_service, sheet_service = get_google_services()
        results = drive_service.files().list(
            q=f"'{FOLDER_ID}' in parents and (mimeType='image/jpeg' or mimeType='image/png')",
            fields="files(id, name, mimeType)"
        ).execute()
        items = results.get('files', [])

        processed_count = 0
        for item in items:
            if process_certificate(drive_service, sheet_service, SPREADSHEET_ID, item):
                processed_count += 1

        logging.info(f"Processed {processed_count} certificates.")
        return f"Processed {processed_count} certificates", 200
    except Exception as e:
        logging.error(f"Error processing certificates: {e}")
        return str(e), 500

def main():
    try:
        drive_service, sheet_service = get_google_services()
        results = drive_service.files().list(
            q=f"'{FOLDER_ID}' in parents and (mimeType='image/jpeg' or mimeType='image/png')",
            fields="files(id, name, mimeType)"
        ).execute()
        items = results.get('files', [])

        processed_count = 0
        for item in items:
            if process_certificate(drive_service, sheet_service, SPREADSHEET_ID, item):
                processed_count += 1

        logging.info(f"Processed {processed_count} certificates.")
        print(f"Processed {processed_count} certificates")
    except Exception as e:
        logging.error(f"Error processing certificates: {e}")
        print(f"Error processing certificates: {e}")

if __name__ == "__main__":
    main()
