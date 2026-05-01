import argparse
import base64
import json
import logging
import os
import pickle
import re
import sys
import time
from typing import Dict, List, Optional, Any
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from email import policy
from email.parser import BytesParser
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
from collections import defaultdict
import html2text
from datetime import datetime, timedelta
import email.utils
import pytz
try:
    from orgparse import load as org_load
except ImportError:
    org_load = None

# Set up logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Load config
def load_config() -> Dict[str, Any]:
    """Load configuration from config.json."""
    config_path = 'config.json'
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            logging.warning("Invalid config.json, using defaults")
    return {}

config = load_config()
API_TIMEOUT = config.get('api_timeout', 120)
CACHE_PATH = config.get('cache_path', '.label_cache.json')
MAX_RETRIES = config.get('max_retries', 5)
ATTACHMENT_DIR = config.get('attachment_dir', 'attachments')

# This scope allows reading, composing, sending, and permanently deleting email.
# We need it to move messages to trash.
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']
MAX_RETRIES = 5
BASE_DELAY = 2  # Seconds
API_TIMEOUT = 120  # Seconds, increased for large mailboxes

def get_gmail_service(credentials_path: str = 'credentials.json') -> Any:
    """Authenticates with the Gmail API and returns a service object.

    Args:
        credentials_path: Path to the credentials JSON file.

    Returns:
        Gmail API service object.
    """
    creds = None
    creds_stem = os.path.splitext(os.path.basename(credentials_path))[0]
    token_path = os.path.join(os.path.dirname(credentials_path), f'{creds_stem}-token.json')
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)
    service = build('gmail', 'v1', credentials=creds)
    service._http.timeout = API_TIMEOUT  # Set timeout for API calls
    return service

def get_label_id_map(service: Any) -> Dict[str, str]:
    """Returns a dictionary mapping label names to label IDs, with caching.

    Args:
        service: Gmail API service object.

    Returns:
        Dict mapping label names to IDs.
    """
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, 'r') as f:
                cached = json.load(f)
            # Optionally, check if cache is recent, but for now, use it
            return cached
        except (json.JSONDecodeError, KeyError):
            pass  # Fall back to API

    results = service.users().labels().list(userId='me').execute()
    labels = results.get('labels', [])
    label_map = {label['name']: label['id'] for label in labels}

    # Cache the result
    try:
        with open(CACHE_PATH, 'w') as f:
            json.dump(label_map, f)
    except Exception:
        pass  # Ignore cache write errors

    return label_map

def modify_thread_labels(service, thread_id, old_label_name, new_label_name):
    """Moves a thread from an old label to a new one."""
    try:
        label_map = get_label_id_map(service)
        old_label_id = label_map.get(old_label_name)
        new_label_id = label_map.get(new_label_name)

        if not old_label_id:
            print(f"Error: Old label '{old_label_name}' not found in Gmail.", file=sys.stderr)
            sys.exit(1)
        if not new_label_id:
            print(f"Error: New label '{new_label_name}' not found in Gmail. Please create it first.", file=sys.stderr)
            sys.exit(1)
            
        body = {
            'addLabelIds': [new_label_id],
            'removeLabelIds': [old_label_id]
        }
        service.users().threads().modify(userId='me', id=thread_id, body=body).execute()
        print(f"Thread {thread_id} moved from '{old_label_name}' to '{new_label_name}'.", file=sys.stderr)
    except HttpError as error:
        print(f'An error occurred while modifying labels: {error}', file=sys.stderr)
        sys.exit(1)

def bulk_move_labels(service, old_label_name, new_label_name):
    """Moves all threads from an old label to a new one, creating the new label if it doesn't exist."""
    print(f"Starting bulk move from '{old_label_name}' to '{new_label_name}'.", file=sys.stderr)
    
    label_map = get_label_id_map(service)
    old_label_id = label_map.get(old_label_name)
    new_label_id = label_map.get(new_label_name)

    if not old_label_id:
        print(f"Error: Old label '{old_label_name}' not found in Gmail.", file=sys.stderr)
        sys.exit(1)
    
    if not new_label_id:
        print(f"New label '{new_label_name}' not found. Creating it now...", file=sys.stderr)
        create_label(service, new_label_name)
        # Refresh the label map to get the new ID
        label_map = get_label_id_map(service)
        new_label_id = label_map.get(new_label_name)
        if not new_label_id:
            print(f"Error: Failed to create or find new label '{new_label_name}' after creation attempt.", file=sys.stderr)
            sys.exit(1)

    query = f'label:"{old_label_name}"'
    threads_to_move_stubs = list_messages(service, query=query)
    
    if not threads_to_move_stubs:
        print(f"No threads found with label '{old_label_name}'.", file=sys.stderr)
        return

    # The list_messages function for labels returns message stubs, not thread stubs.
    # We need to get unique thread IDs.
    thread_ids_to_move = sorted(list({stub['threadId'] for stub in threads_to_move_stubs}))

    print(f"Found {len(thread_ids_to_move)} threads to move.", file=sys.stderr)
    
    for i, thread_id in enumerate(thread_ids_to_move):
        print(f"Moving thread {i+1}/{len(thread_ids_to_move)} (ID: {thread_id})...", file=sys.stderr)
        modify_thread_labels(service, thread_id, old_label_name, new_label_name)
        time.sleep(0.1) # Small delay to avoid hitting API rate limits
    
    print("Bulk move completed successfully.", file=sys.stderr)


def create_label(service, label_name):
    """Creates a new label in the user's account."""
    label_object = {
        'name': label_name,
        'labelListVisibility': 'labelShow',
        'messageListVisibility': 'show'
    }
    try:
        label = service.users().labels().create(userId='me', body=label_object).execute()
        print(f"Label '{label['name']}' created successfully.", file=sys.stderr)
    except HttpError as error:
        if error.resp.status == 409:
            print(f"Error: A label with this name ('{label_name}') already exists.", file=sys.stderr)
        else:
            print(f'An error occurred while creating the label: {error}', file=sys.stderr)
        sys.exit(1)

def delete_label(service, label_name):
    """Deletes a label from the user's account."""
    try:
        label_map = get_label_id_map(service)
        label_id = label_map.get(label_name)
        if not label_id:
            print(f"Error: Label '{label_name}' not found.", file=sys.stderr)
            sys.exit(1)
        service.users().labels().delete(userId='me', id=label_id).execute()
        print(f"Label '{label_name}' deleted successfully.", file=sys.stderr)
    except HttpError as error:
        print(f'An error occurred while deleting the label: {error}', file=sys.stderr)
        sys.exit(1)

def list_labels(service):
    """Lists all user-created labels in the user's account."""
    try:
        results = service.users().labels().list(userId='me').execute()
        labels = results.get('labels', [])
        if not labels:
            print("No labels found.", file=sys.stderr)
        else:
            # We only want user-created labels.
            user_labels = [label['name'] for label in labels if label['type'] == 'user']
            print("---LABEL_LIST_START---")
            for label_name in sorted(user_labels):
                print(label_name)
            print("---LABEL_LIST_END---")
    except HttpError as error:
        print(f'An error occurred while listing labels: {error}', file=sys.stderr)
        sys.exit(1)

def delete_message(service, msg_id):
    """Moves a specific message to the trash."""
    try:
        service.users().messages().trash(userId='me', id=msg_id).execute()
        print(f"Message with ID: {msg_id} moved to trash.", file=sys.stderr)
    except HttpError as error:
        print(f'An error occurred while deleting message: {error}', file=sys.stderr)
        sys.exit(1)

def delete_thread(service, thread_id):
    """Moves an entire thread to the trash."""
    try:
        service.users().threads().trash(userId='me', id=thread_id).execute()
        print(f"Thread with ID: {thread_id} moved to trash.", file=sys.stderr)
    except HttpError as error:
        print(f'An error occurred while deleting thread: {error}', file=sys.stderr)
        sys.exit(1)

def parse_natural_language_time(time_str):
    """Parses a natural language time string and returns a datetime object."""
    now = datetime.now(pytz.utc)
    time_str = time_str.lower()

    # Simple cases
    if "tomorrow" in time_str:
        defer_until = now + timedelta(days=1)
        if "morning" in time_str or "9am" in time_str:
            return defer_until.replace(hour=9, minute=0, second=0, microsecond=0)
        elif "evening" in time_str or "7pm" in time_str:
            return defer_until.replace(hour=19, minute=0, second=0, microsecond=0)
        return defer_until.replace(hour=9, minute=0, second=0, microsecond=0)
    
    # "in X hours/days/weeks"
    match = re.match(r"in (\d+) (hour|day|week)s?", time_str)
    if match:
        num = int(match.group(1))
        unit = match.group(2)
        if unit == "hour":
            return now + timedelta(hours=num)
        elif unit == "day":
            return now + timedelta(days=num)
        elif unit == "week":
            return now + timedelta(weeks=num)

    # "next Monday", "next Friday at 5pm" etc.
    days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    for i, day_name in enumerate(days):
        if day_name in time_str:
            days_ahead = i - now.weekday()
            if days_ahead <= 0: # Target day has passed this week
                days_ahead += 7
            defer_until = now + timedelta(days=days_ahead)
            # Check for specific time
            time_match = re.search(r"(\d+)(am|pm)?", time_str)
            if time_match:
                hour = int(time_match.group(1))
                if time_match.group(2) == "pm" and hour < 12:
                    hour += 12
                return defer_until.replace(hour=hour, minute=0, second=0, microsecond=0)
            return defer_until.replace(hour=9, minute=0, second=0, microsecond=0)
    
    # Default fallback
    return (now + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)


def defer_message(service, msg_id, defer_time_str):
    """Snoozes a message until a specific time."""
    try:
        defer_until = parse_natural_language_time(defer_time_str)
        snooze_request = {
            'snoozeUntil': defer_until.isoformat()
        }
        service.users().messages().snooze(userId='me', id=msg_id, body=snooze_request).execute()
        print(f"Message {msg_id} snoozed until {defer_until.strftime('%Y-%m-%d %H:%M:%S %Z')}", file=sys.stderr)
    except HttpError as error:
        print(f'An error occurred while snoozing the message: {error}', file=sys.stderr)
        sys.exit(1)
    except AttributeError:
        print("AttributeError: 'Resource' object has no attribute 'snooze'.", file=sys.stderr)
        print("This likely means your google-api-python-client library is outdated.", file=sys.stderr)
        print("Please upgrade it with: pip install --upgrade google-api-python-client", file=sys.stderr)
        sys.exit(1)

def reply_to_message(service, msg_id, message_body, to_recipients, cc_recipients):
    """Sends a reply to a specific message."""
    try:
        original_message = service.users().messages().get(userId='me', id=msg_id, format='metadata', metadataHeaders=['subject', 'message-id', 'references']).execute()
        headers = {h['name'].lower(): h['value'] for h in original_message['payload']['headers']}
        
        reply = MIMEText(message_body)
        reply['to'] = to_recipients
        if cc_recipients:
            reply['cc'] = cc_recipients
        reply['subject'] = "Re: " + headers['subject']
        reply['In-Reply-To'] = headers['message-id']
        reply['References'] = headers.get('references', '') + ' ' + headers['message-id']

        raw_message = base64.urlsafe_b64encode(reply.as_bytes()).decode()
        body = {'raw': raw_message, 'threadId': original_message['threadId']}
        
        service.users().messages().send(userId='me', body=body).execute()
        print(f"Reply sent for message ID: {msg_id}", file=sys.stderr)
    except HttpError as error:
        print(f'An error occurred while sending the reply: {error}', file=sys.stderr)
        sys.exit(1)

def delegate_message(service, msg_id, recipient, note):
    """Forwards a message to a recipient with a note."""
    try:
        original_message_raw = service.users().messages().get(userId='me', id=msg_id, format='raw').execute()
        raw_data = base64.urlsafe_b64decode(original_message_raw['raw'])
        original_email = BytesParser(policy=policy.default).parsebytes(raw_data)

        forward = MIMEMultipart()
        forward['to'] = recipient
        forward['from'] = original_email['to'] # Assuming 'to' is your address
        forward['subject'] = "Fwd: " + original_email['subject']
        
        forward.attach(MIMEText(note + "\n\n" + "---------- Forwarded message ----------\n"))
        forward.attach(original_email)

        raw_message = base64.urlsafe_b64encode(forward.as_bytes()).decode()
        body = {'raw': raw_message, 'threadId': original_message_raw['threadId']}
        
        service.users().messages().send(userId='me', body=body).execute()
        print(f"Message {msg_id} delegated to {recipient}", file=sys.stderr)
    except HttpError as error:
        print(f'An error occurred while delegating the message: {error}', file=sys.stderr)
        sys.exit(1)


def normalize_label(label_name):
    """Normalizes a Gmail label name to be used in a query."""
    # Gmail's API uses hyphens for nested labels instead of slashes.
    normalized = label_name.replace('/', '-')
    print(f"Normalized label: {normalized}", file=sys.stderr)
    return normalized

def convert_markdown_to_org_links(text):
    """Converts Markdown links to Org-mode links."""
    markdown_link_pattern = r'\[([^\]\[\\]*)\]\(([^\)\(\\]*)\)'
    return re.sub(markdown_link_pattern, r'[[\2][\1]]', text)

def parse_org_for_email_ids(agenda_files):
    """Parses Org-mode files to extract existing EMAIL_IDs."""
    email_ids = defaultdict(list)
    for org_file in agenda_files.split(','):
        org_file = os.path.expanduser(org_file.strip())
        if org_file and os.path.exists(org_file):
            try:
                # Using regex as it's more reliable and doesn't have external dependencies
                with open(org_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                pattern = r':EMAIL_ID:\s*([a-zA-Z0-9]+)'
                matches = re.findall(pattern, content)
                for email_id in matches:
                    email_ids[email_id].append(org_file)
                print(f"Scanned {org_file} (regex): found {len(matches)} EMAIL_IDs", file=sys.stderr)
            except Exception as e:
                print(f"Error reading {org_file}: {e}", file=sys.stderr)
    total_ids = len(email_ids)
    print(f"Total unique email IDs across agenda files: {total_ids}", file=sys.stderr)
    return email_ids

def parse_org_for_labels_from_properties(agenda_files):
    """Parses Org-mode files to find all labels stored in properties."""
    labels = set()
    label_pattern = re.compile(r':LABEL:\s*(.*)$')
    for org_file in agenda_files.split(','):
        org_file = os.path.expanduser(org_file.strip())
        if org_file and os.path.exists(org_file):
            try:
                with open(org_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        match = label_pattern.search(line)
                        if match:
                            labels.add(match.group(1).strip())
            except Exception as e:
                print(f"Error reading {org_file} to find labels from properties: {e}", file=sys.stderr)
    return list(labels)

def parse_org_for_thread_locations(agenda_files):
    """Parses Org-mode files to find THREAD_ID locations."""
    thread_locations = {}
    thread_pattern = re.compile(r':THREAD_ID:\s*(.*)$')
    for org_file in agenda_files.split(','):
        org_file = os.path.expanduser(org_file.strip())
        if org_file and os.path.exists(org_file):
            try:
                with open(org_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        match = thread_pattern.search(line)
                        if match:
                            thread_id = match.group(1).strip()
                            thread_locations[thread_id] = org_file
            except Exception as e:
                print(f"Error reading {org_file} to find thread locations: {e}", file=sys.stderr)
    return thread_locations

def insert_under_thread(file_path, thread_id, content):
    """Insert content under the thread heading in the Org file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Error reading {file_path}: {e}", file=sys.stderr)
        return

    # Find the index of the :THREAD_ID: thread_id
    thread_index = None
    for i, line in enumerate(lines):
        if f':THREAD_ID: {thread_id}' in line:
            thread_index = i
            break

    if thread_index is None:
        # Not found, append
        try:
            with open(file_path, 'a', encoding='utf-8') as f:
                f.write(content)
        except Exception as e:
            print(f"Error appending to {file_path}: {e}", file=sys.stderr)
        return

    # Find the ** above
    heading_index = None
    for i in range(thread_index, -1, -1):
        if lines[i].strip().startswith('** '):
            heading_index = i
            break

    if heading_index is None:
        # Append
        try:
            with open(file_path, 'a', encoding='utf-8') as f:
                f.write(content)
        except Exception as e:
            print(f"Error appending to {file_path}: {e}", file=sys.stderr)
        return

    # Find the end of the subtree: next line with * not starting with ***
    end_index = len(lines)
    for i in range(heading_index + 1, len(lines)):
        line = lines[i].strip()
        if line.startswith('*') and not line.startswith('***'):
            end_index = i
            break

    # Insert content at end_index
    insert_content = content.strip() + '\n'
    lines.insert(end_index, insert_content)

    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
    except Exception as e:
        print(f"Error writing to {file_path}: {e}", file=sys.stderr)

def list_messages(service, query=None, thread_id=None):
    """Lists messages from Gmail API with retries and better feedback."""
    for attempt in range(MAX_RETRIES):
        try:
            if thread_id:
                print(f"Fetching thread {thread_id} (attempt {attempt + 1}/{MAX_RETRIES})", file=sys.stderr)
                thread = service.users().threads().get(userId='me', id=thread_id).execute()
                messages = thread.get('messages', [])
            else:
                print(f"Executing query: '{query}' (attempt {attempt + 1}/{MAX_RETRIES})", file=sys.stderr)
                messages = []
                response = service.users().messages().list(userId='me', q=query, includeSpamTrash=False).execute()
                if 'messages' in response:
                    messages.extend(response['messages'])
                page_count = 1
                while 'nextPageToken' in response:
                    page_token = response['nextPageToken']
                    print(f"Fetching next page of results (page {page_count + 1})...", file=sys.stderr)
                    response = service.users().messages().list(userId='me', q=query, pageToken=page_token, includeSpamTrash=False).execute()
                    if 'messages' in response:
                        messages.extend(response['messages'])
                    page_count += 1

            print(f"Found {len(messages)} messages for {'thread ' + thread_id if thread_id else 'query: ' + query}", file=sys.stderr)
            return messages
        except HttpError as error:
            print(f'An HTTP error {error.resp.status} occurred on attempt {attempt + 1}/{MAX_RETRIES}: {error}', file=sys.stderr)
            if attempt < MAX_RETRIES - 1:
                delay = BASE_DELAY * (2 ** attempt)
                print(f"Retrying in {delay} seconds...", file=sys.stderr)
                time.sleep(delay)
            else:
                print("Max retries reached. Aborting.", file=sys.stderr)
                raise
        except Exception as e:
            print(f"An unexpected error occurred: {e}", file=sys.stderr)
            break
    return []


def convert_to_org_timestamp(date_str):
    """Converts an email date string to an Org-mode timestamp."""
    try:
        parsed_date = email.utils.parsedate_to_datetime(date_str)
        if parsed_date is None:
            return "<Unknown Date>"
        if parsed_date.tzinfo is None:
            parsed_date = parsed_date.replace(tzinfo=pytz.UTC)
        return parsed_date.strftime("<%Y-%m-%d %a %H:%M>")
    except (TypeError, ValueError):
        print(f"Failed to parse date: {date_str}", file=sys.stderr)
        return "<Unknown Date>"

def split_quoted_content(body):
    """Splits the main content from the quoted content in an email body."""
    lines = body.split('\n')
    main_content = []
    quoted_content = []
    in_quoted = False
    for line in lines:
        if line.strip().startswith('>'):
            in_quoted = True
            quoted_content.append(line)
        elif in_quoted and not line.strip(): # Handles blank lines between quoted sections
             quoted_content.append(line)
        else:
            in_quoted = False
            main_content.append(line)

    main_content_str = '\n'.join(main_content).strip()
    quoted_content_str = '\n'.join(quoted_content).strip()
    print(f"Main content length: {len(main_content_str)}, Quoted content length: {len(quoted_content_str)}", file=sys.stderr)
    return main_content_str, quoted_content_str

def escape_org_headings(text):
    """Prepends a comma to lines starting with '*' to escape them in Org mode."""
    lines = text.split('\n')
    escaped_lines = [',' + line if line.strip().startswith('*') else line for line in lines]
    return '\n'.join(escaped_lines)

def get_message_details(service, msg_id, label_name=None):
    """Fetches and parses the details of a single email message."""
    for attempt in range(MAX_RETRIES):
        try:
            print(f"Attempting to download message {msg_id} (attempt {attempt + 1}/{MAX_RETRIES})", file=sys.stderr)
            message = service.users().messages().get(userId='me', id=msg_id, format='full').execute()
            raw_message = service.users().messages().get(userId='me', id=msg_id, format='raw').execute()
            raw_data = base64.urlsafe_b64decode(raw_message['raw'])

            email_msg = BytesParser(policy=policy.default).parsebytes(raw_data)
            subject = email_msg['subject'] or 'No Subject'
            from_addr = email_msg['from'] or 'Unknown Sender'
            to_addr = email_msg['to'] or 'Unknown Recipient'
            date = email_msg['date'] or 'Unknown Date'
            thread_id = message['threadId']

            org_timestamp = convert_to_org_timestamp(date)

            body = ''
            if email_msg.is_multipart():
                for part in email_msg.walk():
                    content_type = part.get_content_type()
                    content_disposition = str(part.get('Content-Disposition'))

                    if content_type == 'text/plain' and 'attachment' not in content_disposition:
                        body = part.get_payload(decode=True).decode('utf-8', errors='replace')
                        break
                    elif content_type == 'text/html' and 'attachment' not in content_disposition:
                        html_content = part.get_payload(decode=True).decode('utf-8', errors='replace')
                        body = html2text.html2text(html_content)
            else:
                if email_msg.get_content_type() == 'text/plain':
                    body = email_msg.get_payload(decode=True).decode('utf-8', errors='replace')
                elif email_msg.get_content_type() == 'text/html':
                    html_content = email_msg.get_payload(decode=True).decode('utf-8', errors='replace')
                    body = html2text.html2text(html_content)

            body = body.replace('\r\n', '\n').replace('\r', '')
            body = re.sub(r'\n\s*\n+', '\n\n', body.strip())
            body = convert_markdown_to_org_links(body)

            main_content, quoted_content = split_quoted_content(body)
            
            # Escape potential Org mode headings in the email body
            main_content = escape_org_headings(main_content)
            quoted_content = escape_org_headings(quoted_content)

            # Download attachments
            att_dir = ATTACHMENT_DIR.replace("{label}", label_name or "default")
            attachments = []
            for part in email_msg.walk():
                if part.get_content_maintype() == 'multipart':
                    continue
                content_disposition = str(part.get('Content-Disposition'))
                if 'attachment' in content_disposition:
                    filename = part.get_filename()
                    if filename:
                        os.makedirs(att_dir, exist_ok=True)
                        filepath = os.path.join(att_dir, filename)
                        try:
                            with open(filepath, 'wb') as f:
                                f.write(part.get_payload(decode=True))
                            attachments.append(filepath)
                        except Exception as e:
                            logging.error(f"Failed to save attachment {filename}: {e}")

            print(f"Processed message {msg_id}: Subject='{subject}', From='{from_addr}'", file=sys.stderr)
            return {
                'msg_id': msg_id,
                'thread_id': thread_id,
                'subject': subject,
                'from': from_addr,
                'to': to_addr,
                'date': org_timestamp,
                'main_content': main_content,
                'quoted_content': quoted_content,
                'attachments': attachments
            }
        except HttpError as error:
            print(f'Error downloading message {msg_id} (attempt {attempt + 1}/{MAX_RETRIES}): {error}', file=sys.stderr)
            if attempt < MAX_RETRIES - 1:
                time.sleep(BASE_DELAY * (2 ** attempt))
            else:
                raise
    return None

def _get_message_details_batch_chunk(service: Any, msg_ids: List[str], label_name: Optional[str] = None) -> List[Dict[str, str]]:
    """Fetches and parses details for a chunk of messages using a single batch request.

    Args:
        service: Gmail API service object.
        msg_ids: List of message IDs to fetch (caller must ensure len <= 45).
        label_name: Label name for attachment dir.

    Returns:
        List of dicts with message details.
    """
    if not msg_ids:
        return []

    batch = service.new_batch_http_request()
    full_results = {}
    raw_results = {}

    def full_callback(request_id, response, exception):
        if exception:
            logging.error(f"Full get error for {request_id}: {exception}")
        else:
            full_results[request_id] = response

    def raw_callback(request_id, response, exception):
        if exception:
            logging.error(f"Raw get error for {request_id}: {exception}")
        else:
            raw_results[request_id] = response

    for msg_id in msg_ids:
        batch.add(service.users().messages().get(userId='me', id=msg_id, format='full'), callback=full_callback, request_id=f"full_{msg_id}")
        batch.add(service.users().messages().get(userId='me', id=msg_id, format='raw'), callback=raw_callback, request_id=f"raw_{msg_id}")

    batch.execute()

    details = []
    for msg_id in msg_ids:
        full = full_results.get(f"full_{msg_id}")
        raw = raw_results.get(f"raw_{msg_id}")
        if full and raw:
            # Parse similar to get_message_details
            message = full
            raw_message = raw
            raw_data = base64.urlsafe_b64decode(raw_message['raw'])
            email_msg = BytesParser(policy=policy.default).parsebytes(raw_data)
            subject = email_msg['subject'] or 'No Subject'
            from_addr = email_msg['from'] or 'Unknown Sender'
            to_addr = email_msg['to'] or 'Unknown Recipient'
            date = email_msg['date'] or 'Unknown Date'
            thread_id = message['threadId']
            org_timestamp = convert_to_org_timestamp(date)

            body = ''
            if email_msg.is_multipart():
                for part in email_msg.walk():
                    content_type = part.get_content_type()
                    content_disposition = str(part.get('Content-Disposition'))
                    if content_type == 'text/plain' and 'attachment' not in content_disposition:
                        body = part.get_payload(decode=True).decode('utf-8', errors='replace')
                        break
                    elif content_type == 'text/html' and 'attachment' not in content_disposition:
                        html_content = part.get_payload(decode=True).decode('utf-8', errors='replace')
                        body = html2text.html2text(html_content)
            else:
                if email_msg.get_content_type() == 'text/plain':
                    body = email_msg.get_payload(decode=True).decode('utf-8', errors='replace')
                elif email_msg.get_content_type() == 'text/html':
                    html_content = email_msg.get_payload(decode=True).decode('utf-8', errors='replace')
                    body = html2text.html2text(html_content)

            body = body.replace('\r\n', '\n').replace('\r', '')
            body = re.sub(r'\n\s*\n+', '\n\n', body.strip())
            body = convert_markdown_to_org_links(body)

            main_content, quoted_content = split_quoted_content(body)
            main_content = escape_org_headings(main_content)
            quoted_content = escape_org_headings(quoted_content)

            # Download attachments
            att_dir = ATTACHMENT_DIR.replace("{label}", label_name or "default")
            attachments = []
            for part in email_msg.walk():
                if part.get_content_maintype() == 'multipart':
                    continue
                content_disposition = str(part.get('Content-Disposition'))
                if 'attachment' in content_disposition:
                    filename = part.get_filename()
                    if filename:
                        os.makedirs(att_dir, exist_ok=True)
                        filepath = os.path.join(att_dir, filename)
                        try:
                            with open(filepath, 'wb') as f:
                                f.write(part.get_payload(decode=True))
                            attachments.append(filepath)
                        except Exception as e:
                            logging.error(f"Failed to save attachment {filename}: {e}")

            details.append({
                'msg_id': msg_id,
                'thread_id': thread_id,
                'subject': subject,
                'from': from_addr,
                'to': to_addr,
                'date': org_timestamp,
                'main_content': main_content,
                'quoted_content': quoted_content,
                'attachments': attachments
            })
        else:
            logging.error(f"Failed to fetch details for message {msg_id}")

    return details

def get_message_details_batch(service: Any, msg_ids: List[str], label_name: Optional[str] = None) -> List[Dict[str, str]]:
    """Fetches and parses details for multiple messages using batch requests.
    Processes in chunks of 45 to stay under the 100-inner-request batch limit
    (each message requires 2 requests: format='full' and format='raw').

    Args:
        service: Gmail API service object.
        msg_ids: List of message IDs to fetch.
        label_name: Label name for attachment dir.

    Returns:
        List of dicts with message details.
    """
    if not msg_ids:
        return []

    CHUNK_SIZE = 45  # 45 msgs × 2 requests = 90, safely under limit of 100
    all_details = []
    for chunk_start in range(0, len(msg_ids), CHUNK_SIZE):
        chunk = msg_ids[chunk_start:chunk_start + CHUNK_SIZE]
        all_details.extend(_get_message_details_batch_chunk(service, chunk, label_name))
    return all_details

def sync_email_ids(agenda_files, consolidate=False, org_file=None):
    """Finds and optionally consolidates duplicate EMAIL_IDs."""
    email_ids = parse_org_for_email_ids(agenda_files)
    duplicates = [(email_id, files) for email_id, files in email_ids.items() if len(files) > 1]

    if duplicates:
        print("Duplicate EMAIL_IDs found:", file=sys.stderr)
        for email_id, files in duplicates:
            print(f"EMAIL_ID: {email_id} appears in: {', '.join(files)}", file=sys.stderr)
        if consolidate and org_file:
            print(f"Consolidating duplicates to {org_file}", file=sys.stderr)
            org_file = os.path.expanduser(org_file)
            os.makedirs(os.path.dirname(org_file), exist_ok=True)
            new_content = "* Consolidated Duplicate Emails\n"
            for email_id, files in duplicates:
                for file in files[1:]:  # Keep first occurrence, consolidate others
                    with open(file, 'r', encoding='utf-8') as f:
                        content = f.read()
                        pattern = rf'(\*+.*?:EMAIL_ID:\s*{re.escape(email_id)}\s*\n.*?)(?=\*+|\Z)'
                        match = re.search(pattern, content, re.DOTALL)
                        if match:
                            new_content += match.group(1) + "\n"
                    with open(file, 'r', encoding='utf-8') as f:
                        content = f.read()
                    content = re.sub(rf'\*+.*?:EMAIL_ID:\s*{re.escape(email_id)}\s*\n.*?(?=\*+|\Z)', '', content, flags=re.DOTALL)
                    with open(file, 'w', encoding='utf-8') as f:
                        f.write(content.strip() + "\n")
            with open(org_file, 'a', encoding='utf-8') as f:
                f.write(new_content)
            print(f"Consolidated {len(duplicates)} duplicate EMAIL_IDs to {org_file}", file=sys.stderr)
    else:
        print("No duplicate EMAIL_IDs found.", file=sys.stderr)
    print(f"Total unique EMAIL_IDs: {len(email_ids)}", file=sys.stderr)
    return email_ids

def download_and_append_label(service, label_name, org_file, date_drawer, agenda_files):
    """Downloads all new messages for a given label and appends them to the org file."""
    start_time = time.time()
    print(f"\n--- Syncing Label: {label_name} ---", file=sys.stderr)
    
    query = f'label:"{label_name}"'
    messages_with_label = list_messages(service, query=query)

    if not messages_with_label:
        print(f"No messages found with the given label.", file=sys.stderr)
        return

    thread_ids = {msg['threadId'] for msg in messages_with_label}
    print(f"DEBUG: Found {len(thread_ids)} threads associated with the label.", file=sys.stderr)

    all_messages_in_threads = []
    processed_message_ids = set()

    for tid in thread_ids:
        messages_in_thread = list_messages(service, thread_id=tid)
        for msg in messages_in_thread:
            if msg['id'] not in processed_message_ids:
                all_messages_in_threads.append(msg)
                processed_message_ids.add(msg['id'])
    
    messages = all_messages_in_threads
    print(f"DEBUG: Fetched a total of {len(messages)} unique messages from all associated threads.", file=sys.stderr)

    if not messages:
        print(f"No messages found for the given criteria.", file=sys.stderr)
        return

    print("Parsing existing email IDs from agenda files...", file=sys.stderr)
    parse_start = time.time()
    existing_email_ids = parse_org_for_email_ids(agenda_files)
    parse_end = time.time()
    print(f"DEBUG: Org parsing took {parse_end - parse_start:.2f} seconds", file=sys.stderr)
    
    print(f"DEBUG: Found {len(messages)} messages via API before filtering.", file=sys.stderr)
    print(f"DEBUG: Found {len(existing_email_ids)} existing messages in Org files.", file=sys.stderr)

    new_messages = [msg for msg in messages if msg['id'] not in existing_email_ids]
    print(f"DEBUG: Found {len(new_messages)} new messages after filtering.", file=sys.stderr)

    if not new_messages:
        print(f"All messages for label '{label_name}' are already downloaded.", file=sys.stderr)
        return

    print(f"Found {len(new_messages)} new messages to download.", file=sys.stderr)
    threads = defaultdict(list)
    msg_ids = [msg['id'] for msg in new_messages]
    print(f"--- Batch fetching {len(msg_ids)} messages ---", file=sys.stderr)
    msg_details_list = get_message_details_batch(service, msg_ids)
    for msg_details in msg_details_list:
        threads[msg_details['thread_id']].append(msg_details)

    # Get thread locations
    thread_locations = parse_org_for_thread_locations(agenda_files)

    # Collect content per file
    file_contents = defaultdict(str)

    for thread_id_key, messages_in_thread in threads.items():
        messages_in_thread.sort(key=lambda x: x['date'])
        first_msg = messages_in_thread[0]
        subject = first_msg['subject']

        # Determine file for this thread
        target_file = thread_locations.get(thread_id_key, org_file)

        if target_file == org_file:
            # New thread, include heading
            content = f"\n** [[https://mail.google.com/mail/u/0/#inbox/{thread_id_key}][{subject}]]\n"
        else:
            # Existing thread, no heading
            content = ""

        for msg in messages_in_thread:
            is_reply = msg != first_msg
            if is_reply or target_file != org_file:
                content += f"*** Re: {msg['subject']}\n"
            
            content += f":PROPERTIES:\n"
            content += f":EMAIL_ID: {msg['msg_id']}\n"
            content += f":THREAD_ID: {msg['thread_id']}\n"
            content += f":LABEL: {label_name}\n"
            content += f":FROM: {msg['from']}\n"
            content += f":TO: {msg['to']}\n"
            content += f":SUBJECT: {msg['subject']}\n"
            content += f":END:\n:{date_drawer}:\n"
            content += f"{msg['date']}\n"
            content += f":END:\n\n"
            content += f"{msg['main_content']}\n\n"
            if msg['quoted_content']:
                content += f"**** Quoted Content\n:QUOTED:\n{msg['quoted_content']}\n:END:\n\n"
            if msg.get('attachments'):
                content += "** Attachments\n"
                for att in msg['attachments']:
                    rel_path = os.path.relpath(att, os.path.dirname(target_file))
                    content += f"- [[file:{rel_path}][{os.path.basename(att)}]] ([[https://mail.google.com/mail/u/0/#inbox/{msg['msg_id']}][view in Gmail]])\n"
                content += "\n"

        file_contents[target_file] += content

    for thread_id_key, messages_in_thread in threads.items():
        target_file = thread_locations.get(thread_id_key, org_file)
        file_path_expanded = os.path.expanduser(target_file)
        os.makedirs(os.path.dirname(file_path_expanded), exist_ok=True)

        messages_in_thread.sort(key=lambda x: x['date'])
        first_msg = messages_in_thread[0]
        subject = first_msg['subject']

        if target_file == org_file:
            # New thread, append with heading
            content = f"\n** [[https://mail.google.com/mail/u/0/#inbox/{thread_id_key}][{subject}]]\n"
        else:
            # Existing thread, insert without heading
            content = ""

        for msg in messages_in_thread:
            is_reply = msg != first_msg
            if is_reply or target_file != org_file:
                content += f"*** Re: {msg['subject']}\n"
            
            content += f":PROPERTIES:\n"
            content += f":EMAIL_ID: {msg['msg_id']}\n"
            content += f":THREAD_ID: {msg['thread_id']}\n"
            content += f":LABEL: {label_name}\n"
            content += f":FROM: {msg['from']}\n"
            content += f":TO: {msg['to']}\n"
            content += f":SUBJECT: {msg['subject']}\n"
            content += f":END:\n:{date_drawer}:\n"
            content += f"{msg['date']}\n"
            content += f":END:\n\n"
            content += f"{msg['main_content']}\n\n"
            if msg['quoted_content']:
                content += f"**** Quoted Content\n:QUOTED:\n{msg['quoted_content']}\n:END:\n\n"
            if msg.get('attachments'):
                content += "** Attachments\n"
                for att in msg['attachments']:
                    rel_path = os.path.relpath(att, os.path.dirname(target_file))
                    content += f"- [[file:{rel_path}][{os.path.basename(att)}]] ([[https://mail.google.com/mail/u/0/#inbox/{msg['msg_id']}][view in Gmail]])\n"
                content += "\n"

        if target_file == org_file:
            print(f"Writing to {file_path_expanded}", file=sys.stderr)
            with open(file_path_expanded, 'a', encoding='utf-8') as f:
                f.write(f"* Emails for Label: {label_name}\n" + content)
            print(f"Successfully appended messages to {file_path_expanded}", file=sys.stderr)
        else:
            print(f"Inserting into {file_path_expanded}", file=sys.stderr)
            insert_under_thread(file_path_expanded, thread_id_key, content)
            print(f"Successfully inserted messages into {file_path_expanded}", file=sys.stderr)
    
    end_time = time.time()
    print(f"DEBUG: Total time for download_and_append_label: {end_time - start_time:.2f} seconds", file=sys.stderr)

def handle_defer(service, msg_id, defer_time):
    defer_message(service, msg_id, defer_time)

def handle_reply(service, msg_id, message_body, to_recipients, cc_recipients):
    reply_to_message(service, msg_id, message_body, to_recipients, cc_recipients)

def handle_delegate(service, msg_id, recipient, note):
    delegate_message(service, msg_id, recipient, note)

def handle_bulk_move_labels(service, old_label, new_label):
    bulk_move_labels(service, old_label, new_label)

def handle_modify_thread_labels(service, thread_id, old_label, new_label):
    modify_thread_labels(service, thread_id, old_label, new_label)

def handle_sync_labels(service, agenda_files, org_file, date_drawer, ignore_labels):
    # Get all user-created labels from Gmail
    try:
        results = service.users().labels().list(userId='me').execute()
        all_labels = results.get('labels', [])
        # Only get user-created labels
        labels_to_sync = [label['name'] for label in all_labels if label['type'] == 'user']
        print(f"Found {len(labels_to_sync)} user labels in Gmail: {labels_to_sync}", file=sys.stderr)
    except HttpError as error:
        print(f'An error occurred while fetching labels: {error}', file=sys.stderr)
        return

    if ignore_labels:
        filtered_labels = []
        ignored_count = 0
        for label in labels_to_sync:
            if not any(re.search(pattern, label) for pattern in ignore_labels):
                filtered_labels.append(label)
            else:
                ignored_count += 1
        print(f"Ignoring {ignored_count} labels matching ignore patterns. Syncing {len(filtered_labels)} labels.", file=sys.stderr)
        labels_to_sync = filtered_labels

    for label in labels_to_sync:
        download_and_append_label(service, label, org_file, date_drawer, agenda_files)

def handle_sync_email_ids(agenda_files, consolidate, org_file):
    sync_email_ids(agenda_files, consolidate, org_file)

def handle_delete_message(service, delete_message_id):
    delete_message(service, delete_message_id)

def handle_delete_thread(service, delete_thread_id):
    delete_thread(service, delete_thread_id)

def handle_download_by_label(service, label_name, org_file, date_drawer, agenda_files):
    download_and_append_label(service, label_name, org_file, date_drawer, agenda_files)

def handle_download_thread(service, thread_id, org_file, date_drawer, agenda_files):
    messages = list_messages(service, thread_id=thread_id)
    if not messages:
        print(f"No messages found for thread.", file=sys.stderr)
        return

    existing_email_ids = parse_org_for_email_ids(agenda_files)
    new_messages = [msg for msg in messages if msg['id'] not in existing_email_ids]

    if not new_messages:
        print(f"All messages for thread are already downloaded.", file=sys.stderr)
        return

    threads = defaultdict(list)
    for msg in new_messages:
        msg_details = get_message_details(service, msg['id'])
        if msg_details:
            threads[msg_details['thread_id']].append(msg_details)

    new_content = ""
    for _, messages_in_thread in threads.items():
        messages_in_thread.sort(key=lambda x: x['date'])
        for msg in messages_in_thread:
            new_content += f"*** Re: {msg['subject']}\n"
            new_content += f":PROPERTIES:\n"
            new_content += f":EMAIL_ID: {msg['msg_id']}\n"
            new_content += f":THREAD_ID: {msg['thread_id']}\n"
            new_content += f":FROM: {msg['from']}\n"
            new_content += f":TO: {msg['to']}\n"
            new_content += f":SUBJECT: {msg['subject']}\n"
            new_content += f":END:\n:{date_drawer}:\n"
            new_content += f"{msg['date']}\n"
            new_content += f":END:\n\n"
            new_content += f"{msg['main_content']}\n\n"
            if msg['quoted_content']:
                new_content += f"**** Quoted Content\n:QUOTED:\n{msg['quoted_content']}\n:END:\n\n"
            if msg.get('attachments'):
                new_content += "** Attachments\n"
                for att in msg['attachments']:
                    rel_path = os.path.relpath(att, os.path.dirname(org_file))
                    new_content += f"- [[file:{rel_path}][{os.path.basename(att)}]] ([[https://mail.google.com/mail/u/0/#inbox/{msg['msg_id']}][view in Gmail]])\n"
                new_content += "\n"
    
    if new_content:
        if org_file:
            # This case is now handled by Emacs Lisp which captures stdout
            print("---ORG_CONTENT_START---")
            print(new_content.strip())
            print("---ORG_CONTENT_END---")
        else:
            print("---ORG_CONTENT_START---")
            print(new_content.strip())
            print("---ORG_CONTENT_END---")

def _collect_attachments(payload):
    """Return list of attachment filenames found in message payload parts."""
    attachments = []
    def walk(parts):
        for part in parts:
            fname = part.get('filename', '')
            mime  = part.get('mimeType', '')
            if fname and not mime.startswith('text/'):
                attachments.append(fname)
            sub = part.get('parts', [])
            if sub:
                walk(sub)
    walk(payload.get('parts', []))
    return attachments


def handle_fetch_recent(service, days, agenda_files):
    """Fetch recent inbox threads as JSON for the feed buffer."""
    query = f'in:inbox newer_than:{days}d'
    results = []
    try:
        response = service.users().threads().list(userId='me', q=query).execute()
        threads = response.get('threads', [])
        for thread_stub in threads[:50]:
            thread_id = thread_stub['id']
            try:
                thread = service.users().threads().get(
                    userId='me', id=thread_id, format='metadata',
                    metadataHeaders=['Subject', 'From', 'To', 'Date']
                ).execute()
                messages = thread.get('messages', [])
                if not messages:
                    continue
                latest = messages[-1]
                payload = latest.get('payload', {})
                headers = {h['name'].lower(): h['value']
                           for h in payload.get('headers', [])}
                attachments = _collect_attachments(payload)
                results.append({
                    'msg_id':      latest['id'],
                    'thread_id':   thread_id,
                    'subject':     headers.get('subject', 'No Subject'),
                    'from':        headers.get('from', 'Unknown'),
                    'to':          headers.get('to', ''),
                    'date':        convert_to_org_timestamp(headers.get('date', '')),
                    'preview':     latest.get('snippet', '')[:200],
                    'attachments': attachments,
                })
            except HttpError as e:
                logging.warning(f"Failed to fetch thread {thread_id}: {e}")
    except HttpError as e:
        print(f"Error fetching recent threads: {e}", file=sys.stderr)
        sys.exit(1)
    print("---FEED_JSON_START---")
    print(json.dumps(results))
    print("---FEED_JSON_END---")


def handle_sync_threads(service, thread_specs, date_drawer):
    """For each thread spec ('thread_id:known_id1,known_id2'), fetch new messages."""
    for spec in thread_specs:
        if ':' in spec:
            thread_id, known_ids_str = spec.split(':', 1)
            known_ids = set(filter(None, known_ids_str.split(',')))
        else:
            thread_id = spec.strip()
            known_ids = set()
        try:
            messages = list_messages(service, thread_id=thread_id)
            new_messages = [m for m in messages if m['id'] not in known_ids]
            print(f"---THREAD_SYNC_START:{thread_id}---")
            if new_messages:
                new_messages_sorted = sorted(
                    new_messages,
                    key=lambda m: int(m.get('internalDate', '0'))
                    if m.get('internalDate') else 0
                )
                for msg in new_messages_sorted:
                    details = get_message_details(service, msg['id'])
                    if details:
                        content = f"*** Re: {details['subject']}\n"
                        content += ":PROPERTIES:\n"
                        content += f":EMAIL_ID:  {details['msg_id']}\n"
                        content += f":THREAD_ID: {details['thread_id']}\n"
                        content += f":FROM:      {details['from']}\n"
                        content += f":TO:        {details['to']}\n"
                        content += f":SUBJECT:   {details['subject']}\n"
                        content += ":END:\n"
                        content += f":{date_drawer}:\n{details['date']}\n:END:\n\n"
                        content += f"{details['main_content']}\n\n"
                        if details['quoted_content']:
                            content += f"**** Quoted Content\n:QUOTED:\n{details['quoted_content']}\n:END:\n\n"
                        print(content.strip())
            print(f"---THREAD_SYNC_END---")
        except Exception as e:
            logging.error(f"Error syncing thread {thread_id}: {e}")
            print(f"---THREAD_SYNC_START:{thread_id}---")
            print(f"---THREAD_SYNC_END---")


def handle_apply_label(service, thread_id, label_name):
    """Apply LABEL_NAME to THREAD_ID, creating the label if it doesn't exist."""
    try:
        label_map = get_label_id_map(service)
        label_id = label_map.get(label_name)
        if not label_id:
            create_label(service, label_name)
            if os.path.exists(CACHE_PATH):
                os.remove(CACHE_PATH)
            label_map = get_label_id_map(service)
            label_id = label_map.get(label_name)
        if label_id:
            service.users().threads().modify(
                userId='me', id=thread_id,
                body={'addLabelIds': [label_id]}
            ).execute()
            print(f"Label '{label_name}' applied to thread {thread_id}", file=sys.stderr)
        else:
            print(f"Error: could not find or create label '{label_name}'", file=sys.stderr)
            sys.exit(1)
    except HttpError as e:
        print(f"Error applying label: {e}", file=sys.stderr)
        sys.exit(1)


def handle_archive_thread(service, thread_id):
    """Archive THREAD_ID by removing the INBOX label."""
    try:
        service.users().threads().modify(
            userId='me', id=thread_id,
            body={'removeLabelIds': ['INBOX']}
        ).execute()
        print(f"Thread {thread_id} archived", file=sys.stderr)
    except HttpError as e:
        print(f"Error archiving thread {thread_id}: {e}", file=sys.stderr)
        sys.exit(1)


def handle_fetch_message_body(service, msg_id):
    """Fetch full body of a single message and print it with delimiters to stdout."""
    try:
        details = get_message_details(service, msg_id)
        if not details:
            print("---BODY_START---", flush=True)
            print("[Could not fetch message body]", flush=True)
            print("---BODY_END---", flush=True)
            return
        main_content  = details.get('main_content',  '') or ''
        quoted_content = details.get('quoted_content', '') or ''
        print("---BODY_START---", flush=True)
        print(main_content, flush=True)
        if quoted_content.strip():
            print("---QUOTED_START---", flush=True)
            print(quoted_content, flush=True)
        print("---BODY_END---", flush=True)
    except Exception as e:
        print("---BODY_START---", flush=True)
        print(f"[Error fetching body: {e}]", flush=True)
        print("---BODY_END---", flush=True)


def handle_triage_thread(service, thread_id, actions):
    """Apply a combination of triage actions to THREAD_ID in one or two API calls.

    actions is a list of strings from: mark-read, archive, star, delete.
    delete/trash is exclusive and takes priority over label modifications.
    """
    if not thread_id:
        print("triage-thread: no thread_id provided", file=sys.stderr)
        sys.exit(1)
    if 'delete' in actions or 'trash' in actions:
        try:
            service.users().threads().trash(userId='me', id=thread_id).execute()
            print(f"Thread {thread_id} trashed", file=sys.stderr)
        except HttpError as e:
            print(f"Error trashing thread {thread_id}: {e}", file=sys.stderr)
            sys.exit(1)
        return
    add_labels = []
    remove_labels = []
    if 'star' in actions:
        add_labels.append('STARRED')
    if 'mark-read' in actions:
        remove_labels.append('UNREAD')
    if 'archive' in actions:
        remove_labels.append('INBOX')
    if not add_labels and not remove_labels:
        print(f"triage-thread: no recognised actions in {actions}", file=sys.stderr)
        return
    body = {}
    if add_labels:
        body['addLabelIds'] = add_labels
    if remove_labels:
        body['removeLabelIds'] = remove_labels
    try:
        service.users().threads().modify(userId='me', id=thread_id, body=body).execute()
        print(f"Thread {thread_id} triaged: add={add_labels} remove={remove_labels}", file=sys.stderr)
    except HttpError as e:
        print(f"Error triaging thread {thread_id}: {e}", file=sys.stderr)
        sys.exit(1)


def main(label_name=None, org_file=None, date_drawer=None, agenda_files=None, thread_id=None,
          do_sync_email_ids=False, consolidate=False, credentials=None,
          delete_message_id=None, delete_thread_id=None, sync_labels=False,
          modify_thread_labels_args=None, bulk_move_labels_args=None,
          ignore_labels=None, defer_args=None, reply_args=None, delegate_args=None,
          fetch_recent_days=None, sync_threads_specs=None,
          apply_label_args=None, archive_thread_id=None,
          triage_thread_args=None):
    """Main function to drive the script's logic."""
    print(f"Starting main with: label='{label_name}', thread_id='{thread_id}', sync={do_sync_email_ids}", file=sys.stderr)
    
    try:
        service = get_gmail_service(credentials)
        print("Gmail service initialized successfully.", file=sys.stderr)

        if defer_args:
            handle_defer(service, *defer_args)
            return
        
        if reply_args:
            handle_reply(service, *reply_args)
            return

        if delegate_args:
            handle_delegate(service, *delegate_args)
            return

        if bulk_move_labels_args:
            handle_bulk_move_labels(service, *bulk_move_labels_args)
            return

        if modify_thread_labels_args:
            handle_modify_thread_labels(service, *modify_thread_labels_args)
            return

        if sync_labels:
            handle_sync_labels(service, agenda_files, org_file, date_drawer, ignore_labels)
            return

        if do_sync_email_ids:
            handle_sync_email_ids(agenda_files, consolidate, org_file)
            return
        
        if fetch_recent_days is not None:
            handle_fetch_recent(service, fetch_recent_days, agenda_files)
            return

        if sync_threads_specs is not None:
            handle_sync_threads(service, sync_threads_specs, date_drawer or 'org-gmail')
            return

        if apply_label_args:
            handle_apply_label(service, *apply_label_args)
            return

        if archive_thread_id:
            handle_archive_thread(service, archive_thread_id)
            return

        if triage_thread_args:
            handle_triage_thread(service, triage_thread_args[0], triage_thread_args[1:])
            return

        if delete_message_id:
            handle_delete_message(service, delete_message_id)
            return
        if delete_thread_id:
            handle_delete_thread(service, delete_thread_id)
            return

        if label_name:
            handle_download_by_label(service, label_name, org_file, date_drawer, agenda_files)
            return

        # Handle thread-id download
        if thread_id:
            handle_download_thread(service, thread_id, org_file, date_drawer, agenda_files)
    except Exception as e:
        logging.error(f"Error in main: {e}")
        print(f"An error occurred: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Download Gmail emails by label or thread into an Org-mode file, or sync EMAIL_IDs.")
    # Add all arguments
    parser.add_argument('--label', help="Gmail label to search (e.g., '1Projects/MyProject')")
    parser.add_argument('--thread-id', help="Gmail thread ID to fetch new messages for")
    parser.add_argument('--org-file', help="Path to the Org-mode file to append emails")
    parser.add_argument('--date-drawer', help="Name of the Org-mode drawer for email date")
    parser.add_argument('--agenda-files', help="Comma-separated list of Org agenda files to check for existing emails")
    parser.add_argument('--sync-email-ids', action='store_true', help="Sync and report duplicate EMAIL_IDs across agenda files")
    parser.add_argument('--sync-labels', action='store_true', help="Sync all previously downloaded labels.")
    parser.add_argument('--consolidate', action='store_true', help="Consolidate duplicate EMAIL_IDs to org-file when syncing")
    parser.add_argument('--credentials', default='credentials.json', help="Path to the Gmail API credentials file.")
    parser.add_argument('--delete-message', help="Gmail message ID to delete (trash).")
    parser.add_argument('--delete-thread', help="Gmail thread ID to delete (trash).")
    parser.add_argument('--list-labels', action='store_true', help="List all user-created Gmail labels.")
    parser.add_argument('--create-label', help="Create a new Gmail label.")
    parser.add_argument('--delete-label', help="Delete a Gmail label.")
    parser.add_argument('--modify-thread-labels', nargs=3, metavar=('THREAD_ID', 'OLD_LABEL', 'NEW_LABEL'), help="Move a thread from an old label to a new one.")
    parser.add_argument('--bulk-move-labels', nargs=2, metavar=('OLD_LABEL', 'NEW_LABEL'), help="Move all threads from an old label to a new one.")
    parser.add_argument('--ignore-labels', nargs='*', help="A list of regex patterns for labels to ignore during sync.")
    parser.add_argument('--defer', nargs=2, metavar=('MSG_ID', 'TIME'), help="Snooze a message.")
    parser.add_argument('--reply', nargs=4, metavar=('MSG_ID', 'BODY', 'TO', 'CC'), help="Reply to a message.")
    parser.add_argument('--delegate', nargs=3, metavar=('MSG_ID', 'RECIPIENT', 'NOTE'), help="Forward a message.")
    parser.add_argument('--fetch-recent', type=int, metavar='DAYS',
                        help="Fetch recent inbox threads as JSON feed. Use with --agenda-files.")
    parser.add_argument('--sync-threads', nargs='+', metavar='THREAD_SPEC',
                        help="Sync threads by ID. Format: thread_id:known_id1,known_id2. Requires --date-drawer.")
    parser.add_argument('--apply-label', nargs=2, metavar=('THREAD_ID', 'LABEL_NAME'),
                        help="Apply a Gmail label to a thread (creates it if needed).")
    parser.add_argument('--archive-thread', metavar='THREAD_ID',
                        help="Archive a thread by removing the INBOX label.")
    parser.add_argument('--triage-thread', metavar='THREAD_ID',
                        help="Apply triage actions to a thread.")
    parser.add_argument('--triage-actions', nargs='+',
                        choices=['mark-read', 'archive', 'star', 'delete', 'trash'],
                        help="Actions for --triage-thread: mark-read, archive, star, delete.")
    parser.add_argument('--fetch-message-body', metavar='MSG_ID',
                        help="Fetch and print the full body of MSG_ID (stdout, with delimiters).")

    args = parser.parse_args()

    # Determine which action to take
    if args.list_labels:
        service = get_gmail_service(args.credentials)
        list_labels(service)
    elif args.create_label:
        service = get_gmail_service(args.credentials)
        create_label(service, args.create_label)
    elif args.delete_label:
        service = get_gmail_service(args.credentials)
        delete_label(service, args.delete_label)
    elif args.delete_message:
        main(delete_message_id=args.delete_message, credentials=args.credentials)
    elif args.delete_thread:
        main(delete_thread_id=args.delete_thread, credentials=args.credentials)
    elif args.sync_email_ids:
        if not args.agenda_files: parser.error("--sync-email-ids requires --agenda-files")
        main(do_sync_email_ids=True, agenda_files=args.agenda_files, org_file=args.org_file, consolidate=args.consolidate, credentials=args.credentials)
    elif args.sync_labels:
        if not all([args.org_file, args.date_drawer, args.agenda_files]):
            parser.error("--sync-labels requires --org-file, --date-drawer, and --agenda-files")
        main(sync_labels=True, org_file=args.org_file, date_drawer=args.date_drawer, agenda_files=args.agenda_files, credentials=args.credentials, ignore_labels=args.ignore_labels)
    elif args.label:
        if not all([args.org_file, args.date_drawer, args.agenda_files]):
            parser.error("--label requires --org-file, --date-drawer, and --agenda-files")
        main(label_name=args.label, org_file=args.org_file, date_drawer=args.date_drawer, agenda_files=args.agenda_files, credentials=args.credentials)
    elif args.thread_id:
        if not all([args.date_drawer, args.agenda_files]):
            parser.error("--thread-id requires --date-drawer and --agenda-files")
        main(thread_id=args.thread_id, org_file=args.org_file, date_drawer=args.date_drawer, agenda_files=args.agenda_files, credentials=args.credentials)
    elif args.modify_thread_labels:
        main(modify_thread_labels_args=args.modify_thread_labels, credentials=args.credentials)
    elif args.bulk_move_labels:
        main(bulk_move_labels_args=args.bulk_move_labels, credentials=args.credentials)
    elif args.defer:
        main(defer_args=args.defer, credentials=args.credentials)
    elif args.reply:
        main(reply_args=args.reply, credentials=args.credentials)
    elif args.delegate:
        main(delegate_args=args.delegate, credentials=args.credentials)
    elif args.fetch_recent is not None:
        main(fetch_recent_days=args.fetch_recent,
             agenda_files=args.agenda_files, credentials=args.credentials)
    elif args.sync_threads:
        if not args.date_drawer:
            parser.error("--sync-threads requires --date-drawer")
        main(sync_threads_specs=args.sync_threads,
             date_drawer=args.date_drawer, credentials=args.credentials)
    elif args.apply_label:
        main(apply_label_args=args.apply_label, credentials=args.credentials)
    elif args.archive_thread:
        main(archive_thread_id=args.archive_thread, credentials=args.credentials)
    elif args.triage_thread:
        actions = args.triage_actions or []
        main(triage_thread_args=[args.triage_thread] + actions, credentials=args.credentials)
    elif args.fetch_message_body:
        service = get_gmail_service(args.credentials)
        handle_fetch_message_body(service, args.fetch_message_body)
    else:
        parser.error("You must provide a command.")
