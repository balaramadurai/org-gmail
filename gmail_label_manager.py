import argparse
import base64
import os
import pickle
import re
import sys
import time
from email import policy
from email.parser import BytesParser
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
from collections import defaultdict
import html2text
from datetime import datetime
import email.utils
import pytz
try:
    from orgparse import load as org_load
except ImportError:
    org_load = None

# This scope allows reading, composing, sending, and permanently deleting email.
# We need it to move messages to trash.
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']
MAX_RETRIES = 5
BASE_DELAY = 2  # Seconds
API_TIMEOUT = 120  # Seconds, increased for large mailboxes

def get_gmail_service(credentials_path='credentials.json'):
    """Authenticates with the Gmail API and returns a service object."""
    creds = None
    token_path = os.path.join(os.path.dirname(credentials_path), 'token.json')
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


def get_message_details(service, msg_id):
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

            print(f"Processed message {msg_id}: Subject='{subject}', From='{from_addr}'", file=sys.stderr)
            return {
                'msg_id': msg_id,
                'thread_id': thread_id,
                'subject': subject,
                'from': from_addr,
                'to': to_addr,
                'date': org_timestamp,
                'main_content': main_content,
                'quoted_content': quoted_content
            }
        except HttpError as error:
            print(f'Error downloading message {msg_id} (attempt {attempt + 1}/{MAX_RETRIES}): {error}', file=sys.stderr)
            if attempt < MAX_RETRIES - 1:
                time.sleep(BASE_DELAY * (2 ** attempt))
            else:
                raise
    return None

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

def main(label_name=None, org_file=None, date_drawer=None, agenda_files=None, thread_id=None, 
         do_sync_email_ids=False, consolidate=False, credentials=None, 
         delete_message_id=None, delete_thread_id=None):
    """Main function to drive the script's logic."""
    print(f"Starting main with: label='{label_name}', thread_id='{thread_id}', sync={do_sync_email_ids}", file=sys.stderr)
    
    if do_sync_email_ids:
        sync_email_ids(agenda_files, consolidate, org_file)
        return

    service = get_gmail_service(credentials)
    print("Gmail service initialized successfully.", file=sys.stderr)
    
    if delete_message_id:
        delete_message(service, delete_message_id)
        return
    if delete_thread_id:
        delete_thread(service, delete_thread_id)
        return

    messages = []
    if thread_id:
        messages = list_messages(service, thread_id=thread_id)
    elif label_name:
        normalized_label = normalize_label(label_name)
        query = f'label:"{normalized_label}"'
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
    existing_email_ids = parse_org_for_email_ids(agenda_files)
    
    print(f"DEBUG: Found {len(messages)} messages via API before filtering.", file=sys.stderr)
    print(f"DEBUG: Found {len(existing_email_ids)} existing messages in Org files.", file=sys.stderr)

    new_messages = [msg for msg in messages if msg['id'] not in existing_email_ids]
    print(f"DEBUG: Found {len(new_messages)} new messages after filtering.", file=sys.stderr)

    if not new_messages:
        print(f"All messages for {'thread ' + thread_id if thread_id else 'label: ' + label_name} are already downloaded.", file=sys.stderr)
        return

    print(f"Found {len(new_messages)} new messages to download.", file=sys.stderr)
    threads = defaultdict(list)
    for i, msg in enumerate(new_messages):
        print(f"--- Processing message {i+1}/{len(new_messages)} (ID: {msg['id']}) ---", file=sys.stderr)
        msg_details = get_message_details(service, msg['id'])
        if msg_details:
            threads[msg_details['thread_id']].append(msg_details)

    new_content = ""
    if label_name:
        new_content = f"* Emails for Label: {label_name}\n"

    for thread_id_key, messages_in_thread in threads.items():
        messages_in_thread.sort(key=lambda x: x['date'])
        first_msg = messages_in_thread[0]
        subject = first_msg['subject']

        if label_name:
            new_content += f"\n** [[https://mail.google.com/mail/u/0/#inbox/{thread_id_key}][{subject}]]\n"
        
        for msg in messages_in_thread:
            is_reply = msg != first_msg
            if thread_id or (label_name and is_reply):
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

    if new_content:
        if org_file:
            org_file_path = os.path.expanduser(org_file)
            os.makedirs(os.path.dirname(org_file_path), exist_ok=True)
            print(f"Writing to {org_file_path}", file=sys.stderr)
            with open(org_file_path, 'a', encoding='utf-8') as f:
                f.write(new_content)
            print(f"Successfully appended {len(new_messages)} new messages to {org_file_path}", file=sys.stderr)
        else:
            # For inserting into buffer, separate content from logs
            print("---ORG_CONTENT_START---")
            print(new_content.strip())
            print("---ORG_CONTENT_END---")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Download Gmail emails by label or thread into an Org-mode file, or sync EMAIL_IDs.")
    # Add all arguments
    parser.add_argument('--label', help="Gmail label to search (e.g., '1Projects/MyProject')")
    parser.add_argument('--thread-id', help="Gmail thread ID to fetch new messages for")
    parser.add_argument('--org-file', help="Path to the Org-mode file to append emails")
    parser.add_argument('--date-drawer', help="Name of the Org-mode drawer for email date")
    parser.add_argument('--agenda-files', help="Comma-separated list of Org agenda files to check for existing emails")
    parser.add_argument('--sync-email-ids', action='store_true', help="Sync and report duplicate EMAIL_IDs across agenda files")
    parser.add_argument('--consolidate', action='store_true', help="Consolidate duplicate EMAIL_IDs to org-file when syncing")
    parser.add_argument('--credentials', default='credentials.json', help="Path to the Gmail API credentials file.")
    parser.add_argument('--delete-message', help="Gmail message ID to delete (trash).")
    parser.add_argument('--delete-thread', help="Gmail thread ID to delete (trash).")
    parser.add_argument('--list-labels', action='store_true', help="List all user-created Gmail labels.")
    
    args = parser.parse_args()

    # Determine which action to take
    if args.list_labels:
        service = get_gmail_service(args.credentials)
        list_labels(service)
    elif args.delete_message:
        main(delete_message_id=args.delete_message, credentials=args.credentials)
    elif args.delete_thread:
        main(delete_thread_id=args.delete_thread, credentials=args.credentials)
    elif args.sync_email_ids:
        if not args.agenda_files: parser.error("--sync-email-ids requires --agenda-files")
        main(do_sync_email_ids=True, agenda_files=args.agenda_files, org_file=args.org_file, consolidate=args.consolidate, credentials=args.credentials)
    elif args.label:
        if not all([args.org_file, args.date_drawer, args.agenda_files]):
            parser.error("--label requires --org-file, --date-drawer, and --agenda-files")
        main(label_name=args.label, org_file=args.org_file, date_drawer=args.date_drawer, agenda_files=args.agenda_files, credentials=args.credentials)
    elif args.thread_id:
        if not all([args.date_drawer, args.agenda_files]):
            parser.error("--thread-id requires --date-drawer and --agenda-files")
        main(thread_id=args.thread_id, org_file=args.org_file, date_drawer=args.date_drawer, agenda_files=args.agenda_files, credentials=args.credentials)
    else:
        parser.error("You must provide a command: --label, --thread-id, --sync-email-ids, --delete-message, --delete-thread, or --list-labels.")
