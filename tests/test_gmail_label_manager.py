import pytest
from unittest.mock import Mock, patch, mock_open
import sys
import os
import json
import tempfile

# Add the current directory to sys.path to import the module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from gmail_label_manager import get_label_id_map, parse_org_for_email_ids, normalize_label, convert_to_org_timestamp

@patch('gmail_label_manager.json.dump')
@patch('gmail_label_manager.os.path.exists')
@patch('gmail_label_manager.open', new_callable=mock_open)
def test_get_label_id_map_no_cache(mock_file, mock_exists, mock_json_dump):
    mock_exists.return_value = False
    # Mock the Gmail service
    mock_service = Mock()
    mock_labels = {
        'labels': [
            {'name': 'INBOX', 'id': 'INBOX_ID'},
            {'name': 'SENT', 'id': 'SENT_ID'}
        ]
    }
    mock_list_method = Mock()
    mock_list_method.return_value.execute.return_value = mock_labels
    mock_service.users.return_value.labels.return_value.list = mock_list_method

    # Call the function
    result = get_label_id_map(mock_service)

    # Assert
    assert result == {'INBOX': 'INBOX_ID', 'SENT': 'SENT_ID'}
    mock_list_method.assert_called_once_with(userId='me')
    # Check cache write
    mock_json_dump.assert_called_once_with({'INBOX': 'INBOX_ID', 'SENT': 'SENT_ID'}, mock_file())

@patch('gmail_label_manager.os.path.exists')
@patch('gmail_label_manager.open', new_callable=mock_open, read_data='{"INBOX": "INBOX_ID"}')
def test_get_label_id_map_with_cache(mock_file, mock_exists):
    mock_exists.return_value = True
    mock_service = Mock()

    result = get_label_id_map(mock_service)

    assert result == {'INBOX': 'INBOX_ID'}
    # Should not call API
    mock_service.users.assert_not_called()

def test_parse_org_for_email_ids():
    # Create a temporary Org file
    org_content = """
* Task 1
:PROPERTIES:
:EMAIL_ID: msg123
:END:
Some content

* Task 2
:PROPERTIES:
:EMAIL_ID: msg456
:END:
More content
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.org', delete=False) as f:
        f.write(org_content)
        temp_file = f.name

    try:
        result = parse_org_for_email_ids(temp_file)
        expected = {'msg123': [temp_file], 'msg456': [temp_file]}
        assert result == expected
    finally:
        os.unlink(temp_file)

def test_normalize_label():
    assert normalize_label('1Projects/MyProject') == '1Projects-MyProject'
    assert normalize_label('INBOX') == 'INBOX'

def test_convert_to_org_timestamp():
    # Mock a date string
    date_str = 'Wed, 07 Dec 2025 11:00:00 +0000'
    result = convert_to_org_timestamp(date_str)
    assert result.startswith('<2025-12-07')
    assert '11:00' in result

@patch('gmail_label_manager.build')
@patch('gmail_label_manager.pickle')
@patch('gmail_label_manager.InstalledAppFlow')
@patch('gmail_label_manager.os.path.exists', return_value=True)
@patch('gmail_label_manager.open', new_callable=mock_open)
def test_get_gmail_service_reauths_when_refresh_token_revoked(
        mock_file, mock_exists, mock_flow, mock_pickle, mock_build):
    from google.auth.exceptions import RefreshError
    from gmail_label_manager import get_gmail_service
    stale = Mock(valid=False, expired=True, refresh_token='r')
    stale.refresh.side_effect = RefreshError('invalid_grant: Bad Request')
    fresh = Mock(valid=True)
    mock_pickle.load.return_value = stale
    mock_flow.from_client_secrets_file.return_value.run_local_server.return_value = fresh

    get_gmail_service('/tmp/creds.json')

    mock_flow.from_client_secrets_file.return_value.run_local_server.assert_called_once()
    mock_pickle.dump.assert_called_once_with(fresh, mock_file())
    mock_build.assert_called_once_with('gmail', 'v1', credentials=fresh)


def test_extract_html_from_email_prefers_html_part_and_skips_attachments():
    from email.message import EmailMessage
    from gmail_label_manager import _extract_html_from_email
    msg = EmailMessage()
    msg.set_content("plain body")
    msg.add_alternative("<p>Hello <b>world</b> — café</p>", subtype='html')
    msg.add_attachment("<p>not me</p>", subtype='html', filename='a.html')
    assert _extract_html_from_email(msg) == "<p>Hello <b>world</b> — café</p>\n"

    plain = EmailMessage()
    plain.set_content("only text")
    assert _extract_html_from_email(plain) == ''


@patch('gmail_label_manager.get_message_details')
def test_fetch_message_body_emits_base64_html_block(mock_details, capsys=None):
    import base64, io, contextlib
    from gmail_label_manager import handle_fetch_message_body
    html = "<p>---BODY_END--- inside html</p>"
    mock_details.return_value = {'main_content': 'hi', 'quoted_content': '', 'html_body': html}
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        handle_fetch_message_body(Mock(), 'm1')
    out = buf.getvalue()
    encoded = out.split("---HTML_START---\n")[1].split("\n---HTML_END---")[0]
    assert base64.b64decode(encoded).decode('utf-8') == html
    assert out.index("---BODY_END---") < out.index("---HTML_START---")


def test_fetch_recent_emits_prediction_signals():
    import io, contextlib, json
    from gmail_label_manager import handle_fetch_recent
    service = Mock()
    threads = service.users.return_value.threads.return_value
    threads.list.return_value.execute.return_value = {'threads': [{'id': 't1'}]}
    threads.get.return_value.execute.return_value = {'messages': [{
        'id': 'm1', 'snippet': 'sale',
        'labelIds': ['INBOX', 'CATEGORY_PROMOTIONS'],
        'payload': {'headers': [
            {'name': 'From', 'value': 'news@shop.com'},
            {'name': 'Subject', 'value': 'Deals'},
            {'name': 'List-Unsubscribe', 'value': '<mailto:u@shop.com>'}]}}]}
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        handle_fetch_recent(service, 7, [])
    out = buf.getvalue()
    email = json.loads(out.split("---FEED_JSON_START---")[1]
                       .split("---FEED_JSON_END---")[0])[0]
    assert email['bulk'] is True
    assert email['categories'] == ['CATEGORY_PROMOTIONS']
