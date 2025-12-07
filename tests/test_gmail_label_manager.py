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