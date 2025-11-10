import pytest
from unittest.mock import patch, MagicMock
from ms2_extractor.core.ms2_invoice_extractor import extract_invoice_data
import json

@pytest.fixture
def mock_rabbitmq_connection():
    """Fixture to mock the RabbitMQConnection."""
    with patch('ms2_extractor.core.ms2_invoice_extractor.RabbitMQConnection') as mock_conn:
        mock_instance = MagicMock()
        mock_conn.return_value = mock_instance
        yield mock_instance

@patch('ms2_extractor.core.ms2_invoice_extractor._load_xml_content')
@patch('ms2_extractor.core.ms2_invoice_extractor.map_invoice')
def test_extract_invoice_data_publishes_on_success(
    mock_map_invoice,
    mock_load_xml,
    mock_rabbitmq_connection
):
    """
    Tests that extract_invoice_data calls publish on successful extraction.
    """
    # Arrange
    test_email_id = "test_email_123"
    mock_xml_content = "<xml>some data</xml>"
    mock_extracted_data = {"invoice_id": "inv-001", "total": 100, "items": []}
    
    mock_load_xml.return_value = mock_xml_content
    mock_map_invoice.return_value = mock_extracted_data

    # Act
    result = extract_invoice_data(test_email_id)

    # Assert
    # 1. Check that the original functions were called
    mock_load_xml.assert_called_once_with(test_email_id)
    mock_map_invoice.assert_called_once_with(mock_xml_content)
    
    # 2. Check that the function returns the correct data
    assert result == mock_extracted_data

    # 3. Check that RabbitMQ connection was initiated and used
    mock_rabbitmq_connection.connect.assert_called_once()
    
    # 4. Check that publish was called with the correct arguments
    expected_body = json.dumps(mock_extracted_data, ensure_ascii=False)
    mock_rabbitmq_connection.publish.assert_called_once_with(
        exchange='invoice_exchange',
        routing_key='queue.for_persistence',
        body=expected_body
    )

    # 5. Check that the connection was closed
    mock_rabbitmq_connection.close.assert_called_once()

@patch('ms2_extractor.core.ms2_invoice_extractor._load_xml_content')
def test_extract_invoice_data_does_not_publish_on_failure(
    mock_load_xml,
    mock_rabbitmq_connection
):
    """
    Tests that extract_invoice_data does NOT call publish on failed extraction.
    """
    # Arrange
    test_email_id = "test_email_456"
    mock_load_xml.return_value = None # Simulate extraction failure

    # Act
    result = extract_invoice_data(test_email_id)

    # Assert
    # 1. Check that the extraction was attempted
    mock_load_xml.assert_called_once_with(test_email_id)
    
    # 2. Check that the function returns None
    assert result is None

    # 3. Check that RabbitMQ connection was NOT initiated
    mock_rabbitmq_connection.connect.assert_not_called()
    
    # 4. Check that publish was NOT called
    mock_rabbitmq_connection.publish.assert_not_called()