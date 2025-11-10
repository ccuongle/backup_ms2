import unittest
from unittest.mock import patch, MagicMock, call
import json
import pika
from ms2_extractor.core.ms2_invoice_extractor import extract_invoice_data
from ms2_extractor.utils import config

class TestExtractorIntegration(unittest.TestCase):

    @patch('ms2_extractor.utils.rabbitmq.pika')
    @patch('ms2_extractor.core.ms2_invoice_extractor.get_model')
    @patch('ms2_extractor.core.ms2_invoice_extractor.os.path.exists')
    @patch('builtins.open')
    def test_extract_invoice_and_publish_integration_with_mock(self, mock_open, mock_exists, mock_get_model, mock_pika):
        # Arrange
        # Mock Pika connection
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_pika.BlockingConnection.return_value = mock_connection
        mock_pika.PlainCredentials.return_value = "credentials"
        mock_pika.ConnectionParameters.return_value = "params"
        mock_pika.BasicProperties.return_value = "properties"

        email_result = {"email_id": "test_email_integration", "isInvoice": True}
        extracted_json = {"invoice_id": "integration-123", "total": 500}

        # Mock parts that are not under test (file system, model)
        mock_exists.return_value = True
        mock_open.return_value.__enter__.return_value.read.return_value = "<xml>integration test</xml>"
        
        mock_model_instance = MagicMock()
        mock_response = MagicMock()
        mock_response.text = json.dumps(extracted_json)
        mock_model_instance.generate_content.return_value = mock_response
        mock_get_model.return_value = mock_model_instance

        # Act
        result = extract_invoice_data(email_result)

        # Assert
        self.assertEqual(result, extracted_json)

        # Assert that a connection was attempted
        mock_pika.BlockingConnection.assert_called_once()
        mock_connection.channel.assert_called_once()

        # Assert that publish was called with the correct parameters
        mock_channel.basic_publish.assert_called_once_with(
            exchange=config.RABBITMQ_EXCHANGE,
            routing_key=config.RABBITMQ_ROUTING_KEY,
            body=json.dumps(extracted_json),
            properties="properties"
        )

        # Assert the connection was closed
        mock_connection.close.assert_called_once()

if __name__ == '__main__':
    unittest.main()