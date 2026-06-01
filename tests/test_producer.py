from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaException

from producer.producer import publish_order


class TestPublishOrder:
    """Test suite for the publish_order() function using a mocked Kafka producer."""

    @pytest.fixture
    def mock_producer(self):
        return MagicMock()

    @pytest.fixture
    def sample_order(self):
        return {
            "order_id": 9999,
            "city": "Chennai",
            "amount": 250.00
        }

    def test_publish_order_success(self, mock_producer, sample_order):
        topic = "test_topic"
        publish_order(mock_producer, sample_order, topic)

        # Ensure produce() was called
        mock_producer.produce.assert_called_once()
        args, kwargs = mock_producer.produce.call_args

        assert kwargs["topic"] == topic
        assert kwargs["key"] == b"9999"

        # The value should be JSON encoded
        value_dict = json.loads(kwargs["value"].decode("utf-8"))
        assert value_dict["order_id"] == 9999
        assert value_dict["city"] == "Chennai"

        # Ensure poll was called
        mock_producer.poll.assert_called_once_with(0)

    def test_publish_order_handles_kafka_exception(self, mock_producer, sample_order):
        topic = "test_topic"
        # Simulate a Confluent Kafka error
        mock_producer.produce.side_effect = KafkaException("Mocked Kafka Error")

        with pytest.raises(KafkaException):
            publish_order(mock_producer, sample_order, topic)
