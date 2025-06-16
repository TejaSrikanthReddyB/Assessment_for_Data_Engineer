import unittest
from datetime import datetime
from main import is_valid, get_structured_event
import pandas as pd

class TestEventPipeline(unittest.TestCase):

    def test_valid_event(self):
        valid_event = {
            "user_id": "abc123",
            "timestamp": "2025-03-01T10:15:30+00:00",
            "event_type": "click"
        }
        self.assertTrue(is_valid(valid_event))

    def test_invalid_event_missing_user_id(self):
        invalid_event = {
            "timestamp": "2025-03-01T10:15:30+00:00",
            "event_type": "click"
        }
        self.assertFalse(is_valid(invalid_event))

    def test_get_structured_event(self):
        raw_event = {
            "user_id": "xyz789",
            "timestamp": "2025-03-01T10:15:30+00:00",
            "event_type": "purchase",
            "metadata": {
                "screen": "checkout",
                "amount": "49.99",
                "currency": "USD"
            }
        }

        result = get_structured_event(raw_event)
        print(result)
        self.assertEqual(result["user_id"], "xyz789")
        self.assertEqual(result["event_type"], "purchase")
        self.assertEqual(result["screen"], "checkout")
        self.assertEqual(result["amount"], 49.99)
        self.assertEqual(result["currency"], "USD")
        self.assertEqual(result["event_date"], pd.to_datetime("2025-03-01T10:15:30+00:00").date())

if __name__ == "__main__":
    unittest.main()
