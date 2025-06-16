import pandas as pd
import os, json
from datetime import datetime


input_file = 'raw_events.json'
output_file = 'output/cleaned_events.csv'
log_file = 'output/skipped_logs.txt'

def is_valid(event):
    # Checking if all required fields exist and are not empty
    if 'user_id' not in event or not event['user_id']:
        return False
    if 'timestamp' not in event or not event['timestamp']:
        return False
    if 'event_type' not in event or not event['event_type']:
        return False
    return True

def get_structured_event(event):
    user_id = event.get("user_id")
    timestamp = event["timestamp"]
    event_date = pd.to_datetime(event["timestamp"]).date()
    event_type = event["event_type"]
    screen = event["metadata"].get("screen")
    button_id = event["metadata"].get("button_id")
    amount = float(event["metadata"].get("amount")) if event["metadata"].get("amount") else None
    currency = event["metadata"].get("currency", None)

    return {
        "user_id": user_id,
        "timestamp": timestamp,
        "event_date": event_date,
        "event_type": event_type,
        "screen": screen,
        "button_id": button_id,
        "amount": amount,
        "currency": currency
    }

def main():

    os.makedirs("output", exist_ok=True)

    with open(input_file, "r") as file:
        raw_events = json.load(file)
    
    valid_events = []
    skipped_events = []

    for event in raw_events:
        if is_valid(event):
            try:
                structured_event = get_structured_event(event)
                valid_events.append(structured_event)
            except Exception as e:
                skipped_events.append(f"{event} - Error captured: {str(e)}")

        else:
            skipped_events.append("Validation failed for : ", {event})
    
    df = pd.DataFrame(valid_events)
    df.to_csv(output_file, index=False)

    with open(log_file, "w") as log:
        for line in skipped_events:
            log.write(line + "\n")

if __name__ == "__main__":
    main()