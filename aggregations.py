import pandas as pd
import os

input_file = 'output/cleaned_events.csv'

def aggregate_events():
    data = pd.read_csv(input_file, parse_dates=["timestamp"])
    # aggregate count of events by date and type

    events_summary = data.groupby(["event_date", "event_type"]).size().reset_index(name="event_count")
    events_summary.insert(0, "id", range(1, len(events_summary) + 1))
    events_summary.to_csv("output/1_events_summary.csv", index=False)
    print("==== Compute the total number of events per event type per day. ====")
    print(events_summary)

    # count unique users for active user metric
    active_users = pd.DataFrame([{
        "metric": "total_active_users",
        "value": data["user_id"].nunique()
    }])
    active_users.to_csv("output/2_active_users.csv", index=False)
    print("==== Find the total number of active users. ====")
    print(active_users)

    # most active user by event count
    user_counts = data["user_id"].value_counts()
    top_user_summary = pd.DataFrame([{
        "metric": "most_active_user",
        "user_id": user_counts.idxmax(),
        "event_count": user_counts.max()
    }])
    top_user_summary.to_csv("output/3_top_user.csv", index=False)
    print("==== Find the most active app user. ====")
    print(top_user_summary)
    



# Only run this when the script is executed directly
if __name__ == "__main__":
    aggregate_events()
