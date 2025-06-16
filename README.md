# Event Data Pipeline Assessment

This assessment implements a modular Python-based pipeline to convert raw JSON event logs into clean, structured CSV outputs. It is designed to enable downstream analytics to use the data directly.

---

## Approach

The pipeline validates essential fields (`user_id`, `timestamp`, `event_type`) and skips malformed records, logging them for review. Valid events are flattened to extract metadata fields like `screen`, `button_id`, and `amount`, and enhanced with a derived `event_date` for aggregation. The logic is split into two scripts: `main.py` for cleaning and transformation, and `aggregations.py` for summary metrics such as event counts, active users, and top users.

---

## Assumptions

- Only events with a valid structure are processed.
- Metadata fields like `amount` may only apply to certain event types (e.g., `purchase`).
- All timestamps are assumed to be in UTC and are parsed using `pandas.to_datetime`.


---
# Instructions to run

## Requirements

This assessment is developed and tested with the following environment:

```
Python  : 3.10 or above
pandas  : 2.0.0 or above
```

---

## Installation

Before running the scripts, install dependencies using the steps below:

```bash

# Upgrade pip
pip install --upgrade pip

# Install required package
pip install pandas
```

---

## How to Run

1. Make sure `raw_events.json` is placed in the same directory as `main.py`

 Note: Depending on your local environment, you may need to use python3 instead of python to run the scripts (main.py, aggregations.py, and   test_main.py). 

2. Run the ingestion and cleaning logic:

```bash
python main.py
```

3. Run the aggregation logic:

```bash
python aggregations.py
```

## Viewing the Output

After running the pipeline, the following files will be generated in the output/ folder:

cleaned_events.csv — The structured and cleaned version of your raw event data.

1_events_summary.csv — Daily event counts per event type.

2_active_users.csv — Total number of unique active users.

3_top_user.csv — User with the highest number of events.

skipped_logs.txt — Log of any malformed or skipped events.

Additionally, summary tables are printed directly to the console when running aggregations.py so you can quickly verify the results.

---

## Tests

Basic unit tests for validation and transformation logic are included in `test_main.py`.

To run the tests:

```bash
python -m unittest test_main.py
```

---

## Author

**Teja Reddy Burri**  
📧 Email: [tejasrikanthredyb@gmail.com](mailto:tejasrikanthredyb@gmail.com)
