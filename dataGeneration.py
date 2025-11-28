import json
from datetime import datetime, timedelta

# --- USER INPUT ---
frequency = input("Enter frequency (daily / weekly / monthly): ").strip().lower()

if frequency not in ["daily", "weekly", "monthly"]:
    raise ValueError("Invalid input. Must be: daily, weekly, or monthly.")

# --- TIME DELTA SELECTION ---
if frequency == "daily":
    delta = timedelta(days=1)
elif frequency == "weekly":
    delta = timedelta(weeks=1)
elif frequency == "monthly":
    delta = timedelta(days=30)  # can be adjusted to calendar months if needed

# --- START SETUP ---
today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
num_entries = 20
execution_stats = []

current_time = today

for i in range(num_entries):
    start_time = current_time.replace(hour=10, minute=0, second=0)
    end_time = current_time.replace(hour=11, minute=0, second=0)

    execution_stats.append({
        "StepId": f"s-{i+1:08d}UVNJU8X2MF00",
        "StepName": "Batch3-CSVtoParquet",
        "ExecutionStatus": "COMPLETED",
        "StartDateTime": start_time.strftime("%Y:%m:%dT%H:%M:%S"),
        "EndDateTime": end_time.strftime("%Y:%m:%dT%H:%M:%S"),
        "ExecutionWeekDay": "Friday"
    })

    # Move backwards based on frequency
    current_time -= delta

# --- WRITE OUTPUT ---
output_file = "generatedDataWeekly.json"
with open(output_file, "w") as f:
    json.dump(execution_stats, f, indent=2)

print(f"Execution stats written to {output_file}")
