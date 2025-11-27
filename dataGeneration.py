import json
from datetime import datetime, timedelta

# Starting from today
today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
num_days = 20

execution_stats = []

for i in range(num_days):
    exec_date = today - timedelta(days=i)
    start_time = exec_date.replace(hour=10, minute=0, second=0)
    end_time = exec_date.replace(hour=11, minute=0, second=0)
    
    execution_stats.append({
        "StepId": f"s-{i+1:08d}UVNJU8X2MF00",
        "StepName": "Batch3-CSVtoParquet",
        "ExecutionStatus": "COMPLETED",
        "StartDateTime": start_time.strftime("%Y%m%d_%H%M%S"),
        "EndDateTime": end_time.strftime("%Y%m%d_%H%M%S")
    })

output_file = "generatedData.json"
with open(output_file, "w") as f:
    json.dump(execution_stats, f, indent=2)

print(f"Execution stats written to {output_file}")
