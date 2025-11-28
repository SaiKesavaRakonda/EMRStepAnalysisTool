import boto3
import json
from pydantic import BaseModel
import configparser

config = configparser.ConfigParser()

config.read("awsconfig.ini")

# -------------------------------
# Structured output formats
# -------------------------------
class OutputFormat(BaseModel):
    total_executions: int
    Successful_executions: int
    failed_executions: int
    frequency: str
    avg_execution_time_mins: int 
    outlier_count: int

class QueryResultFormat(BaseModel):
    explanation: str

# -------------------------------
# Initialize Bedrock client
# -------------------------------
bedrock = boto3.client(service_name="bedrock-runtime", region_name="ap-southeast-1", verify=False)  # Ensure AWS credentials with Bedrock access

# -------------------------------
# Helper to invoke a Bedrock model
# -------------------------------

def invoke_bedrock_model(prompt: str, model_id: str = config['bedrock']['modelId']) -> str:
    # Format the request payload using the model's native structure.
    native_request = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1000,
        "temperature": 0,
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": prompt}],
            }
        ],
    }

    # Convert the native request to JSON.

    request = json.dumps(native_request)

    response = bedrock.invoke_model(
        modelId=model_id,
        body=request,
        contentType="application/json",
        accept="application/json"
    )
    result = json.loads(response["body"].read())["content"][0]["text"]
    return result

# -------------------------------
# Function to get execution stats
# -------------------------------
def get_stats(emr_state_info):
    json_list = json.dumps(emr_state_info)

    prompt = f"""
        You are a senior data analyst. Analyze the following EMR execution history JSON data:

        {json_list}

        Your tasks:

        1. Determine execution frequency.
        - If executions occur approximately every 7 days, classify as weekly.
        - When identifying weekly patterns, you MUST get Weekday from ExecutionWeekDay parameter
        2. Calculate the average execution time in minutes (`avg_execution_time_mins`) with these rules:
        - Only include steps with `ExecutionStatus` = "COMPLETED".
        - Exclude steps that have missing `StartDateTime` or `EndDateTime`.
        - Exclude outliers. Define outliers as steps where the duration is more than 1.5 times the interquartile range (IQR) above the third quartile or below the first quartile.
        - Compute duration as the difference between `EndDateTime` and `StartDateTime` in **minutes**.
        - Return the **average** of the filtered durations as `avg_execution_time_mins`.

        3. Produce a summary with the following fields (strict schema):

        - total_executions: Total number of executions.
        - Successful_executions: Count with ExecutionStatus = COMPLETED.
        - failed_executions: Count with non-COMPLETED status.
        - frequency:
            • Daily → include typical execution time.
            • Weekly → include the correct weekday and time (from ExecutionWeekDay)
            • Monthly → include day of month + time
            Exclude outliers when determining frequency.
        - avg_execution_time_mins: Average duration in minutes, excluding failures and outliers.(refer above calculated avg_execution_time_mins)
        - outlier_count: Count of failed executions, abnormal durations, schedule deviations.

        STRICT RULE:  
            When determining weekly execution day, infer it directly from ExecutionWeekDay.
            Don't assume any data
        Only return values for the defined fields. No additional text.
    """

    response_text = invoke_bedrock_model(prompt)
    
    try:
        structured_response = json.loads(response_text)
    except json.JSONDecodeError:
        structured_response = {"error": "LLM response was not valid JSON", "raw": response_text}

    print(structured_response)
    return structured_response

# -------------------------------
# Function to answer custom queries
# -------------------------------
def get_summary(query, emr_state_info):
    json_list = json.dumps(emr_state_info)

    prompt = f"""
You are a senior data analyst. Analyze the following EMR execution history JSON data:

{json_list}

Answer the user query: "{query}"

Return only a JSON object with a single key "explanation" and your answer as the value. Do not include any extra text.
"""

    response_text = invoke_bedrock_model(prompt)
    
    try:
        structured_response = json.loads(response_text)
    except json.JSONDecodeError:
        structured_response = {"error": "LLM response was not valid JSON", "raw": response_text}

    print(structured_response)
    return structured_response

"""# -------------------------------
# Example usage
# -------------------------------
if __name__ == "__main__":
    # Sample EMR execution data
    emr_state_info = [
        {
            "StepId": "s-00000001",
            "StepName": "Batch3-CSVtoParquet",
            "ExecutionStatus": "COMPLETED",
            "StartDateTime": "20251128_100000",
            "EndDateTime": "20251128_110000",
            "ExecutionWeekDay": "Friday"
        },
        {
            "StepId": "s-00000002",
            "StepName": "Batch3-CSVtoParquet",
            "ExecutionStatus": "FAILED",
            "StartDateTime": "20251121_100000",
            "EndDateTime": "20251121_110000",
            "ExecutionWeekDay": "Friday"
        }
    ]

    # Get summary stats
    stats = get_stats(emr_state_info)

    # Example query
    query_result = get_summary("Which step failed most frequently?", emr_state_info)
"""