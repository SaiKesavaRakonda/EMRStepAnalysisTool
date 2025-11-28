import numpy as np
import pandas as pd
from langchain.agents import create_agent
from langchain.agents.structured_output import ToolStrategy
from langchain_openai import ChatOpenAI
#from langchain.memory import ConversationBufferMemory

"""from langchain_community.vectorstores import Chroma
from langchain_openai import OpenAIEmbeddings
from langchain_core.documents import Document"""

from GetEMRStepDetails import load_json_array_from_s3
from pydantic import BaseModel
import dotenv, json
import ssl
import httpx

# Disable SSL verification globally
ssl._create_default_https_context = ssl._create_unverified_context
custom_http_client = httpx.Client(verify=False)
custom_async_http_client = httpx.AsyncClient(verify=False)

dotenv.load_dotenv()
#Structured output formats
class OutputFormat(BaseModel):
        total_executions: int
        Successful_executions: int
        failed_executions: int
        frequency: str
        avg_execution_time_mins: int 
        outlier_count: int

class queryResultFormat(BaseModel):
        explanation: str
        


def get_stats(emr_state_info):
    # Initialize LLM
    llm = ChatOpenAI(
        model="gpt-oss-20b:free",
        temperature=0,
        base_url="https://openrouter.ai/api/v1",
        http_client=custom_http_client,
        http_async_client=custom_async_http_client
    )
      
    # Create agent
    agent = create_agent(
        model=llm,
        response_format=ToolStrategy(OutputFormat),
        cache=None 
    )
    json_list = json.dumps(emr_state_info)
    # Invoke agent
    result = agent.invoke({
     
    "messages": [{
    "role": "user",
    "content": f"""
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
        }]

    })

    
    print(result["structured_response"])
    return result["structured_response"]



def get_summary(query,emr_state_info):
    # Initialize LLM
    llm = ChatOpenAI(
        model="gpt-oss-20b:free",
        temperature=0,
        base_url="https://openrouter.ai/api/v1",
        http_client=custom_http_client,
        http_async_client=custom_async_http_client
    )
      
    # Create agent
    agent = create_agent(
        model=llm,

        response_format=ToolStrategy(queryResultFormat),
    )
    json_list = json.dumps(emr_state_info)
    # Invoke agent
    result = agent.invoke({
        
        "messages": [{"role": "user", "content": f"Consider your self as a Data Analyst and based on following data {json_list} answer the query: {query}"}]
    })

    
    print(result["structured_response"])
    return result["structured_response"]

