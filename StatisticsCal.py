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
    )
    json_list = json.dumps(emr_state_info)
    # Invoke agent
    result = agent.invoke({
        
        "messages": [{"role": "user", "content": f"Consider your self as a Data Analyst and based on following data {json_list} get the summary as follows\
                      total_executions: int\
                        Successful_executions: int\
                        failed_executions: int\
                        frequency: str (get full explanation like if daily what time if weekly what day if monthly which day of month, exclude outliers if any) \
                        avg_execution_time_mins: int (exclude outliers if any)\
                      outlier_count: int (This include failed executions, executions took exceptionlly high time, not started as per schedule)"}]
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

