import numpy as np
import pandas as pd
from langchain.agents import create_agent
from langchain.agents.structured_output import ToolStrategy
from langchain_openai import ChatOpenAI
from langchain.tools import tool

from langchain_community.vectorstores import Chroma
from langchain_openai import OpenAIEmbeddings
from langchain_core.documents import Document
from GetEMRStepDetails import load_json_array_from_s3
from pydantic import BaseModel
import dotenv
import ssl
import httpx

# Disable SSL verification globally
ssl._create_default_https_context = ssl._create_unverified_context
custom_http_client = httpx.Client(verify=False)
custom_async_http_client = httpx.AsyncClient(verify=False)

dotenv.load_dotenv()

class OutputFormat(BaseModel):
        total_executions: int
        Success_executions: int
        failed_executions: int
        frequency: str
        avg_execution_time_mins: int 

@tool
def retrive_base_data(s3_bucket_name,s3_file_key) -> str:
    """Use the data read from S3 as knowledge base to get required summary of the data"""
    json_list = load_json_array_from_s3(s3_bucket_name,s3_file_key)
    docs = []
    for obj in json_list:
        # Convert dict → readable text
        text = "\n".join([f"{k}: {v}" for k, v in obj.items()])
        
        docs.append(Document(
            page_content=text
        ))
    embeddings = OpenAIEmbeddings()
    vectorstore = Chroma.from_documents(docs, embeddings)
    retriever = vectorstore.as_retriever(search_kwargs={"k": 3})
    result = retriever.invoke({
        "messages": [{"role": "user", "content": "Get summary of data in requested format"}]
    })
    return result["structured_response"]




def get_summary(s3_bucket_name,s3_file_key):
    
        

    # Initialize LLM
    llm = ChatOpenAI(
        model="gpt-oss-20b:free",
        base_url="https://openrouter.ai/api/v1",
        http_client=custom_http_client,
        http_async_client=custom_async_http_client
    )
    
    
    # Create agent
    agent = create_agent(
        model=llm,
        tools=[retrive_base_data],
        response_format=ToolStrategy(OutputFormat),
    )

    # Invoke agent
    result = agent.invoke({
        "messages": [{"role": "user", "content": f"Based on s3_bucket_name:{s3_bucket_name},s3_file_key:{s3_file_key} get summary in requested format"}]
    })

    
    print(result["structured_response"])
    return result["structured_response"]










    df = pd.DataFrame(step_records)
    df["duration_sec"] = (df["EndDateTime"] - df["StartDateTime"]).dt.total_seconds()

    stats = {
        "total_runs": len(df),
        "avg_time": df["duration_sec"].mean(),
        "median_time": df["duration_sec"].median(),
        "p95_time": df["duration_sec"].quantile(0.95),
        "max_time": df["duration_sec"].max(),
        "failures": df[df["ExecutionStatus"] != "COMPLETED"].shape[0],
        "timeouts": df[df["duration_sec"] > df["duration_sec"].mean() + 3*df["duration_sec"].std()].shape[0],
    }

    return stats

#print(compute_stats([{'StepName': 'Batch1-CSVtoParquet', 'ExecutionStatus': 'COMPLETED', 'StartDateTime': datetime.datetime(2025, 11, 27, 15, 20, 40, 933000, tzinfo=tzlocal()), 'EndDateTime': datetime.datetime(2025, 11, 27, 15, 21, 51, 40000, tzinfo=tzlocal())}, {'StepName': 'Batch1-CSVtoParquet', 'ExecutionStatus': 'FAILED', 'StartDateTime': datetime.datetime(2025, 11, 27, 15, 4, 45, 563000, tzinfo=tzlocal()), 'EndDateTime': datetime.datetime(2025, 11, 27, 15, 5, 55, 681000, tzinfo=tzlocal())}, {'StepName': 'Batch1-CSVtoParquet', 'ExecutionStatus': 'FAILED', 'StartDateTime': datetime.datetime(2025, 11, 27, 14, 44, 17, 65000, tzinfo=tzlocal()), 'EndDateTime': datetime.datetime(2025, 11, 27, 14, 45, 25, 211000, tzinfo=tzlocal())}, {'StepName': 'Batch1-CSVtoParquet', 'ExecutionStatus': 'FAILED', 'StartDateTime': datetime.datetime(2025, 11, 27, 14, 37, 36, 833000, tzinfo=tzlocal()), 'EndDateTime': datetime.datetime(2025, 11, 27, 14, 38, 6, 948000, tzinfo=tzlocal())}, {'StepName': 'Batch1-CSVtoParquet', 'ExecutionStatus': 'FAILED', 'StartDateTime': datetime.datetime(2025, 11, 27, 14, 29, 8, 28000, tzinfo=tzlocal()), 'EndDateTime': datetime.datetime(2025, 11, 27, 14, 30, 26, 649000, tzinfo=tzlocal())}, {'StepName': 'Batch1-CSVtoParquet', 'ExecutionStatus': 'FAILED', 'StartDateTime': datetime.datetime(2025, 11, 27, 12, 50, 59, 669000, tzinfo=tzlocal()), 'EndDateTime': datetime.datetime(2025, 11, 27, 12, 52, 14, 188000, tzinfo=tzlocal())}]))