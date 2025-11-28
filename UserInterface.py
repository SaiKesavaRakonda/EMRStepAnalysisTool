import streamlit as st
from bedrockInvoker import get_stats,get_summary
from GetEMRStepDetails import fetch_step_metadata, getAwsAccountdetails
import json
st.title("Welcome to EMR Step Analyzer")


account_id = getAwsAccountdetails()
st.title("AWS Account: "+account_id)

if "stats_fetched" not in st.session_state:
    st.session_state.stats_fetched = False
    st.session_state.emr_state_info = None
    st.session_state.stats = None

# First section: Fetch Stats
st.header("Step Stats")
keyword = st.text_input("Enter Step Name Keyword")

if st.button("Fetch Stats"):
    emr_state_info = fetch_step_metadata(keyword)
    #emr_state_info = json.load(open("generatedDataWeekly.json", "r"))
    stats = get_stats(emr_state_info)
    
    # Store in session state
    st.session_state.stats_fetched = True
    st.session_state.emr_state_info = emr_state_info
    st.session_state.stats = stats

# Display stats if fetched
if st.session_state.stats_fetched:
    st.write(st.session_state.stats)
    
    # Second section: Query (only shown after stats are fetched)
    st.divider()
    st.header("Query Steps Summary")
    query = st.text_input("Enter Query here")
    
    if st.button("Query Steps Summary"):
        summary = get_summary(
            query,
            st.session_state.emr_state_info
        )
        st.write(summary)

    
    
