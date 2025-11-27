import streamlit as st
from StatisticsCal import get_stats,get_summary
from GetEMRStepDetails import fetch_step_metadata, getAwsAccountdetails

st.title("Welcome to EMR Step Analyzer")


account_id = getAwsAccountdetails()
st.title("AWS Account: "+account_id)

if "stats_fetched" not in st.session_state:
    st.session_state.stats_fetched = False
    st.session_state.s3_bucket_name = None
    st.session_state.s3_file_key = None
    st.session_state.stats = None

# First section: Fetch Stats
st.header("Step Stats")
keyword = st.text_input("Enter Step Name Keyword")

if st.button("Fetch Stats"):
    s3_bucket_name, s3_file_key = fetch_step_metadata(keyword)
    #s3_bucket_name, s3_file_key = "emrstepanalysistool","Batch3/generatedData.json"
    stats = get_stats(s3_bucket_name, s3_file_key)
    
    # Store in session state
    st.session_state.stats_fetched = True
    st.session_state.s3_bucket_name = s3_bucket_name
    st.session_state.s3_file_key = s3_file_key
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
            st.session_state.s3_bucket_name,
            st.session_state.s3_file_key
        )
        st.write(summary)

    
    
