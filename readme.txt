Prerequisites:
1. Extract the contents of the attached ZIP file.
2. Configure your AWS Access Key and Secret Key in: ~/.aws/credentials
3. Navigate to the project’s root directory and run: 
   pip install -r requirements.txt

Running the Tool:
1. From the project’s root directory, start the Streamlit application using:
   streamlit run UserInterface.py
2. This will open the tool in your browser (a message will also appear in the terminal with the access URL).
3. Enter the EMR Step Name → Press Enter → Click "Fetch Stats" to retrieve the execution summary.
