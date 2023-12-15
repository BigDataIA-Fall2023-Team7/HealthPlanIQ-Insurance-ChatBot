import streamlit as st
import re
import warnings
import ast
import os 
from model.insuranceBotModel import *
import snowflake.connector
from streamlit_extras.switch_page_button import switch_page


st.set_page_config(page_title='HealthPlanIQ : Home',
page_icon=	":robot_face:",
layout = 'wide',
initial_sidebar_state='collapsed')

warnings.filterwarnings("ignore")


if "reset" not in st.session_state :
    st.session_state["reset"] = False

if st.session_state.reset is True:
    st.session_state.informationCollected = False
    st.session_state["information"] = {"state":None,"county":None, "planid":''}

# if "snowflake_conn" not in st.session_state:
#     st.session_state.snowflake_conn = st.connection("snowflake")

if "botModel" not in st.session_state or st.session_state.reset:
    openai_api_key = st.secrets.openai_api_key
    openai_model = st.secrets.openai_model
    user=st.secrets.user
    password=st.secrets.password
    account=st.secrets.account
    warehouse=st.secrets.warehouse
    database=st.secrets.database
    schema=st.secrets.schema
    role=st.secrets.role

    st.session_state.botModel = insuranceBotModel(openai_api_key,openai_model,user,password,account,warehouse,database,schema,role)
    st.session_state.reset = False

if "informationCollected" not in st.session_state or st.session_state.informationCollected == False:
    st.session_state.informationCollected = False
    st.session_state["information"] = {"state":"","county":"", "planid":""}

def validID(input_string):
    pattern = re.compile(r'^\d{5}[A-Z]{2}\d{7}$')
    return bool(pattern.match(input_string))

if st.session_state.informationCollected:
    switch_page('ChatBot') 
else :
    left_co, cent_co,right_co = st.columns([3,2,3])
    with cent_co:
        st.image(os.path.join(os.getcwd(),"frontend/images/insurance-chatbot.png"))
    left_co, cent_co,right_co = st.columns([2,3,2])
    with cent_co:
        index = None
        st.markdown("<h1 style='text-align: center;'>Welcome to HealthPlanIQ</h1>", unsafe_allow_html=True)
        st.markdown("<p style='text-align: center;'>Enter the following information to proceed.</p>", unsafe_allow_html=True)
    left_co, cent_co,right_co = st.columns(3)
    with cent_co:

        state_codes = st.session_state.botModel.get_states()
        st.session_state["information"]["state"] = st.selectbox('State',placeholder="Choose an option",options = state_codes, index = index)
        
        if st.session_state["information"]["state"] is None:
            countynames = []
        else:

            countynames = st.session_state.botModel.get_county(st.session_state["information"]["state"])
        st.session_state["information"]["county"] = st.selectbox('County',placeholder="Choose an option",options = countynames, index = index)

        st.session_state["information"]["planid"] = st.text_input("Plan ID [optional]",placeholder="eg. 38344AK1060002")
        
        left_co1, cent_co1,right_co1 = st.columns([3,2,3])
        with cent_co1:
            proceed_button = st.button('PROCEED')
        if proceed_button:    
            if st.session_state["information"]["state"] is None or st.session_state["information"]["county"] is None:
                st.error('Select State and County')
            elif st.session_state.information["planid"]!= "" and not validID(st.session_state.information["planid"]):
                st.error('Enter Valid Insurance Id')
            elif st.session_state.information["planid"]!= "" and not st.session_state.botModel.id_present_in_db(st.session_state.information["state"],st.session_state.information["county"],st.session_state.information["planid"]):
                st.error('Plan Id Not present in Database against selected State and County')
            else: 
                st.session_state.informationCollected = True
                switch_page('ChatBot')
            print(st.session_state["information"])

        
print(f"""------------------------------------------------

st.session_state
{st.session_state}

""")
