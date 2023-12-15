import streamlit as st
from model.insuranceBotModel import *
from utils.chat_ui import message_func 
from streamlit_extras.switch_page_button import switch_page
import time

st.set_page_config(page_title='HealthPlanIQ : Insurance Chat Bot',
page_icon=	":robot_face:",
layout = 'wide',
initial_sidebar_state='collapsed')

if "reset" not in st.session_state:
    switch_page('Home')

if "reset" not in st.session_state:
    st.session_state.reset = False

if "response" not in st.session_state:
    st.session_state.ques_session = True
    if st.session_state["information"]["planid"]=='':
        st.session_state.response = {'answer': 'Hello there! Welcome to our insurance assistance service! How may I help you today? ',
                                'followup_questions': ["Provide me Insurance plan that covers drugs like Atenolol.",
                                                        "Give me 5 plans with cheapest premiums for couples?",
                                                        "List of Plans that cover Dental Insurance."
                                                        ]}
    else:
        st.session_state.response = {'answer': 'Hello there! Welcome to our insurance assistance service! How may I help you today? ',
                                'followup_questions': ["What prescription drugs are covered by the insurance plan?",
                                                        f'What is premium for my insurance plan {st.session_state["information"]["planid"]} ?',
                                                        f'Does Plan {st.session_state["information"]["planid"]} cover dental insurance?'
                                                        ]}
                    

#front-end chatmemory
INITIAL_MESSAGE = [{"role": "assistant",
        "content": "Hello there! Welcome to our insurance assistance service! How may I help you today? "}]
if "messages" not in st.session_state:
    st.session_state["messages"] = INITIAL_MESSAGE


#function to send prompt to model and store reponse
def send_prompt(prompt):
    st.session_state.messages.append({"role": "user", "content": prompt})
    message_func(prompt, is_user =True , is_df=False) #user
    state = st.session_state["information"]["state"]
    county = st.session_state["information"]["county"]
    planid = st.session_state["information"]["planid"]

    with st.spinner('Wait for it...'):
        time.sleep(1)
        st.session_state.response = st.session_state.botModel.get_response(prompt, state, county, planid)

    answer = st.session_state.response["answer"]
    st.session_state.messages.append({"role": "assistant", "content": answer})
    st.session_state.ques_session = True
    st.rerun()


st.markdown("<h1 style='text-align: center;'>HealthPlanIQ : Insurance Chat Bot</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center;'><i>Disclaimer: This bot is designed to provide insights about insurance plans. Before purchasing any plans, we strongly recommend to thoroughly read the documents and consulting subject matter experts.", unsafe_allow_html=True)

for message in st.session_state.messages:
    message_func(
        message["content"],
        True if message["role"] == "user" else False,
        True if message["role"] == "data" else False,
    )

if type(st.session_state.response["followup_questions"]) == str:
    followup_questions = ast.literal_eval(st.session_state.response["followup_questions"])
else:
    followup_questions = st.session_state.response["followup_questions"]

col0, col1, col2, col3, col5 = st.columns([0.05,0.30,0.30,0.30,0.05])

btn1,btn2,btn3 = False,False,False

if st.session_state.response["followup_questions"] != [] and len(st.session_state.response["followup_questions"]) == 3:
    with col1:
        ques1 = followup_questions[0]
        btn1 = st.button(ques1)
       
    with col2:
        ques2 = followup_questions[1]
        btn2 = st.button(ques2)

    with col3:
        ques3 = followup_questions[2]
        btn3 = st.button(ques3)

if prompt := st.chat_input():
        send_prompt(prompt) 

if btn1:
    send_prompt(ques1)
if btn2:
    send_prompt(ques2)
if btn3:
    send_prompt(ques3)


