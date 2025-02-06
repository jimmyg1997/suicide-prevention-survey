# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
    [*] Description : Py3 class for MarkI system design for all frameworks
    [*] Author      : Dimitrios Georgiou (dgeorgiou3@gmail.com)
    [*] Date        : Jan, 2024
    [*] Links       :
"""

# -*-*-*-*-*-*-*-*-*-*-* #
#     Basic Modules      #
# -*-*-*-*-*-*-*-*-*-*-* #
import os, json, base64
import numpy    as np
import pandas   as pd
import datetime as dt
import streamlit as st
import streamlit.components.v1 as components

from retry                  import retry
from tqdm                   import tqdm
from dateutil.relativedelta import relativedelta
from datetime               import datetime
from IPython.display        import display
from typing                 import Dict, Any, List

# -*-*-*-*-*-*-*-*-*-*-* #
#     Project Modules    #
# -*-*-*-*-*-*-*-*-*-*-* #

class SurveyHandler():
    def __init__(
            self,
            mk1,
            data_loader
        ) :
        ## System Design
        self.mk1 = mk1

        ## APIs & Handlers
        self.data_loader = data_loader

        ## Survey
        self.questions = {
            "Ερώτηση 1" : "[Σκέψεις-Ευχές θανάτου] Είχατε τον τελευταίο μήνα σκέψεις ότι δεν αξίζει η ζωή, ότι δεν θέλετε να ζείτε, ή όταν πάτε για ύπνο σκέπτεστε ότι θα ήταν καλύτερα να μην ξυπνήσετε;",
            "Ερώτηση 2" : "[Ιστορικό Αποπειρών Αυτοκτονίας] Έχετε κάνει ποτέ κάποια απόπειρα αυτοκτονίας;",
            "Ερώτηση 3" : "[Κληρονομικότητα] Υπάρχει κάποιο άτομο στο οικογενειακό σας περιβάλλον που έχει αυτοκτονήσει ή που έχει κάνει απόπειρα αυτοκτονίας;",
            "Ερώτηση 4" : "[Αυτοκτονικές Σκέψεις στο Παρόν] Είχατε τον τελευταίο μήνα σκέψεις να αυτοκτονήσετε / να βλάψετε τον εαυτό σας;",
            "Ερώτηση 5" : "[Αυτοκτονικό Πλάνο - Αυτοκτονική Πρόθεση] Έχετε σκεφτεί με ποιον τρόπο θα αυτοκτονήσετε;",
            "Ερώτηση 6" : "[Πρόσβαση στον Τρόπο Αυτοκτονίας] Έχετε πρόσβαση στον τρόπο αυτοκτονίας που μου λέτε;"
        }

         
    # def get_base64_of_bin_file(self, bin_file):
    #     """
    #     function to read png file 
    #     ----------
    #     bin_file: png -> the background image in local folder
    #     """
    #     with open(bin_file, 'rb') as f:
    #         data = f.read()
    #     return base64.b64encode(data).decode()

    # def set_background(self, image_path):
    #     """
    #     function to display png as bg
    #         image_path: png -> the background image in local folder
    #     """
    #     bin_str = self.get_base64_of_bin_file(image_path)
    #     page_bg_img = '''
    #     <style>
    #     st.App {
    #     background-image: url("data:image/png;base64,%s");
    #     background-size: cover;
    #     }
    #     </style>
    #     ''' % bin_str
        
    #     st.markdown(page_bg_img, unsafe_allow_html=True)

    # def set_background(self, image_path):
    #     """ Sets a background image with reduced transparency in Streamlit."""
    #     background_style = f"""
    #     <style>
    #     body {
    #         background-image: url('{image_path}');
    #         background-size: cover;
    #     }
    #     </style>
    #     """
    #     st.markdown(background_style, unsafe_allow_html=True)


    def set_background(self, image_path: str, opacity: float = 0.1):
        """
        Sets a background image in a Streamlit app with reduced opacity.

        Parameters:
        - image_url (str): Direct URL to the background image (GitHub raw link).
        - opacity (float): Opacity level (0.0 to 1.0, where 1 is fully visible and 0 is fully transparent).
        """
        st.markdown(
            f"""
            <style>
            .stApp {{
                background: linear-gradient(rgba(255,255,255, {1-opacity}), rgba(255,255,255, {1-opacity})), 
                            url("{image_path}") no-repeat center center fixed;
                background-size: cover;
            }}
            </style>
            """,
            unsafe_allow_html=True
        )


    def set_background_v2(self, image_path: str, opacity: float = 0.5):
        """
        Sets a background image in the main content area of a Streamlit app with reduced opacity.

        Parameters:
        - image_url (str): Direct URL to the background image.
        - opacity (float): Opacity level (0.0 to 1.0, where 1 is fully visible and 0 is fully transparent).
        """
        st.markdown(
            f"""
            <style>
            .block-container {{
                background: linear-gradient(rgba(255,255,255, {1-opacity}), rgba(255,255,255, {1-opacity})), 
                            url("{image_path}") no-repeat center center;
                background-size: cover;
                padding: 20px;
                border-radius: 15px;
            }}
            </style>
            """,
            unsafe_allow_html=True
        )

    def get_survey_result(self):
        st.title("Ερωτηματολόγιο Αξιολόγησης Αυτοκτονικού Κινδύνου Ασθενή")
        
        # Create two columns
        left_column, right_column = st.columns([1, 3])  # Adjust the ratio of column width

        # Left column: Metadata
        #st.image("https://drive.google.com/uc?id=1FSh-igr3BGwh71kRr-nNQBB-KvHfDPbt") #, use_container_width=True)

        with left_column:
            st.image("https://raw.githubusercontent.com/jimmyg1997/suicide-prevention-survey/main/static/8.png", use_container_width=True)
            #st.markdown('<style><img src="https://drive.google.com/uc?id=1FSh-igr3BGwh71kRr-nNQBB-KvHfDPbt"</style> alt="" border="0">')
            doctor_name = st.text_input("Ονοματεπώνυμο Γιατρού", "")
            clinic = st.selectbox("Κλινική", ["Παθολογική", "Καρδιολογική", "Νεφρολογική", "Γυναικολογική", "Ορθοπαιδική"])
            patient_name = st.text_input("Ονοματεπώνυμο Ασθενή", "")
            patient_age = st.text_input("Ηλικία Ασθενή", "")
            patient_vat = st.text_input("AMKA Ασθενή", "")
            patient_arrival = st.selectbox("Προέλευση Ασθενή", ["Τ.Ε.Π.", "Εξωτερικά Ιατρεία"])

            st.image("https://raw.githubusercontent.com/jimmyg1997/suicide-prevention-survey/main/static/9.png", use_container_width=True)

            metadata = {
                "doctor_name"     : doctor_name,
                "clinic"          : clinic,
                "patient_name"    : patient_name,
                "patient_age"     : patient_age,
                "patient_vat"     : patient_vat,
                "patient_arrival" : patient_arrival
            }

        # Right column: Questions
        with right_column:
            # st.markdown(
            #     """
            #     <div style="display: flex; justify-content: center;">
            #         <img src="https://raw.githubusercontent.com/jimmyg1997/suicide-prevention-survey/main/static/3.png" width="250">
            #     </div>
            #     """,
            #     unsafe_allow_html=True
            # )
            #st.image("https://raw.githubusercontent.com/jimmyg1997/suicide-prevention-survey/main/static/3.png", width=250) #use_container_width=True)
            #st.image("https://raw.githubusercontent.com/jimmyg1997/suicide-prevention-survey/main/static/4.png", use_container_width=True)

            # Initialize session state to track responses
            if 'responses' not in st.session_state:
                st.session_state.responses = []
            
            # Start Questionnaire
            self.ask_q1(metadata)



    def ask_q1(self, metadata):
        st.markdown("*1-6 ερωτήσεις, < 1 λεπτό συμπλήρωσης*")
        #st.markdown("*Το πολύ 6 ερωτήσεις και θα χρειαστείς το πολύ 40 δευτερόλεπτα για να τις απαντήσεις*.")
        st.markdown("<h4 style='margin-top:-10px; margin-bottom:-30px;font-size: 20px;'>Ερώτηση 1: Σκέψεις-Ευχές θανάτου</h4>", unsafe_allow_html=True)
        #st.subheader("Ερώτηση 1: Σκέψεις-Ευχές θανάτου")
        q1 = st.radio(
            "Είχατε τον τελευταίο μήνα σκέψεις ότι δεν αξίζει η ζωή, ότι δεν θέλετε να ζείτε, ή όταν πάτε για ύπνο σκέπτεστε ότι θα ήταν καλύτερα να μην ξυπνήσετε;", 
            options = ["Ναι", "Όχι"],
            index = None  # Do not preselect any option
        )

        if q1 == "Ναι":
            result = ""
            self.store_response("Ερώτηση 1", self.questions["Ερώτηση 1"], q1, metadata, result)
            self.ask_q2(metadata)

        elif q1 == "Όχι":
            result = "Παρακολούθηση κατά την επόμενη επίσκεψη"
            self.store_response("Ερώτηση 1", self.questions["Ερώτηση 1"], q1, metadata, result)
            st.markdown(f'<p style="color:green;">{result}</p>', unsafe_allow_html=True)


    def ask_q2(self, metadata):
        st.markdown("<h4 style='margin-top:-10px; margin-bottom:-30px;font-size: 20px;'>Ερώτηση 2: Ιστορικό Αποπειρών Αυτοκτονίας</h4>", unsafe_allow_html=True)

        q2 = st.radio(
            "Έχετε κάνει ποτέ κάποια απόπειρα αυτοκτονίας;",
            options = ["Ναι", "Όχι"],
            index = None # Do not preselect any option
        )
        result = ""
        self.store_response("Ερώτηση 2", self.questions["Ερώτηση 2"], q2, metadata, result)
        self.ask_q3(metadata, q2)


    def ask_q3(self, metadata, q2):
        st.markdown("<h4 style='margin-top:-10px; margin-bottom:-30px;font-size: 20px;'>Ερώτηση 3: Κληρονομικότητα</h4>", unsafe_allow_html=True)
        q3 = st.radio(
            "Υπάρχει κάποιο άτομο στο οικογενειακό σας περιβάλλον που έχει αυτοκτονήσει ή που έχει κάνει απόπειρα αυτοκτονίας;",
            options = ["Ναι", "Όχι"],
            index = None # Do not preselect any option
        )
        result = ""
        self.store_response("Ερώτηση 3", self.questions["Ερώτηση 3"], q3, metadata, result)
        self.ask_q4(metadata, q2, q3)


    def ask_q4(self, metadata, q2, q3):
        st.markdown("<h4 style='margin-top:-10px; margin-bottom:-30px;font-size: 20px;'>Ερώτηση 4: Αυτοκτονικές Σκέψεις στο Παρόν</h4>", unsafe_allow_html=True)
        q4 = st.radio(
            "Είχατε τον τελευταίο μήνα σκέψεις να αυτοκτονήσετε / να βλάψετε τον εαυτό σας;",
            options = ["Ναι", "Όχι"],
            index = None,  # Do not preselect any option
        )

        if q4 == "Ναι":
            result = ""
            self.store_response("Ερώτηση 4", self.questions["Ερώτηση 4"], q4, metadata, result)
            self.ask_q5(metadata)

        elif q4 == "Όχι" :
            if q2 == "Ναι" or q3 == "Ναι":
                result = "Παραπομπή σε ψυχίατρο"
                self.store_response("Ερώτηση 4", self.questions["Ερώτηση 4"], q4, metadata, result)
                st.markdown(f'<p style="color:orange;">{result}</p>', unsafe_allow_html=True)
            else:
                result = "Παραπομπή σε ειδικό ψυχικής υγείας"
                self.store_response("Ερώτηση 4", self.questions["Ερώτηση 4"], q4, metadata, result)
                st.markdown(f'<p style="color:yellow;">{result}</p>', unsafe_allow_html=True)



    def ask_q5(self, metadata):
        st.markdown("<h4 style='margin-top:-10px; margin-bottom:-30px;font-size: 20px;'>Ερώτηση 5: Αυτοκτονικό Πλάνο - Αυτοκτονική Πρόθεση</h4>", unsafe_allow_html=True)
        q5 = st.radio(
            "Έχετε σκεφτεί με ποιον τρόπο θα αυτοκτονήσετε;",
            options = ["Ναι", "Όχι"],
            index = None,  # Do not preselect any option
        )

        if q5 == "Ναι":
            result = ""
            self.store_response("Ερώτηση 5", self.questions["Ερώτηση 5"], q5, metadata, result)
            self.ask_q6(metadata)

        elif q5 == "Όχι" :
            result = "Παραπομπή σε ψυχίατρο και follow-up"
            st.markdown(f'<p style="color:orange;">{result}</p>', unsafe_allow_html=True)
            self.store_response("Ερώτηση 5", self.questions["Ερώτηση 5"], q5, metadata, result)



    def ask_q6(self, metadata):
        st.markdown("<h4 style='margin-top:-10px; margin-bottom:-30px;font-size: 20px;'>Ερώτηση 6: Πρόσβαση στον Τρόπο Αυτοκτονίας</h4>", unsafe_allow_html=True)
        q6 = st.radio(
            "Έχετε πρόσβαση στον τρόπο αυτοκτονίας που μου λέτε;",
            options = ["Ναι", "Όχι"],
            index = None,  # Do not preselect any option
        )

        if q6 == "Ναι":
            result = "Υψηλός Κίνδυνος: Άμεση παραπομπή σε ψυχίατρο / νοσηλεία, αναγκαία η ενημέρωση συγγενών, follow-up"
            self.store_response("Ερώτηση 6", self.questions["Ερώτηση 6"], q6, metadata, result)
            st.markdown(f'<p style="color:red;">{result}</p>', unsafe_allow_html=True)

            # Alternative approach 1
            # alert = """<script>
            #         setTimeout(function(){
            #             alert("Υψηλός Κίνδυνος! Επείγουσα Αντίδραση Απαιτείται!");
            #         }, 1000);  // Delay to trigger after 1 second
            #     </script>
            # """
            # alert_with_image = """
            #     <div style="position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%);
            #                 background-color: white; padding: 20px; border-radius: 10px; box-shadow: 0 0 10px rgba(0, 0, 0, 0.2); z-index: 9999;">
            #         <h2>⚠️ Υψηλός Κίνδυνος</h2>
            #         <img src="https://i.postimg.cc/hvYywVzB/2.png" alt="High Risk Image" style="width: 200px; height: 200px; display: block; margin: 10px auto;">
            #         <p>Υψηλός Κίνδυνος! Επείγουσα Αντίδραση Απαιτείται!</p>
            #     </div>
            #     <script>
            #         setTimeout(function(){
            #             alert("Υψηλός Κίνδυνος! Επείγουσα Αντίδραση Απαιτείται!");
            #         }, 1000);  // Delay to trigger after 1 second
            #     </script>
            # """
            # components.html(
            #     alert, 
            #     height=0, width=0
            # )

            # HTML and JS to create a real popup
            # st.markdown(alert, unsafe_allow_html=True)
            st.image("https://i.postimg.cc/hvYywVzB/2.png")

            # with st.expander("Υψηλός Κίνδυνος!", expanded=True):
            #     st.image("https://i.postimg.cc/hvYywVzB/2.png")
            #     st.warning("Υψηλός Κίνδυνος... Επείγουσα Αντίδραση Απαιτείται!")
        
        elif q6 == "Όχι" :
            result = "Άμεση παραπομπή σε ψυχίατρο, follow-up, σύσταση για ενημέρωση συγγενών"
            self.store_response("Ερώτηση 6", self.questions["Ερώτηση 6"], q6, metadata, result)
            st.markdown(f'<p style="color:red;">{result}</p>', unsafe_allow_html=True)


    def store_response(self, question_idx, question, answer, metadata, result):
        response = {
            "Timestamp": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Ονοματεπώνυμο Γιατρού": metadata["doctor_name"],
            #"ΑΦΜ Γιατρού": metadata["doctor_id"],
            "Κλινική": metadata["clinic"],
            "Ονοματεπώνυμο Ασθενή": metadata["patient_name"],
            "ΑΜΚΑ Ασθενή": metadata["patient_vat"],
            "Ηλικία Ασθενή": metadata["patient_age"],
            "Προέλευση Ασθενή": metadata["patient_arrival"],
            "Ερώτηση (idx)": question_idx,
            "Ερωτήση": question,
            "Απάντηση": answer,
            "Αποτέλεσμα": result
        }
        st.session_state.responses.append(response)



    def log_survey_result(
            self, 
            sheets_reporter_id                 : str, 
            sheets_reporter_tab_survey_results : str
        ):
        # Create two columns
        left_column, right_column = st.columns([1, 2])  # Adjust the ratio of column width

        with right_column :
            # Log results with timestamp
            if st.button("Submit Response"):
                # Converting responses to DataFrame
                if 'responses' in st.session_state:
                    df = pd.DataFrame(st.session_state.responses)
                    print(df)

                    df = df.sort_values('Timestamp')\
                        .drop_duplicates('Ερωτήση', keep='last')\
                        .sort_values('Ερώτηση (idx)')

                    # Save to CSV
                    # timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    # df.to_csv(
                    #     path_or_buf = f"storage/responses_{timestamp}.csv", 
                    #     mode        = "a", 
                    #     header      = False, 
                    #     index       = False
                    # )
                    st.success("Response submitted successfully!")
                    st.dataframe(df)


                    # Save to Google Sheets
                    self.data_loader.append_data_to_google_sheets(
                        df                     = df,
                        spreadsheet_id         = sheets_reporter_id,
                        spreadsheet_range_name = sheets_reporter_tab_survey_results,
                    )

                    return df
                else:
                    st.warning("No responses available to create the dataframe.")
                    return None