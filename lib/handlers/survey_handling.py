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
import os, json
import numpy    as np
import pandas   as pd
import datetime as dt
import streamlit as st
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

    def get_survey_result(self):
        st.title("Ερωτηματολόγιο Αξιολόγησης Αυτοκτονικού Κινδύνου")

        # Collecting required information
        doctor_name = st.text_input("Ονοματεπώνυμο Γιατρού", "")
        #doctor_id = st.text_input("ΑΦΜ Γιατρού", "")
        doctor_clinic = st.selectbox("Κλινική Γιατρού", ["Παθολογική", "Καρδιολογική", "Νεφρολογική", "Γυναικολογική", "Ορθοπαιδική"])
        patient_name = st.text_input("Ονοματεπώνυμο Ασθενή", "")
        #patient_id = st.text_input("ΑΦΜ Ασθενή", "")
        patient_age = st.text_input("Ηλικία Ασθενή", "")
        patient_arrival = st.selectbox("Προέλευση Ασθενή", ["Τ.Ε.Π.", "Εξωτερικά Ιατρεία"])


        metadata = {
            "doctor_name" : doctor_name,
            #"doctor_id"   : doctor_id,
            "patient_name" : patient_name,
            #"patient_id" : patient_id,
            "patient_age" : patient_age,
            "doctor_clinic":doctor_clinic,
            "patient_arrival":patient_arrival
        }

        if not all([doctor_name, patient_name, patient_age]): # doctor_id, patient_id
            st.error("All fields are required to proceed.")
            return

        # Initialize session state to track responses
        if 'responses' not in st.session_state:
            st.session_state.responses = []
        
        # Start Questionnaire
        self.ask_q1(metadata)



    def ask_q1(self, metadata):
        st.subheader("Ερώτηση 1: Σκέψεις-Ευχές θανάτου")
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
        st.subheader("Ερώτηση 2: Ιστορικό Αποπειρών Αυτοκτονίας")
        q2 = st.radio(
            "Έχετε κάνει ποτέ κάποια απόπειρα αυτοκτονίας;",
            options = ["Ναι", "Όχι"],
            index = None # Do not preselect any option
        )
        result = ""
        self.store_response("Ερώτηση 2", self.questions["Ερώτηση 2"], q2, metadata, result)
        self.ask_q3(metadata, q2)


    def ask_q3(self, metadata, q2):
        st.subheader("Ερώτηση 3: Κληρονομικότητα")
        q3 = st.radio(
            "Υπάρχει κάποιο άτομο στο οικογενειακό σας περιβάλλον που έχει αυτοκτονήσει ή που έχει κάνει απόπειρα αυτοκτονίας;",
            options = ["Ναι", "Όχι"],
            index = None # Do not preselect any option
        )
        result = ""
        self.store_response("Ερώτηση 3", self.questions["Ερώτηση 3"], q3, metadata, result)
        self.ask_q4(metadata, q2, q3)


    def ask_q4(self, metadata, q2, q3):
        st.subheader("Ερώτηση 4: Αυτοκτονικές Σκέψεις στο Παρόν")
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
        st.subheader("Ερώτηση 5: Αυτοκτονικό Πλάνο - Αυτοκτονική Πρόθεση")
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
        st.subheader("Ερώτηση 6: Πρόσβαση στον Τρόπο Αυτοκτονίας")
        q6 = st.radio(
            "Έχετε πρόσβαση στον τρόπο αυτοκτονίας που μου λέτε;",
            options = ["Ναι", "Όχι"],
            index = None,  # Do not preselect any option
        )

        
        if q6 == "Ναι":
            result = "Υψηλός Κίνδυνος: Άμεση παραπομπή σε ψυχίατρο / νοσηλεία, αναγκαία η ενημέρωση συγγενών, follow-up"
            self.store_response("Ερώτηση 6", self.questions["Ερώτηση 6"], q6, metadata, result)
            st.markdown(f'<p style="color:red;">{result}</p>', unsafe_allow_html=True)
        
        elif q6 == "Όχι" :
            result = "Άμεση παραπομπή σε ψυχίατρο, follow-up, σύσταση για ενημέρωση συγγενών"
            self.store_response("Ερώτηση 6", self.questions["Ερώτηση 6"], q6, metadata, result)
            st.markdown(f'<p style="color:red;">{result}</p>', unsafe_allow_html=True)


    def store_response(self, question_idx, question, answer, metadata, result):
        response = {
            "Timestamp": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Ονοματεπώνυμο Γιατρού": metadata["doctor_name"],
            #"ΑΦΜ Γιατρού": metadata["doctor_id"],
            "Κλινική Γιατρού": metadata["doctor_clinic"],
            "Ονοματεπώνυμο Ασθενή": metadata["patient_name"],
            #"ΑΦΜ Ασθενή": metadata["patient_id"],
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