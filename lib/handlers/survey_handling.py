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

#         Ερώτηση 1: Καταθλιπτικό επεισόδιο
# Είχατε περάσει ποτέ διάστημα πάνω από 10 μέρες που είχατε άσχημη διάθεση, πηγαίνατε
# με το ζόρι στην δουλειά , είχατε αυπνία η υπνηλία, αισθανόσασταν κουρασμένος,
# καταθλιπτικός;
#  Είτε η απάντηση είναι &quot;Ναι&quot;, είτε είναι &quot;Οχι&quot;, το ερωτηματολόγιο ανοίγει την
# Ερώτηση 2


        self.questions = {
            "Ερώτηση 1" : "[Καταθλιπτικό επεισόδιο] Είχατε περάσει ποτέ διάστημα πάνω από 10 μέρες που είχατε άσχημη διάθεση, πηγαίνατε με το ζόρι στην δουλειά , είχατε αυπνία η υπνηλία, αισθανόσασταν κουρασμένος, καταθλιπτικός;",
            "Ερώτηση 2" : "[Σκέψεις-Ευχές θανάτου] Είχατε τον τελευταίο μήνα σκέψεις ότι δεν αξίζει η ζωή, ότι δεν θέλετε να ζείτε, ή όταν πάτε για ύπνο σκέπτεστε ότι θα ήταν καλύτερα να μην ξυπνήσετε;",
            "Ερώτηση 3" : "[Ιστορικό Αποπειρών Αυτοκτονίας] Έχετε κάνει ποτέ κάποια απόπειρα αυτοκτονίας;",
            "Ερώτηση 4" : "[Κληρονομικότητα, Ευαλοτώτητα] Υπάρχει κάποιο άτομο στο οικογενειακό σας περιβάλλον που έχει αυτοκτονήσει ή που έχει κάνει απόπειρα αυτοκτονίας;",
            "Ερώτηση 5" : "[Αυτοκτονικές Σκέψεις στο Παρόν] Είχατε τον τελευταίο μήνα σκέψεις να αυτοκτονήσετε / να βλάψετε τον εαυτό σας;",
            "Ερώτηση 6" : "[Αυτοκτονικό Πλάνο - Αυτοκτονική Πρόθεση] Έχετε σκεφτεί με ποιον τρόπο θα αυτοκτονήσετε;",
            "Ερώτηση 7" : "[Πρόσβαση στον Τρόπο Αυτοκτονίας] Έχετε πρόσβαση στον τρόπο αυτοκτονίας που μου λέτε;"
        }

        self.clinics = [
            "Παθολογική",
            "Αιματολογική",
            "Καρδιολογική",
            "Νεφρολογίας & Μεταμόσχευσης Νεφρού",
            "Μονάδα Εντατικής Θεραπείας",
            "Μονάδα Ειδικών Λοιμώξεων",
            "Αλλεργιολογικό Τμήμα",
            "Νευρολογικό Τμήμα",
            "Πνευμονολογικό Τμήμα",
            "Γαστρεντερολογική Κλινική",
            "Χειρουργική",
            "Αγγειοχειρουργική",
            "Γυναικολογική",
            "Οφθαλμολογική",
            "Ορθοπαιδική",
            "Ουρολογική",
            "Ωτορινολαρυγγολογική",
            "Μονάδα Μεταμόσχευσης",
            "Αναισθησιολογικό",
            "Οδοντιατρικό",
            "Άλλη"
        ]



    def set_background(self, image_path: str, opacity: float = 0.3):
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
        # st.title("Ερωτηματολόγιο Αξιολόγησης Αυτοκτονικού Κινδύνου Ασθενή")

        st.markdown(
            """
            <style>
            /* Responsive title */
            h1 {
                font-size: 2.2vw;  /* Scales with screen width */
                display: flex;
                align-items: center;
            }

            /* Mobile adjustments */
            @media (max-width: 768px) {
                h1 {
                    font-size: 4.5vw; /* Adjust for mobile */
                }
            }

            /* Image styling to match text height */
            h1 img {
                height: 1em; /* Matches text height */
                margin-left: 10px; /* Spacing between text and image */
            }
            </style>
            
            <h1>
                Ερωτηματολόγιο Αξιολόγησης Αυτοκτονικού Κινδύνου Ασθενή
                <img src="https://raw.githubusercontent.com/jimmyg1997/suicide-prevention-survey/main/static/18.png" 
                    alt="Logo">
            </h1>
            """,
            unsafe_allow_html=True
        )

           
        # Create two columns
        left_column, right_column = st.columns([1, 3])  # Adjust the ratio of column width

        # Left column: Metadata
        #st.image("https://drive.google.com/uc?id=1FSh-igr3BGwh71kRr-nNQBB-KvHfDPbt") #, use_container_width=True)

        with left_column:
            st.image("https://raw.githubusercontent.com/jimmyg1997/suicide-prevention-survey/main/static/8.png", use_container_width=True)
            st.markdown("*Όλα τα πεδία είναι προαιρετικά*")
            #st.markdown('<style><img src="https://drive.google.com/uc?id=1FSh-igr3BGwh71kRr-nNQBB-KvHfDPbt"</style> alt="" border="0">')
            #doctor_name = st.text_input("Ονοματεπώνυμο Γιατρού", "")
            clinic = st.selectbox("Κλινική", self.clinics)
            #patient_name = st.text_input("Ονοματεπώνυμο Ασθενή", "")
            patient_age = st.text_input("Ηλικία Ασθενή", "")
            patient_gender = st.selectbox("Γένος", ["ΑΡΡΕΝ", "ΘΗΛΥ"])
            patient_vat = st.text_input("AMKA Ασθενή", "")
            patient_arrival = st.selectbox("Προέλευση Ασθενή", ["Τ.Ε.Π.", "Εξωτερικά Ιατρεία"])
            survey_reason = st.selectbox("Λόγος συμπλήρωσης ερωτηματολογίου", ["Απόπειρα Αυτοκτονίας", "Σκέψεις αυτοκτονίας/Θανάτου/απελπισίας", "Αυτοτραυματισμός"])

            metadata = {
                #"doctor_name"     : doctor_name,
                "clinic"          : clinic,
                #"patient_name"    : patient_name,
                "patient_age"     : patient_age,
                "patient_gender"  : patient_gender,
                "patient_vat"     : patient_vat,
                "patient_arrival" : patient_arrival,
                "survey_reason"   : survey_reason
            }

            st.image("https://raw.githubusercontent.com/jimmyg1997/suicide-prevention-survey/main/static/9.png", use_container_width=True)

        # Right column: Questions
        with right_column:
            # image_path = "https://raw.githubusercontent.com/jimmyg1997/suicide-prevention-survey/main/static/14.png"
            # opacity = 0.9
            # st.markdown(
            #     f"""
            #     <style>
            #     /* Target only the second column in the layout */
            #     .stColumn:nth-child(2) {{
            #         background: linear-gradient(rgba(255,255,255,{opacity}), rgba(255,255,255,{opacity})), 
            #                     url("{image_path}") no-repeat center center fixed;
            #         background-size: cover;
            #         background-position: center;
            #         padding: 10px;  /* Optional: add padding to ensure content doesn't overlap */
            #     }}
            #     </style>
            #     """,
            #     unsafe_allow_html=True
            # )
                    
            #st.image("https://raw.githubusercontent.com/jimmyg1997/suicide-prevention-survey/main/static/3.png", width=250) #use_container_width=True)
            #st.image("https://raw.githubusercontent.com/jimmyg1997/suicide-prevention-survey/main/static/4.png", use_container_width=True)

            # Initialize session state to track responses
            if 'responses' not in st.session_state:
                st.session_state.responses = []
            
            # Start Questionnaire
            st.markdown("*1-7 ερωτήσεις, < 1 λεπτό συμπλήρωσης*")
            self.ask_q1(metadata)

    def ask_q1(self, metadata):
        st.markdown("<h4 style='margin-top:-10px; margin-bottom:-30px;font-size: 20px;'>Ερώτηση 1: Καταθλιπτικό επεισόδιο</h4>", unsafe_allow_html=True)
        
        q1 = st.radio(
            "Είχατε περάσει ποτέ διάστημα πάνω από 10 μέρες που είχατε άσχημη διάθεση, πηγαίνατε με το ζόρι στην δουλειά , είχατε αυπνία η υπνηλία, αισθανόσασταν κουρασμένος, καταθλιπτικός;",
            options = ["Ναι", "Όχι"],
            index = None # Do not preselect any option
        )
        result = ""
        self.store_response("Ερώτηση 1", self.questions["Ερώτηση 1"], q1, metadata, result)
        self.ask_q2(metadata)

    def ask_q2(self, metadata):
        #st.markdown("*Το πολύ 6 ερωτήσεις και θα χρειαστείς το πολύ 40 δευτερόλεπτα για να τις απαντήσεις*.")
        st.markdown("<h4 style='margin-top:-10px; margin-bottom:-30px;font-size: 20px;'>Ερώτηση 2: Σκέψεις-Ευχές θανάτου</h4>", unsafe_allow_html=True)
        #st.subheader("Ερώτηση 1: Σκέψεις-Ευχές θανάτου")
        q2 = st.radio(
            "Είχατε τον τελευταίο μήνα σκέψεις ότι δεν αξίζει η ζωή, ότι δεν θέλετε να ζείτε, ή όταν πάτε για ύπνο σκέπτεστε ότι θα ήταν καλύτερα να μην ξυπνήσετε;", 
            options = ["Ναι", "Όχι"],
            index = None  # Do not preselect any option
        )

        if q2 == "Ναι":
            result = ""
            self.store_response("Ερώτηση 2", self.questions["Ερώτηση 2"], q2, metadata, result)
            self.ask_q3(metadata)

        elif q2 == "Όχι" :
            result = "Παρακολούθηση κατά την επόμενη επίσκεψη"
            st.markdown(f'<p style="color:green;">{result}</p>', unsafe_allow_html=True)
            self.store_response("Ερώτηση 2", self.questions["Ερώτηση 2"], q2, metadata, result)



    def ask_q3(self, metadata):
        st.markdown("<h4 style='margin-top:-10px; margin-bottom:-30px;font-size: 20px;'>Ερώτηση 3: Ιστορικό Αποπειρών Αυτοκτονίας</h4>", unsafe_allow_html=True)

        q3 = st.radio(
            "Έχετε κάνει ποτέ κάποια απόπειρα αυτοκτονίας;",
            options = ["Ναι", "Όχι"],
            index = None # Do not preselect any option
        )
        result = ""
        self.store_response("Ερώτηση 3", self.questions["Ερώτηση 3"], q3, metadata, result)
        self.ask_q4(metadata)


    def ask_q4(self, metadata):
        st.markdown("<h4 style='margin-top:-10px; margin-bottom:-30px;font-size: 20px;'>Ερώτηση 4: Κληρονομικότητα, Ευαλοτώτητα</h4>", unsafe_allow_html=True)
        q4 = st.radio(
            "Υπάρχει κάποιο άτομο στο οικογενειακό σας περιβάλλον που έχει αυτοκτονήσει ή που έχει κάνει απόπειρα αυτοκτονίας;",
            options = ["Ναι", "Όχι"],
            index = None # Do not preselect any option
        )
        result = ""
        self.store_response("Ερώτηση 4", self.questions["Ερώτηση 4"], q4, metadata, result)
        self.ask_q5(metadata)


    def ask_q5(self, metadata):
        st.markdown("<h4 style='margin-top:-10px; margin-bottom:-30px;font-size: 20px;'>Ερώτηση 5: Αυτοκτονικές Σκέψεις στο Παρόν</h4>", unsafe_allow_html=True)
        q5 = st.radio(
            "Είχατε τον τελευταίο μήνα σκέψεις να αυτοκτονήσετε / να βλάψετε τον εαυτό σας;",
            options = ["Ναι", "Όχι"],
            index = None,  # Do not preselect any option
        )

        if q5 == "Ναι":
            result = ""
            self.store_response("Ερώτηση 5", self.questions["Ερώτηση 5"], q5, metadata, result)
            self.ask_q6(metadata)

        elif q5 == "Όχι" :
            result = "Προτείνεται επίσκεψη σε ειδικό ψυχικής υγείας"
            self.store_response("Ερώτηση 5", self.questions["Ερώτηση 5"], q5, metadata, result)
            st.markdown(f'<p style="color:orange;">{result}</p>', unsafe_allow_html=True)



    def ask_q6(self, metadata):
        st.markdown("<h4 style='margin-top:-10px; margin-bottom:-30px;font-size: 20px;'>Ερώτηση 6: Αυτοκτονικό Πλάνο - Αυτοκτονική Πρόθεση</h4>", unsafe_allow_html=True)
        q6 = st.radio(
            "Έχετε σκεφτεί με ποιον τρόπο θα αυτοκτονήσετε;",
            options = ["Ναι", "Όχι"],
            index = None,  # Do not preselect any option
        )

        if q6 == "Ναι":
            result = ""
            self.store_response("Ερώτηση 6", self.questions["Ερώτηση 6"], q6, metadata, result)
            self.ask_q7(metadata)

        elif q6 == "Όχι" :
            result = "Προτείνεται άμεση επίσκεψη σε ψυχίατρο"
            st.markdown(f'<p style="color:red;">{result}</p>', unsafe_allow_html=True)
            self.store_response("Ερώτηση 6", self.questions["Ερώτηση 6"], q6, metadata, result)



    def ask_q7(self, metadata):
        st.markdown("<h4 style='margin-top:-10px; margin-bottom:-30px;font-size: 20px;'>Ερώτηση 7: Πρόσβαση στον Τρόπο Αυτοκτονίας</h4>", unsafe_allow_html=True)
        q7 = st.radio(
            "Έχετε πρόσβαση στον τρόπο αυτοκτονίας που μου λέτε;",
            options = ["Ναι", "Όχι"],
            index = None,  # Do not preselect any option
        )

        if q7 == "Ναι":
            result = "Υψηλός Κίνδυνος: Άμεση αξιολόγηση από ψυχίατρο / νοσηλεία, αναγκαία η ενημέρωση συγγενών, follow-up"
            self.store_response("Ερώτηση 7", self.questions["Ερώτηση 7"], q7, metadata, result)
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
            st.image("https://raw.githubusercontent.com/jimmyg1997/suicide-prevention-survey/main/static/16.png", width=100)
            # st.image("https://i.postimg.cc/hvYywVzB/2.png", width=100)

            # with st.expander("Υψηλός Κίνδυνος!", expanded=True):
            #     st.image("https://i.postimg.cc/hvYywVzB/2.png")
            #     st.warning("Υψηλός Κίνδυνος... Επείγουσα Αντίδραση Απαιτείται!")
        
        elif q7 == "Όχι" :
            result = "Συστήνεται άμεση επίσκεψη σε ψυχίατρο, ενημέρωση συγγενών"
            self.store_response("Ερώτηση 7", self.questions["Ερώτηση 7"], q7, metadata, result)
            st.markdown(f'<p style="color:red;">{result}</p>', unsafe_allow_html=True)


    def store_response(self, question_idx, question, answer, metadata, result):
        response = {
            "Timestamp": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            #"Ονοματεπώνυμο Γιατρού": metadata["doctor_name"],
            #"ΑΦΜ Γιατρού": metadata["doctor_id"],
            "Κλινική": metadata["clinic"],
            #"Ονοματεπώνυμο Ασθενή": metadata["patient_name"],
            "Ηλικία Ασθενή": metadata["patient_age"],
            "Γένος Ασθενή": metadata["patient_gender"],
            "ΑΜΚΑ Ασθενή": metadata["patient_vat"],
            "Προέλευση Ασθενή": metadata["patient_arrival"],
            "Λόγος συμπλήρωσης ερωτηματολογίου": metadata["survey_reason"],
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
        left_column, right_column = st.columns([1, 3])  # Adjust the ratio of column width

        # Log results with timestamp
        with right_column :
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