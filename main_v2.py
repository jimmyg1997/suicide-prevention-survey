#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
    [*] Description : Suicid Prevention Survey
    [*] Author      : Dimitrios Georgiou (dgeorgiou3@gmail.com)
    [*] Date        :
    [*] Links       :
"""
# -*-*-*-*-*-*-*-*-*-*-* #
#     Basic Modules      #
# -*-*-*-*-*-*-*-*-*-*-* #
import os, json, time, glob,argparse, re
import numpy     as np
import functools as ft
import pandas    as pd
import datetime  as dt
import streamlit as st
import threading
from urllib.parse    import unquote
from bs4             import BeautifulSoup
from tqdm            import tqdm
from typing          import Dict, Any, List
from IPython.display import display
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, render_template, request, jsonify
from flask import g


# -*-*-*-*-*-*-*-*-*-*-* #
#     Project Modules    #
# -*-*-*-*-*-*-*-*-*-*-* #
# framework
from lib.framework.markI import *

# handlers
from lib.handlers.data_handling   import DataLoader
from lib.handlers.survey_handling import SurveyHandler

# modules
from lib.modules.API_google import (
    GoogleAPI, GoogleSheetsAPI, GoogleEmailAPI, GoogleDocsAPI, GoogleDriveAPI
)
from lib.modules.API_dropbox import DropboxAPI

# helpers
from lib.helpers.utils import * 


# Initialize Flask app
app = Flask(__name__)




class Controller():
    def __init__(self, mk1 : MkI) :
        self.mk1  = mk1
        self.__null_values = [
            None, np.nan, "", "#N/A", "null", "nan", "NaN"
        ]


    def run_initialization(self):
        # Initializing Modules
        self.google_api = GoogleAPI(
            mk1 = self.mk1
        )
        self.google_sheets_api = GoogleSheetsAPI(
            mk1        = self.mk1,
            google_api = self.google_api
        )
        self.google_email_api  = GoogleEmailAPI(
            mk1        = self.mk1,
            google_api = self.google_api
        )
        self.google_drive_api  = GoogleDriveAPI(
            mk1        = self.mk1,
            google_api = self.google_api
        )
        self.google_docs_api = GoogleDocsAPI(
            mk1        = self.mk1,
            google_api = self.google_api
        )
        
        # Initializing Handlers
        self.data_loader = DataLoader(
            mk1               = self.mk1,
            google_sheets_api = self.google_sheets_api
        )

        self.survey_handler = SurveyHandler(
            mk1         = self.mk1,
            data_loader = self.data_loader
        )

    def _refresh_session(self):
        self.run_initialization()


    def _refresh_tokens(self):
        ## _______________ *** Configuration (objects) *** _______________ #
        self.dropbox_api = DropboxAPI(
            mk1 = self.mk1
        )
        ## _______________ *** Configuration (attributes) *** _______________ #
        google_oauth_accessed_dbx_path   = self.mk1.config.get("dropbox", "google_oauth_accessed_dbx_path")
        google_oauth_local_path          = self.mk1.config.get("api_google", "token_file_path")
        google_oauth_accessed_local_path = f"{google_oauth_local_path.rsplit('.', 1)[0]}_accessed.json"

        ## _____________________________________________________________________________________________________________________ ##
        self.dropbox_api.download_file(
            dropbox_path = google_oauth_accessed_dbx_path,
            local_path   = google_oauth_accessed_local_path
        )
        return 


    ## -------------------------------------------------------------------------------------------------------------------------------------------- ##
    def run(self):
        # initialize servicess
        self._refresh_tokens() # Retrieve google oauth accessed token through dropbox
        self.run_initialization()
        self.mk1.logging.logger.info(f"(Controller.run) All services initilalized")


def get_controller():
    if 'controller' not in g:
        g.controller = Controller(MkI.get_instance(_logging=True))
        g.controller.run()
    return g.controller
        
# Flask routes
@app.route('/')
def index():
    controller = get_controller()
    return render_template('survey.html', questions=QUESTIONS, clinics=CLINICS)


@app.route("/submit", methods=["POST"])
def submit():
    
    ## _______________ *** Configuration (attributes) *** _______________ #
    controller = get_controller()
    sheets_reporter_id                 = controller.mk1.config.get("google_sheets","reporter_id")
    sheets_reporter_tab_survey_results = controller.mk1.config.get("google_sheets","reporter_tab_survey_results_v2")
    
    ## _____________________________________________________________________________________________________________________ ##
    # Collect metadata and responses
    metadata = {
        "timestamp"       : dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "clinic"          : request.form.get("clinic"),
        "patient_age"     : request.form.get("patient_age"),
        "patient_gender"  : request.form.get("patient_gender"),
        "patient_arrival" : request.form.get("patient_arrival"),
        "patient_vat"     : request.form.get("patient_vat"),
        "survey_reason"   : request.form.get("survey_reason")
    }
    
    responses = []
    for question in QUESTIONS:
        answer = request.form.get(question["id"])
        result = request.form.get(f"{question['id']}_message")  # Retrieve the message visibility

        
        if answer:
            responses.append({
                **metadata,
                "question_id"    : question["id_full"],
                "question_text"  : f"{question['title']}:{question['text']}",
                "answer"         : answer,
                "result"         : result
            })
    
    responses_df = pd.DataFrame(responses)
    
    
    # Push to google sheets reporter
    controller.data_loader.append_data_to_google_sheets(
        df                     = responses_df,
        spreadsheet_id         = sheets_reporter_id,
        spreadsheet_range_name = sheets_reporter_tab_survey_results,
    )

    return render_template("result.html")

if __name__ == "__main__":
    # Heroku sets the PORT environment variable automatically
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 8600)))