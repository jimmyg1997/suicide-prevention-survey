# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
    [*] Description : Utilities Functions for the current automation
    [*] Author      : dgeorgiou3@gmail.com
    [*] Date        : Jan, 2024
    [*] Links       :
"""

import os, ssl, stat, subprocess, sys
import os.path
from itertools import zip_longest
from typing import Dict, Any, List
import pandas as pd

STAT_0o775 = ( stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
             | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP
             | stat.S_IROTH |                stat.S_IXOTH )


# -*-*-*-*-*-*-*-*-*-*-* #
#       FUNCTIONS        #
# -*-*-*-*-*-*-*-*-*-*-* #
def install_certifi():
    openssl_dir, openssl_cafile = os.path.split(
        ssl.get_default_verify_paths().openssl_cafile)

    print(" -- pip install --upgrade certifi")
    subprocess.check_call([sys.executable,
        "-E", "-s", "-m", "pip", "install", "--upgrade", "certifi"])

    import certifi

    # change working directory to the default SSL directory
    os.chdir(openssl_dir)
    relpath_to_certifi_cafile = os.path.relpath(certifi.where())
    print(" -- removing any existing file or link")
    try:
        os.remove(openssl_cafile)
    except FileNotFoundError:
        pass
    print(" -- creating symlink to certifi certificate bundle")
    os.symlink(relpath_to_certifi_cafile, openssl_cafile)
    print(" -- setting permissions")
    os.chmod(openssl_cafile, STAT_0o775)
    print(" -- update complete")

def chunkify(lst, size):
    args = [iter(lst)] * size
    return [[elem for elem in t if elem is not None] for t in zip_longest(*args)]


CLINICS = [
    "",
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

QUESTIONS = [
    {
        "id"      : "q1",
        "id_full" : "Ερώτηση 1",
        "title"   : "Καταθλιπτικό επεισόδιο",
        "text"    : "Είχατε περάσει ποτέ διάστημα πάνω από 10 μέρες που είχατε άσχημη διάθεση, πηγαίνατε με το ζόρι στην δουλειά , είχατε αυπνία η υπνηλία, αισθανόσασταν κουρασμένος, καταθλιπτικός;",
        "options" : ["Ναι", "Όχι"]
    },
    {
        "id"      : "q2",
        "id_full" : "Ερώτηση 2",
        "title"   : "Σκέψεις-Ευχές θανάτου",
        "text"    : "Είχατε τον τελευταίο μήνα σκέψεις ότι δεν αξίζει η ζωή, ότι δεν θέλετε να ζείτε, ή όταν πάτε για ύπνο σκέπτεστε ότι θα ήταν καλύτερα να μην ξυπνήσετε;",
        "options" : ["Ναι", "Όχι"]
    },
    {
        "id"      : "q3",
        "id_full" : "Ερώτηση 3",
        "title"   : "Ιστορικό Αποπειρών Αυτοκτονίας",
        "text"    : " Έχετε κάνει ποτέ κάποια απόπειρα αυτοκτονίας;",
        "options" : ["Ναι", "Όχι"]
    },
    {
        "id"      : "q4",
        "id_full" : "Ερώτηση 4",
        "title"   : "Κληρονομικότητα, Ευαλοτώτητα",
        "text"    : "Υπάρχει κάποιο άτομο στο οικογενειακό σας περιβάλλον που έχει αυτοκτονήσει ή που έχει κάνει απόπειρα αυτοκτονίας;",
        "options" : ["Ναι", "Όχι"]
    },
    {
        "id"      : "q5",
        "id_full" : "Ερώτηση 5",
        "title"   : "Αυτοκτονικές Σκέψεις στο Παρόν",
        "text"    : "Είχατε τον τελευταίο μήνα σκέψεις να αυτοκτονήσετε / να βλάψετε τον εαυτό σας;",
        "options" : ["Ναι", "Όχι"]
    },
    {
        "id"      : "q6",
        "id_full" : "Ερώτηση 6",
        "title"   : "Αυτοκτονικό Πλάνο - Αυτοκτονική Πρόθεση",
        "text"    : "Έχετε σκεφτεί με ποιον τρόπο θα αυτοκτονήσετε;",
        "options" : ["Ναι", "Όχι"]
    },
    {
        "id"      : "q7",
        "id_full" : "Ερώτηση 7",
        "title"   : "Πρόσβαση στον Τρόπο Αυτοκτονίας",
        "text"    : "Έχετε πρόσβαση στον τρόπο αυτοκτονίας που μου λέτε;",
        "options" : ["Ναι", "Όχι"]
    }
]
