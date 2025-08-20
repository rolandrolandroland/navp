import os
import requests
from xml.etree import ElementTree as ET
import sqlite3
import pandas as pd

from get_votes import fetch_and_store_batch, build_vote_matrix, compute_member_total_scores


bills = [
    (118, "hr", 8034),
    (118, "hr", 6090),
    (118, "hr", 340),
    (118, "hr", 6126),
    (118, "hr", 2670),
    (118, "hr", 7217),
    (118, "hr", 2882),
    (118, "hr", 5917),
   # (118, "hr", 815),
    (118, "hr", 8038),
    (118, "hr", 8369),
    (118, "hr", 8070),
    (118, "hr", 8771),
    (118, "hr", 8774),
    (118, "hr", 5961),
]


bills = [
    (118, "hr", 8034),
    (118, "hr", 6090),
    (118, "hr", 340),
    (118, "hr", 6126),
    (118, "hr", 2670),
    (118, "hr", 7217),
    (118, "hr", 2882),
    (118, "hr", 5917),
   # (118, "hr", 815),
    (118, "hr", 8038),
    (118, "hr", 8369),
    (118, "hr", 8070),
    (118, "hr", 8771),
    (118, "hr", 8774),
    (118, "hr", 5961),
]
# 2) Fetch & store into votes.db (will create/append the SQLite file)
fetch_and_store_batch(bills, db_path="votes.db")

# 3) Build the vote‐matrix DataFrame
vote_matrix = build_vote_matrix(bills, db_path="votes.db")

# your raw weights list: (congress, bill_type, bill_number, yea, present, nay, no_vote)
weights = [
    (118, "hr", 8034, -1, 0,  1, 0),
    (118, "hr", 6090, -1, 0,  1, 0),
    (118, "hr", 340,  -1, 0,  1, 0),
    (118, "hr", 6126, -1, 0,  1, 0),
    (118, "hr", 2670, -1, 0,  1, 0),
    (118, "hr", 7217, -1, 0,  1, 0),
    (118, "hr", 2882, -1, 0,  1, 0),
    (118, "hr", 5917, -1, 0,  1, 0),
    (118, "hr", 8038, -1, 0,  1, 0),
    (118, "hr", 8369, -1, 0,  1, 0),
    (118, "hr", 8070, -1, 0,  1, 0),
    (118, "hr", 8771, -1, 0,  1, 0),
    (118, "hr", 8774, -1, 0,  1, 0),
    (118, "hr", 5961, -1, 0,  1, 0),
]

# Build the rules dict
rules = {}
for _, bill_type, bill_number, yea_w, pres_w, nay_w, no_w in weights:
    bill_id = f"{bill_type.upper()}.{bill_number}"
    rules[bill_id] = {
        "Yea":        yea_w,
        "Nay":        nay_w,
        "Present":    pres_w,
        "Not Voting": no_w,
    }

# Example output:
# {
#   "HR.8034": {"Yea": -1, "Nay": 1, "Present": 0, "Not Voting": 0},
#    …
# }

# Then pass `rules` into your scorer:
scores = compute_member_total_scores(vote_matrix, rules, default_score=0.0)
print(scores.head())

#ny_scores = scores[scores["state"] == "NY"]
#print(ny_scores)
