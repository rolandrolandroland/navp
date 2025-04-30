import os
import requests
from xml.etree import ElementTree as ET
import sqlite3
import pandas as pd


def fetch_house_bill_votes(congress, bill_type, bill_number, api_key=None):
    """
    Fetches the most recent roll-call vote for one House/Senate bill.
    Returns a list of dicts: one dict per legislator’s vote.
    """
    key = api_key or os.getenv("CONGRESS_API_KEY")
    if not key:
        raise RuntimeError("Set CONGRESS_API_KEY or pass api_key explicitly.")

    # 1) Get the bill’s actions
    base_url   = "https://api.congress.gov/v3"
    actions_ep = f"{base_url}/bill/{congress}/{bill_type}/{bill_number}/actions"
    resp = requests.get(actions_ep, params={"format": "json", "api_key": key})
    resp.raise_for_status()
    payload = resp.json()
    actions = payload.get("data", {}).get("actions") or payload.get("actions") or []
    if not actions:
        raise RuntimeError(f"No actions for {bill_type.upper()}.{bill_number} in {congress}.")

    # 2) Find the most recent roll-call
    roll_calls = [a for a in actions if "Roll no." in a.get("text", "")]
    if not roll_calls:
        raise RuntimeError(f"No roll-call found for {bill_type.upper()}.{bill_number}.")
    last_vote = roll_calls[-1]

    # 3) Extract Clerk EVS XML URL
    recorded = last_vote.get("recordedVotes", [])
    if recorded and recorded[0].get("url"):
        evs_url = recorded[0]["url"]
    else:
        evs_path = last_vote.get("link") or last_vote.get("relatedLink")
        if not evs_path:
            raise RuntimeError("No EVS URL on roll-call action.")
        evs_url = f"https://clerk.house.gov{evs_path}"

    # 4) Download & parse the XML
    xml = requests.get(evs_url)
    xml.raise_for_status()
    root = ET.fromstring(xml.content)

    # 5) Build vote records
    votes = []
    for rv in root.findall(".//recorded-vote"):
        leg = rv.find("legislator")
        vote_elem = rv.find("vote")
        if leg is None or vote_elem is None:
            continue
        votes.append({
            "congress":      congress,
            "bill_type":     bill_type,
            "bill_number":   bill_number,
            "member_id":     leg.attrib.get("name-id"),
            "name":          leg.text.strip(),
            "state":         leg.attrib.get("state"),
            "party":         leg.attrib.get("party"),
            "role":          leg.attrib.get("role"),
            "vote_position": vote_elem.text.strip()
        })
    return votes


def fetch_and_store_batch(bills, db_path="votes.db", api_key=None):
    """
    Given a list of (congress, bill_type, bill_number) tuples,
    fetch each bill’s votes and store them in an SQLite table.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS bill_votes (
        congress       INTEGER,
        bill_type      TEXT,
        bill_number    TEXT,
        member_id      TEXT,
        name           TEXT,
        state          TEXT,
        party          TEXT,
        role           TEXT,
        vote_position  TEXT,
        PRIMARY KEY(congress, bill_type, bill_number, member_id)
    )""")

    for congress, bill_type, bill_number in bills:
        votes = fetch_house_bill_votes(congress, bill_type, bill_number, api_key)
        for v in votes:
            c.execute("""
            INSERT OR IGNORE INTO bill_votes (
                congress, bill_type, bill_number,
                member_id, name, state, party, role, vote_position
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
                congress, bill_type, bill_number,
                v["member_id"], v["name"], v["state"],
                v["party"], v["role"], v["vote_position"]
            ))
        conn.commit()
    conn.close()


def build_vote_matrix(bills, db_path="votes.db"):
    """
    Build a matrix (DataFrame) of vote positions: rows=Members, columns=Bills.
    """
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM bill_votes", conn)
    conn.close()

    bills_df = pd.DataFrame(bills, columns=["congress","bill_type","bill_number"])
    bills_df["bill_number"] = bills_df["bill_number"].astype(str)
    df = df.merge(bills_df, on=["congress","bill_type","bill_number"], how="inner")
    df["bill_id"] = df["bill_type"].str.upper() + "." + df["bill_number"]

    vote_matrix = df.pivot(index="name", columns="bill_id", values="vote_position")
    return vote_matrix