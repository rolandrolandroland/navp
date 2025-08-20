import os
import requests
from xml.etree import ElementTree as ET
import sqlite3
import pandas as pd


import os
import requests
from xml.etree import ElementTree as ET
from typing import List, Dict

def fetch_bill_votes_all_chambers(
    congress: int,
    bill_type: str,
    bill_number: int,
    api_key: str | None = None
) -> List[Dict]:
    """
    Return every legislator’s vote on the *latest* House roll-call **and**
    the *latest* Senate roll-call for a given bill.

    Parameters
    ----------
    congress   : int
        Congress number, e.g. 118.
    bill_type  : str
        "hr" (House bill) or "s" (Senate bill).  Lower-case is fine.
    bill_number: int
        The bill’s numeric identifier, e.g. 8034.
    api_key    : str | None
        Your Congress.gov key.  If None we look for $CONGRESS_API_KEY.

    Returns
    -------
    votes : list[dict]
        One dict per *individual* vote, combined across chambers, with:
            member_id      : unique ID (e.g. "A000370")
            name           : legislator’s name
            state, party   : metadata
            chamber        : "House" or "Senate"
            bill_id        : e.g. "HR.8034"
            roll_number    : int (to keep House & Senate votes distinct)
            vote_position  : "Yea" / "Nay" / "Present" / …
    """
    # ------------------------------------------------------------------
    # 0) House-keeping
    # ------------------------------------------------------------------
    key = api_key or os.getenv("CONGRESS_API_KEY")
    if not key:
        raise RuntimeError("Set CONGRESS_API_KEY or pass api_key.")

    bill_id = f"{bill_type.upper()}.{bill_number}"

    # ------------------------------------------------------------------
    # 1) Pull the *entire* actions list (paginate w/ limit+offset)
    # ------------------------------------------------------------------
    base_url   = "https://api.congress.gov/v3"
    actions_ep = f"{base_url}/bill/{congress}/{bill_type}/{bill_number}/actions"

    all_actions, limit, offset = [], 250, 0
    while True:
        resp = requests.get(
            actions_ep,
            params=dict(format="json", api_key=key, limit=limit, offset=offset)
        )
        resp.raise_for_status()
        payload = resp.json()

        page_actions = (
            payload.get("data", {}).get("actions")
            or payload.get("actions")
            or []
        )
        all_actions.extend(page_actions)

        if len(page_actions) < limit:
            break                       # no more pages
        offset += limit                 # fetch next slice

    if not all_actions:
        raise RuntimeError(f"No actions found for {bill_id}.")

    # ------------------------------------------------------------------
    # 2) Separate House + Senate roll-call actions.
    #    We DON’T look at actionCode anymore; instead we check the
    #    recordedVotes[].chamber value.
    # ------------------------------------------------------------------
    house_rolls  = []
    senate_rolls = []
    for act in all_actions:
        for rv in act.get("recordedVotes", []):
            if rv.get("chamber") == "House":
                house_rolls.append(act)
            elif rv.get("chamber") == "Senate":
                senate_rolls.append(act)

    if not house_rolls and not senate_rolls:
        raise RuntimeError(f"No roll-calls at all for {bill_id}.")

    # Helper: pick the roll-call with the HIGHEST rollNumber (latest)
    def pick_latest(rolls):
        return max(
            rolls,
            key=lambda a: a["recordedVotes"][0]["rollNumber"]
        )

    latest_house = pick_latest(house_rolls)  if house_rolls  else None
    latest_sen   = pick_latest(senate_rolls) if senate_rolls else None

    # ------------------------------------------------------------------
    # 3) For whichever chamber(s) we found, pull the EVS XML & parse votes
    # ------------------------------------------------------------------
    def parse_evs_xml(evs_url, chamber) -> List[Dict]:
        xml = requests.get(evs_url)
        xml.raise_for_status()
        root = ET.fromstring(xml.content)

        vote_rows = []
        for rv in root.findall(".//recorded-vote"):
            leg = rv.find("legislator")
            vote_elem = rv.find("vote")
            if leg is None or vote_elem is None:
                continue
            vote_rows.append({
                "member_id":    leg.attrib.get("name-id"),
                "name":         leg.text.strip(),
                "state":        leg.attrib.get("state"),
                "party":        leg.attrib.get("party"),
                "chamber":      chamber,
                "bill_id":      bill_id,
                # pull rollNumber so we can tell House 217 vs Senate 114 apart
                "roll_number":  int(evs_url.split("roll")[-1].split(".")[0]),
                "vote_position": vote_elem.text.strip(),
            })
        return vote_rows

    all_votes = []
    for rc, chamber in [(latest_house, "House"), (latest_sen, "Senate")]:
        if rc is None:
            continue
        evs_url = rc["recordedVotes"][0]["url"]
        all_votes.extend(parse_evs_xml(evs_url, chamber))

    return all_votes

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
        votes = fetch_bill_votes_all_chambers(congress, bill_type, bill_number, api_key)
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

    vote_matrix = df.pivot(index="member_id", columns="bill_id", values="vote_position")
    rep_info = (
        df[["name", "state", "party", "member_id"]]
        .drop_duplicates()
        .set_index("member_id"))
    vote_matrix = rep_info.join(vote_matrix)
    return vote_matrix

def compute_member_total_scores(
    vote_matrix: pd.DataFrame,
    scoring_rules: dict[str, dict[str, float]],
    default_score: float = 0.0
) -> pd.Series:
    """
    Given a vote-string matrix and per-bill scoring rules, compute each
    member’s total score.

    Parameters
    ----------
    vote_matrix : pd.DataFrame
        Rows are member_id, columns are bill_id, values are vote strings
        (“Yea”, “Nay”, “Present”, or NaN).
    scoring_rules : dict of dict
        Per-bill maps of vote→score, e.g.
            {
              "HR.8034": {"Yea": -1, "Nay": 1,  "Present": 0.5},
              "HR.6090": {"Yea":  2,  "Nay": -2      }
            }
    default_score : float
        Score to assign if a vote is missing or not in the bill’s map.

    Returns
    -------
    pd.Series
        Indexed by member_id, each value is the sum of that member’s
        scores across all bills.
    """
    # 1) Build a DataFrame of numeric scores per (member, bill)
    score_df = pd.DataFrame(index=vote_matrix.index, columns=vote_matrix.columns)

    for bill_id in vote_matrix.columns:
        rule = scoring_rules.get(bill_id, {})
        # map strings → numeric, fill NaN or unmapped with default_score
        score_df[bill_id] = vote_matrix[bill_id].map(rule).fillna(default_score)

        # 2) Sum across all bills (axis=1) to get each member’s total
    total_scores = score_df.sum(axis=1)
    vote_matrix["total_score"] = total_scores
    vote_matrix = vote_matrix.sort_values(by="total_score", ascending=False)

    vote_matrix["rank"] = (
        vote_matrix["total_score"]
          .rank(ascending=False,       # highest score → rank 1
                method="average")        # dense: no gaps in ranking
          .astype(int)                # make them ints rather than floats
    )
    vote_matrix["percent"] = vote_matrix["rank"] / vote_matrix.shape[0] *100
    vote_matrix["percent"] = vote_matrix["percent"].round(1)

    # 1) Pop it out (this returns the Series and removes the column)
    ts = vote_matrix.pop("total_score")
    # 2) Find where "party" lives
    pos = vote_matrix.columns.get_loc("party") + 1
    # 3) Insert total_score back in at that position
    vote_matrix.insert(pos, "total_score", ts)

    # 1) Pop it out (this returns the Series and removes the column)
    ts = vote_matrix.pop("rank")
    # 2) Find where "party" lives
    pos = vote_matrix.columns.get_loc("total_score") + 1
    # 3) Insert total_score back in at that position
    vote_matrix.insert(pos, "rank", ts)

    # 1) Pop it out (this returns the Series and removes the column)
    ts = vote_matrix.pop("percent")
    # 2) Find where "party" lives
    pos = vote_matrix.columns.get_loc("rank") + 1
    # 3) Insert total_score back in at that position
    vote_matrix.insert(pos, "percent", ts)

    vote_matrix = vote_matrix.sort_values(by="total_score", ascending=False)

    return vote_matrix

