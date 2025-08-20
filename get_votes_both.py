# ─── get_votes.py ───────────────────────────────────────────────────────────
import os, requests, sqlite3, pandas as pd, typing as t
from xml.etree import ElementTree as ET

Chamber = t.Literal["h", "s", "both", "house", "senate"]


# ────────────────────────────────────────────────────────────────────────────
# 1. fetch_bill_votes  – latest House +/- Senate roll-call(s)
# ────────────────────────────────────────────────────────────────────────────
def fetch_bill_votes(
    congress: int,
    bill_type: str,
    bill_number: int,
    chamber: Chamber = "both",
    api_key: str | None = None,
) -> list[dict]:
    """
    Return a list of vote-dicts (one per legislator) for the *latest* roll call
    in each requested chamber.

    chamber = "h" | "s" | "both"  (case-insensitive)
    """
    want_house  = chamber.lower() in ("h", "house", "both")
    want_senate = chamber.lower() in ("s", "senate", "both")

    key = api_key or os.getenv("CONGRESS_API_KEY")
    if not key:
        raise RuntimeError("No API key.  Set CONGRESS_API_KEY or pass api_key.")

    # ── pull all actions (paginate w/ limit+offset) ────────────────────────
    base = "https://api.congress.gov/v3"
    url  = f"{base}/bill/{congress}/{bill_type}/{bill_number}/actions"
    acts, lim, off = [], 250, 0
    while True:
        resp = requests.get(url, params=dict(format="json", api_key=key,
                                             limit=lim, offset=off))
        resp.raise_for_status()
        page = (resp.json().get("data", {}).get("actions")
                or resp.json().get("actions") or [])
        acts.extend(page)
        if len(page) < lim:
            break
        off += lim
    if not acts:
        raise RuntimeError("No actions returned for that bill.")

    # ── isolate roll-calls by chamber, pick the *latest* by rollNumber ────
    house_rolls, sen_rolls = [], []
    for a in acts:
        for rv in a.get("recordedVotes", []):
            if rv.get("chamber") == "House":
                house_rolls.append(a)
            elif rv.get("chamber") == "Senate":
                sen_rolls.append(a)

    latest: list[dict] = []
    if want_house and house_rolls:
        latest.append(max(house_rolls, key=lambda a: a["recordedVotes"][0]["rollNumber"]))
    if want_senate and sen_rolls:
        latest.append(max(sen_rolls,  key=lambda a: a["recordedVotes"][0]["rollNumber"]))
    if not latest:
        raise RuntimeError("Requested chambers have no recorded votes for this bill.")

    # ── parse EVS / LIS XML ────────────────────────────────────────────────
    votes: list[dict] = []
    for rc in latest:
        chamber_tag = rc["recordedVotes"][0]["chamber"]        # "House"/"Senate"
        roll_num    = rc["recordedVotes"][0]["rollNumber"]
        evs_url     = rc["recordedVotes"][0]["url"]
        root        = ET.fromstring(requests.get(evs_url).content)

        if chamber_tag == "House":
            # House format: <recorded-vote><legislator …>text</legislator><vote>Yea</vote>
            for rv in root.findall(".//recorded-vote"):
                leg = rv.find("legislator"); pos = rv.find("vote")
                if leg is None or pos is None:
                    continue
                votes.append({
                    "congress": congress, "bill_type": bill_type, "bill_number": bill_number,
                    "chamber": chamber_tag, "roll_number": roll_num,
                    "member_id": leg.attrib.get("name-id"),
                    "name":      (leg.text or "").strip(),
                    "state":     leg.attrib.get("state"),
                    "party":     leg.attrib.get("party"),
                    "role":      "Rep",
                    "vote_position": pos.text.strip(),
                })
        else:
            # ── Senate XML ───────────────────────────────────────────
            # Any <member …> element is in a default namespace, so we
            # iterate over *all* elements and pick the ones whose tag
            # local-name is "member" (handles namespaces transparently).
            for mem in (elem for elem in root.iter()
                        if elem.tag.split('}')[-1] == "member"):

                member_id = (mem.attrib.get("id")
                             or mem.attrib.get("member_id")
                             or mem.attrib.get("lis_member_id")
                             or mem.attrib.get("name-id")
                             or mem.attrib.get("name_id"))
                if member_id is None:
                    continue  # skip unusable rows

                vote_pos = (mem.attrib.get("vote_cast")
                            or mem.attrib.get("vote")
                            or mem.findtext(".//vote_cast", default=""))

                full_name = (mem.text or "").strip() or mem.attrib.get("full_name") or " ".join(
                    p for p in [mem.attrib.get("first_name"),
                                mem.attrib.get("middle_name"),
                                mem.attrib.get("last_name"),
                                mem.attrib.get("suffix")] if p)

                votes.append({
                    "congress": congress, "bill_type": bill_type, "bill_number": bill_number,
                    "chamber": chamber_tag, "roll_number": roll_number,
                    "member_id": member_id,
                    "name": full_name,
                    "state": mem.attrib.get("state"),
                    "party": mem.attrib.get("party"),
                    "role": "Sen",
                    "vote_position": vote_pos,
                })

    return votes


# ────────────────────────────────────────────────────────────────────────────
# 2. fetch_and_store_batch  – writes vote rows to SQLite
# ────────────────────────────────────────────────────────────────────────────
def fetch_and_store_batch(
    bills: list[tuple[int, str, int]],
    db_path: str = "votes.db",
    chamber: Chamber = "both",
    api_key: str | None = None,
) -> None:
    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS bill_votes (
        congress INTEGER, bill_type TEXT, bill_number TEXT,
        chamber  TEXT,  roll_number INTEGER,
        member_id TEXT, name TEXT, state TEXT, party TEXT, role TEXT,
        vote_position TEXT,
        PRIMARY KEY (congress, bill_type, bill_number, chamber, member_id)
    )""")

    for cong, bt, num in bills:
        rows = fetch_bill_votes(cong, bt, num, chamber=chamber, api_key=api_key)
        for r in rows:
            cur.execute("""INSERT OR REPLACE INTO bill_votes
                           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                        (r["congress"], r["bill_type"], r["bill_number"],
                         r["chamber"],  r["roll_number"],
                         r["member_id"], r["name"], r["state"],
                         r["party"],    r["role"], r["vote_position"]))
        conn.commit()
    conn.close()


# ────────────────────────────────────────────────────────────────────────────
# 3. build_vote_matrix  – pivot to Member × Bill table
# ────────────────────────────────────────────────────────────────────────────
def build_vote_matrix(
    bills: list[tuple[int, str, int]],
    chamber: Chamber = "both",
    db_path: str = "votes.db"
) -> pd.DataFrame:
    """Return a DataFrame: meta-cols + one column per bill_id."""
    conn = sqlite3.connect(db_path)
    df   = pd.read_sql_query("SELECT * FROM bill_votes", conn)
    conn.close()

    # keep requested bills
    wanted = pd.DataFrame(bills, columns=["congress","bill_type","bill_number"])
    wanted["bill_number"] = wanted["bill_number"].astype(str)
    df = df.merge(wanted, on=["congress","bill_type","bill_number"], how="inner")

    # filter chamber
    chc = chamber.lower()
    if chc in ("h","house"):
        df = df[df["chamber"] == "House"]
    elif chc in ("s","senate"):
        df = df[df["chamber"] == "Senate"]

    # drop rows with missing ID (just in case)
    df = df[df["member_id"].notna()]

    # human-readable bill label
    df["bill_id"] = df["bill_type"].str.upper() + "." + df["bill_number"]

    # ensure uniqueness, keep latest roll if duplicates
    df = (df.sort_values("roll_number")
            .drop_duplicates(["member_id", "bill_id"], keep="last"))

    meta  = (df[["member_id","name","state","party"]]
             .drop_duplicates()
             .set_index("member_id"))

    pivot = df.pivot_table(index="member_id",
                           columns="bill_id",
                           values="vote_position",
                           aggfunc="first")

    return meta.join(pivot)
