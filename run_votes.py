#!/usr/bin/env python3
import argparse
from get_votes import fetch_and_store_batch, build_vote_matrix


def main():
    parser = argparse.ArgumentParser(
        description="Fetch roll-call votes and build a vote matrix."
    )
    parser.add_argument(
        "--bills", nargs='+', required=True,
        help="CONGRESS:TYPE:NUMBER, e.g. 118:hr:8034"
    )
    parser.add_argument(
        "--db", default="votes.db",
        help="Path to SQLite database file"
    )
    args = parser.parse_args()

    bills = []
    for b in args.bills:
        cong, btype, num = b.split(":")
        bills.append((int(cong), btype, int(num)))

    fetch_and_store_batch(bills, db_path=args.db)
    matrix = build_vote_matrix(bills, db_path=args.db)
    print(matrix)


if __name__ == "__main__":
    main()