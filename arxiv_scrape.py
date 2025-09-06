#!/usr/bin/env python3

import requests
import xml.etree.ElementTree as ET
import pandas as pd
import time
from pathlib import Path
import datetime


ARXIV_API_URL = "http://export.arxiv.org/api/query"

# Calculate the date range
today = datetime.datetime.now()
two_years_ago = today - datetime.timedelta(days=2 * 365)
start_date = two_years_ago.strftime("%Y%m%d%H%M")
end_date = today.strftime("%Y%m%d%H%M")

# The date filter string
date_filter = f"submittedDate:[{start_date} TO {end_date}]"

# runs with keyword combinations of first_list and second_list
# simple is not a combo, just a single query

run = "mobile_manipulator"
simple = ['all:"mobile manipulator" AND {date_filter}']
first_list = ["robot arm", "manipulator", "robotic arm", "end effector"]
second_list = ["mobile base", "mobile robot", "UGV", "AMR", "rover", "AGV"]

# run = "outdoor_robot"
# simple = []
# first_list = ["outdoor", "field", "offroad", "terrain"] 
# second_list = ["robot", "UGV", "AMR", "rover"]

# run = "ros_general"
# simple = []
# first_list = ["ROS", "ROS2"]
# second_list = ["mobile", "robot", "UGV", "AMR"]


def build_queries():

    
    queries = simple #can add a simple query as well such as ['all:"robot operating system"']
    for first in first_list:
        for second in second_list:
            queries.append(f"all:{first} AND all:{second} AND {date_filter}")
    return queries

def query_arxiv_paginated(search_query, batch_size=100, max_batches=10):
    entries = []
    for i in range(max_batches):
        start = i * batch_size
        print(f"Querying: [{search_query}] | Results {start}-{start + batch_size}")
        params = {
            "search_query": search_query,
            "start": start,
            "max_results": batch_size,
            "sortBy": "submittedDate",
            "sortOrder": "descending"
        }

        response = requests.get(ARXIV_API_URL, params=params)
        if response.status_code != 200:
            print(f"Failed on batch {i} with status {response.status_code}")
            break

        root = ET.fromstring(response.content)
        batch_entries = root.findall("{http://www.w3.org/2005/Atom}entry")
        if not batch_entries:
            break

        for entry in batch_entries:
            data = {
                "title": entry.find("{http://www.w3.org/2005/Atom}title").text.strip(),
                "summary": entry.find("{http://www.w3.org/2005/Atom}summary").text.strip(),
                "authors": ", ".join([
                    author.find("{http://www.w3.org/2005/Atom}name").text
                    for author in entry.findall("{http://www.w3.org/2005/Atom}author")
                ]),
                "published": entry.find("{http://www.w3.org/2005/Atom}published").text,
                "updated": entry.find("{http://www.w3.org/2005/Atom}updated").text,
                "url": entry.find("{http://www.w3.org/2005/Atom}id").text,
                "pdf_url": entry.find("{http://www.w3.org/2005/Atom}id").text.replace("abs", "pdf") + ".pdf",
                "matched_query": search_query
            }
            entries.append(data)

        time.sleep(3)  # slow down the calls
    return entries

# Helper function to check for keywords
def has_any(text, keywords):
    """Checks if any keyword from a list is in a given text (case-insensitive)."""
    text_lower = str(text).lower()
    return any(keyword.lower() in text_lower for keyword in keywords)

# New function to find false positives
def find_false_positives(df: pd.DataFrame, first_list: list, second_list: list) -> pd.DataFrame:
    """
    Identifies and returns papers that do not contain keywords from both lists.
    
    Args:
        df: The DataFrame of papers.
        first_list: The list of keywords for the first group (e.g., ROS).
        second_list: The list of keywords for the second group (e.g., robot).
        
    Returns:
        A new DataFrame containing only the papers that are false positives.
    """
    is_true_positive = df.apply(
        lambda row: (has_any(row['title'], first_list) or has_any(row['summary'], first_list)) and
                    (has_any(row['title'], second_list) or has_any(row['summary'], second_list)),
        axis=1
    )
    
    return df[~is_true_positive]

def main():
    all_queries = build_queries()
    all_results = []

    for q in all_queries:
        entries = query_arxiv_paginated(q, batch_size=100, max_batches=5)
        all_results.extend(entries)

    df = pd.DataFrame(all_results)
    df.drop_duplicates(subset=["title", "published"], inplace=True)
    df["year"] = pd.to_datetime(df["published"]).dt.year
    
    false_positives = find_false_positives(df, first_list, second_list)
    print(f"\nFound {len(false_positives)} potential false positives.")
    
    run_dir = Path(__file__).parent / "runs" / run
    run_dir.mkdir(parents=True, exist_ok=True)
    
    papers_path = run_dir / "arxiv_papers.csv"
    false_positives_path = run_dir / "potential_false_positives.csv"

    df.to_csv(papers_path, index=False)
    false_positives.to_csv(false_positives_path, index=False)

    print(f"\n Saved {len(df)} unique papers to: {run_dir}")

if __name__ == "__main__":
    main()
