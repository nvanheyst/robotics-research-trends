import requests
import xml.etree.ElementTree as ET
import pandas as pd
import datetime
import time
from pathlib import Path

ARXIV_API_URL = "http://export.arxiv.org/api/query"

def query_arxiv_paginated(search_query, max_papers, batch_size=100):
    entries = []
    
    for start in range(0, max_papers, batch_size):
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
            print(f"Failed on batch with status {response.status_code}")
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
                "url": entry.find("{http://www.w3.org/2005/Atom}id").text,
                "pdf_url": entry.find("{http://www.w3.org/2005/Atom}id").text.replace("abs", "pdf") + ".pdf",
                "categories": ", ".join([cat.attrib['term'] for cat in entry.findall("{http://www.w3.org/2005/Atom}category")]),
            }
            entries.append(data)
        
        time.sleep(3)
        
    return entries

def main():
    today = datetime.datetime.now()
    years = 10
    category = "cs.RO"
    start_date = today - datetime.timedelta(days=years * 365)
    date_filter = f'submittedDate:[{start_date.strftime("%Y%m%d0000")} TO {today.strftime("%Y%m%d2359")}]'
    
    query_string = f'cat:{category} AND {date_filter}'
    
    all_results = query_arxiv_paginated(query_string, max_papers=50000)
    
    df = pd.DataFrame(all_results)
    
    if not df.empty:
        df.drop_duplicates(subset=["title", "published"], inplace=True)
        
        run_dir = Path("runs")
        run_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{category.replace('.', '_')}_papers_last_{years}_years_requests.csv"
        file_path = run_dir / filename
        df.to_csv(file_path, index=False)
        print(f"\nSuccessfully scraped {len(df)} unique papers and saved to '{file_path}'.")
    else:
        print("\nNo papers were found for the specified criteria.")

if __name__ == "__main__":
    main()
