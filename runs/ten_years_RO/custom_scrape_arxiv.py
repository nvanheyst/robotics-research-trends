import arxiv
import pandas as pd
import datetime
import time
from pathlib import Path

def get_papers_by_category(category, years, max_results=50000):
    
    # The date variables were removed as the arxiv library handles date filtering
    # via sort_by and max_results, and the variables were not used.
    search_query = f"cat:{category}"
    
    print(f"Searching for papers in category '{category}' from the last {years} years...")
    
    client = arxiv.Client(
        page_size=100,
        delay_seconds=5.0, # Increased delay to be more polite to the API
        num_retries=10 # Increased retries to handle transient errors
    )

    search = arxiv.Search(
        query=search_query,
        max_results=max_results,
        sort_by=arxiv.SortCriterion.SubmittedDate,
        sort_order=arxiv.SortOrder.Descending
    )

    all_papers = []
    
    progress_counter = 0
    empty_page_count = 0
    max_empty_pages = 5 # Increased max retries for empty pages

    try:
        print("Fetching the first batch of papers...")
        for result in client.results(search):
            paper_data = {
                "title": result.title.strip(),
                "summary": result.summary.strip(),
                "authors": ", ".join(author.name for author in result.authors),
                "published": result.published,
                "url": result.entry_id,
                "pdf_url": result.pdf_url,
                "categories": ", ".join(result.categories),
            }
            all_papers.append(paper_data)
            time.sleep(0.1) # Reduced sleep between papers to speed up the process
            progress_counter += 1
            if progress_counter % 20 == 0:
                print(f"Scraped {progress_counter} papers so far...")
    except arxiv.UnexpectedEmptyPageError as e:
        empty_page_count += 1
        if empty_page_count <= max_empty_pages:
            print(f"Encountered an empty page. Retrying attempt {empty_page_count} of {max_empty_pages}. Waiting 10 seconds...")
            time.sleep(10) # Wait for 10 seconds before trying the next page
        else:
            print(f"Encountered too many consecutive empty pages. Stopping the scrape.")
            raise e
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        print("Stopping the scrape. The papers retrieved so far will be saved.")
        
    return all_papers

def main():
    years = 1
    category = "cs.RO"
    
    all_results = get_papers_by_category(category, years)
    
    df = pd.DataFrame(all_results)
    
    if not df.empty:
        df.drop_duplicates(subset=["title", "published"], inplace=True)
        
        run_dir = Path("runs")
        run_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{category.replace('.', '_')}_papers_last_{years}_years.csv"
        file_path = run_dir / filename
        df.to_csv(file_path, index=False)
        print(f"\nSuccessfully scraped {len(df)} unique papers and saved to '{file_path}'.")
    else:
        print("\nNo papers were found for the specified criteria.")

if __name__ == "__main__":
    main()
