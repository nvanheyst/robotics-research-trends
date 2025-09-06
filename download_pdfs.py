#!/usr/bin/env python3

import pandas as pd
import requests
import os
import argparse


def download_pdfs(csv_file: str, output_dir: str):
    """
    Downloads all PDFs listed in the 'pdf_url' column of a CSV file.
    """
    try:
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file)
        print(f"Successfully read {csv_file}")
    except FileNotFoundError:
        print(f"Error: The file '{csv_file}' was not found.")
        return

    # Create the output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")

    # Iterate through each row and download the PDF
    for index, row in df.iterrows():
        pdf_url = row['pdf_url']
        
        # Create a clean filename from the paper's title
        title = row['title'].replace('/', '-').strip()
        filename = f"{title}.pdf"
        filepath = os.path.join(output_dir, filename)

        # Download the file
        try:
            print(f"Downloading {index + 1}/{len(df)}: {filename}")
            response = requests.get(pdf_url, stream=True)
            response.raise_for_status() # Raise an exception for bad status codes

            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            print(f"  -> Saved to {filepath}")

        except requests.exceptions.RequestException as e:
            print(f"  -> Failed to download '{filename}': {e}")
        except OSError as e:
            print(f"  -> Failed to save file '{filename}': {e}")


if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(description="Download PDFs from a CSV file.")
    
    # Add the file path argument
    parser.add_argument("file_path", help="The relative path to the input CSV file.")
    
    # Parse the arguments
    args = parser.parse_args()

    # Get the directory of the input file
    input_dir = os.path.dirname(args.file_path)
    
    # Get the base name of the input file without the extension
    base_name = os.path.splitext(os.path.basename(args.file_path))[0]
    
    # Create the new output folder path
    output_folder = os.path.join(input_dir, f"{base_name}_PDFs")
    
    # Call the download function with the user-provided path
    download_pdfs(args.file_path, output_folder)
    print("\nDownload process finished.")