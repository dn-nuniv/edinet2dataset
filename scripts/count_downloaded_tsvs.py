import os
import glob
import csv
from tqdm import tqdm

TARGET_DIR = r"c:\dev\edinet2dataset\edinet_corpus\annual"
OUTPUT_FILE = r"c:\dev\edinet2dataset\download_status.csv"

def main():
    if not os.path.exists(TARGET_DIR):
        print(f"Directory not found: {TARGET_DIR}")
        return

    # Get list of company directories (E00000...)
    company_dirs = [d for d in os.listdir(TARGET_DIR) if os.path.isdir(os.path.join(TARGET_DIR, d))]
    company_dirs.sort()

    print(f"Found {len(company_dirs)} company directories. Scanning contents...")

    results = []

    for company_dir in tqdm(company_dirs):
        full_path = os.path.join(TARGET_DIR, company_dir)
        
        # Find all .tsv files in the company directory
        # glob pattern: path/*.tsv
        tsv_files = glob.glob(os.path.join(full_path, "*.tsv"))
        
        # Extract DocIDs from filenames (S100XXXX.tsv -> S100XXXX)
        doc_ids = [os.path.splitext(os.path.basename(f))[0] for f in tsv_files]
        doc_ids.sort()

        results.append({
            "edinet_code": company_dir,
            "tsv_count": len(tsv_files),
            "doc_ids": ",".join(doc_ids)
        })

    # Write to CSV
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["edinet_code", "tsv_count", "doc_ids"])
        writer.writeheader()
        writer.writerows(results)

    print(f"Done. Saved report to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
