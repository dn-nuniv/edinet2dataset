import os
import json
import glob
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

DOC_DIR = r"c:\dev\edinet2dataset\edinet_corpus\annual"

def process_file(jp):
    try:
        with open(jp, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        p_start = data.get('periodStart')
        p_end = data.get('periodEnd')
        s_date = data.get('submitDateTime')
        
        res = {}
        if p_start:
            try:
                res['p_start'] = datetime.strptime(p_start, "%Y-%m-%d").date()
            except ValueError:
                pass
        
        if p_end:
            try:
                res['p_end'] = datetime.strptime(p_end, "%Y-%m-%d").date()
            except ValueError:
                pass

        if s_date:
            try:
                res['s_date'] = datetime.strptime(s_date, "%Y-%m-%d %H:%M").date()
            except ValueError:
                pass
        return res
    except Exception:
        return None

def main():
    if not os.path.exists(DOC_DIR):
        print(f"Directory not found: {DOC_DIR}")
        return

    json_files = glob.glob(os.path.join(DOC_DIR, "**", "*.json"), recursive=True)
    
    if not json_files:
        print("No JSON files found.")
        return

    print(f"Found {len(json_files)} JSON files. Scanning dates with parallel workers...")

    min_period_start = None
    max_period_end = None
    min_submit_date = None
    max_submit_date = None

    count = 0
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(process_file, jp) for jp in json_files]
        for future in as_completed(futures):
            res = future.result()
            if res:
                if 'p_start' in res:
                    dt = res['p_start']
                    if min_period_start is None or dt < min_period_start:
                        min_period_start = dt
                if 'p_end' in res:
                    dt = res['p_end']
                    if max_period_end is None or dt > max_period_end:
                        max_period_end = dt
                if 's_date' in res:
                    dt = res['s_date']
                    if min_submit_date is None or dt < min_submit_date:
                        min_submit_date = dt
                    if max_submit_date is None or dt > max_submit_date:
                        max_submit_date = dt
            
            count += 1
            if count % 2000 == 0:
                print(f"Processed {count} files...")

    print("-" * 40)
    print(f"Total processed files: {count}")
    print(f"Period Range: {min_period_start} to {max_period_end}")
    print(f"Submit Date Range: {min_submit_date} to {max_submit_date}")
    print("-" * 40)

if __name__ == "__main__":
    main()
