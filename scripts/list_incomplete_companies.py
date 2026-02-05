import os
import glob
import json
import csv
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

TARGET_DIR = r"c:\dev\edinet2dataset\edinet_corpus\annual"
OUTPUT_FILE = r"c:\dev\edinet2dataset\incomplete_companies.csv"
THRESHOLD = 10  # 10年分未満を抽出

def process_company(company_path):
    try:
        edinet_code = os.path.basename(company_path)
        tsv_files = glob.glob(os.path.join(company_path, "*.tsv"))
        count = len(tsv_files)
        
        # 10年分以上あればスキップ（高速化のため詳細読み込みしない）
        # ただし、今回は「10年分無い」企業を知りたいので、THRESHOLD未満の場合のみ詳細を取得
        if count >= THRESHOLD:
            return None

        # 会社名を取得するためにJSONを探す
        company_name = "Unknown"
        json_files = glob.glob(os.path.join(company_path, "*.json"))
        if json_files:
            # 最新のものを読むためにソートしてもいいが、どれでも会社名は同じはず
            try:
                with open(json_files[0], 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    company_name = data.get('filerName', 'Unknown')
            except Exception:
                pass
        
        return {
            "edinet_code": edinet_code,
            "company_name": company_name,
            "file_count": count
        }
    except Exception:
        return None

def main():
    if not os.path.exists(TARGET_DIR):
        print(f"Directory not found: {TARGET_DIR}")
        return

    company_dirs = [os.path.join(TARGET_DIR, d) for d in os.listdir(TARGET_DIR) if os.path.isdir(os.path.join(TARGET_DIR, d))]
    print(f"Scanning {len(company_dirs)} companies for those with less than {THRESHOLD} years of data...")

    incomplete_companies = []
    
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(process_company, p) for p in company_dirs]
        
        for future in tqdm(as_completed(futures), total=len(futures)):
            result = future.result()
            if result:
                incomplete_companies.append(result)

    # 結果を保存
    # CSV形式: edinet_code, company_name, count
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["edinet_code", "company_name", "file_count"])
        writer.writeheader()
        
        # ファイル数が少ない順、あるいはコード順にソートして書き込み
        incomplete_companies.sort(key=lambda x: x['edinet_code'])
        writer.writerows(incomplete_companies)

    print(f"Found {len(incomplete_companies)} companies with less than {THRESHOLD} files.")
    print(f"Saved list to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
