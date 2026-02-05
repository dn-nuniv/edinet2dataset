import csv
import os
import json
import glob
from tqdm import tqdm

TOSHO_CSV = r"c:\dev\edinet2dataset\tosho.csv"
COMPLETE_LIST = r"c:\dev\edinet2dataset\complete_companies.csv"
OUTPUT_FILE = r"c:\dev\edinet2dataset\target_companies.csv"
EXCLUDED_OUTPUT_FILE = r"c:\dev\edinet2dataset\excluded_companies.csv"
CORPUS_DIR = r"c:\dev\edinet2dataset\edinet_corpus\annual"

# 除外する業種名（33業種区分）
EXCLUDE_YINDUSTRIES = {
    "銀行業",
    "保険業",
    "証券、商品先物取引業",
    "その他金融業",
    "-"  # 業種未分類（ETFやREITなどが含まれることが多い）
}

# 除外する市場・商品区分
EXCLUDE_MARKETS = {
    "ETF・ETN",
    "PRO Market",  # 一般的な上場企業分析ならPRO Marketも外すことが多い（任意）
    "REIT・ベンチャーファンド",  # もしあれば
    "出資証券", # REITなど
}

def load_tosho_data():
    """tosho.csvを読み込み、コード(4桁)をキーにした辞書を返す"""
    tosho_map = {}
    with open(TOSHO_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row['コード']
            tosho_map[code] = row
    return tosho_map

def get_sec_code(edinet_code):
    """EDINETコードから証券コードを取得（JSONを読み込む）"""
    company_dir = os.path.join(CORPUS_DIR, edinet_code)
    json_files = glob.glob(os.path.join(company_dir, "*.json"))
    
    if not json_files:
        return None
    
    try:
        with open(json_files[0], 'r', encoding='utf-8') as f:
            data = json.load(f)
            sec_code = data.get('secCode')
            if sec_code and len(sec_code) >= 4:
                return sec_code[:4]
    except Exception:
        pass
    
    return None

def main():
    print("Loading Tosho data...")
    tosho_map = load_tosho_data()
    
    print("Loading complete companies list...")
    companies = []
    with open(COMPLETE_LIST, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        companies = list(reader)
    
    print(f"Processing {len(companies)} companies...")
    
    target_companies = []
    excluded_companies = []
    skipped_count = 0
    
    for comp in tqdm(companies):
        edinet_code = comp['edinet_code']
        sec_code_4 = get_sec_code(edinet_code)
        
        if not sec_code_4:
            skipped_count += 1
            # 証券コードなし＝非上場 or 取得失敗
            excluded_companies.append({
               "edinet_code": edinet_code,
               "sec_code": "",
               "company_name": comp.get('company_name', 'Unknown'),
               "industry": "-",
               "market": "-",
               "reason": "No SecCode (Unlisted?)"
            })
            continue
            
        if sec_code_4 not in tosho_map:
            skipped_count += 1
            # 東証リストにない
            excluded_companies.append({
               "edinet_code": edinet_code,
               "sec_code": sec_code_4,
               "company_name": comp.get('company_name', 'Unknown'),
               "industry": "-",
               "market": "-",
               "reason": "Not in Tosho List"
            })
            continue
            
        row = tosho_map[sec_code_4]
        industry = row['33業種区分']
        market = row['市場・商品区分']
        company_name_tosho = row['銘柄名']
        
        # フィルタリング
        exclusion_reason = None
        if industry in EXCLUDE_YINDUSTRIES:
            exclusion_reason = f"Industry: {industry}"
        elif market in EXCLUDE_MARKETS:
             exclusion_reason = f"Market: {market}"
        elif "ETF" in market or "ETN" in market or "REIT" in market:
             exclusion_reason = f"Market Keyword: {market}"
        
        if exclusion_reason:
            excluded_companies.append({
               "edinet_code": edinet_code,
               "sec_code": sec_code_4,
               "company_name": company_name_tosho,
               "industry": industry,
               "market": market,
               "reason": exclusion_reason
            })
            continue

        target_companies.append({
            "edinet_code": edinet_code,
            "sec_code": sec_code_4,
            "company_name": company_name_tosho,
            "industry": industry,
            "market": market,
            "file_count": comp['file_count']
        })

    # 結果保存 (Target)
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ["edinet_code", "sec_code", "company_name", "industry", "market", "file_count"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(target_companies)

    # 結果保存 (Excluded)
    with open(EXCLUDED_OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ["edinet_code", "sec_code", "company_name", "industry", "market", "reason"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(excluded_companies)

    print("-" * 40)
    print(f"Total processed: {len(companies)}")
    print(f"Target Companies: {len(target_companies)}")
    print(f"Excluded/Skipped: {len(excluded_companies)}")
    print(f"  - Saved targets to {OUTPUT_FILE}")
    print(f"  - Saved excluded to {EXCLUDED_OUTPUT_FILE}")
    print("-" * 40)

if __name__ == "__main__":
    main()
