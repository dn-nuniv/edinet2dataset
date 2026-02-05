import subprocess
import sys
from datetime import date

# 設定
START_YEAR = 2018
END_YEAR = 2018
DOC_TYPES = ["annual"]

def main():
    for year in range(START_YEAR, END_YEAR + 1):
        for doc_type in DOC_TYPES:
            for month in range(1, 13):
                # 開始日と終了日を生成
                start_date = f"{year}-{month:02d}-01"
                
                if month == 12:
                    end_date = f"{year + 1}-01-01"
                else:
                    end_date = f"{year}-{month + 1:02d}-01"

                print(f"Processing: doc_type={doc_type}, start_date={start_date}, end_date={end_date}")

                # 実行コマンド (pythonコマンドを直接使用)
                cmd = [
                    "python",
                    "scripts/prepare_edinet_corpus.py",
                    "--doc_type", doc_type,
                    "--start_date", start_date,
                    "--end_date", end_date
                ]

                try:
                    subprocess.run(cmd, check=True)
                except subprocess.CalledProcessError as e:
                    print(f"Error executing command: {e}")
                except KeyboardInterrupt:
                    print("Aborted by user.")
                    return

if __name__ == "__main__":
    main()
