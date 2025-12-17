import os, csv, datetime
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from edinet2dataset.downloader import Downloader

def safe_get(obj, *names):
    for n in names:
        if hasattr(obj, n):
            return getattr(obj, n)
    return None

def main(edinet_code: str, start_date: str, end_date: str, n: int = 5, out_root: str = "data"):
    dl = Downloader()
    results = dl.get_results(start_date, end_date, edinet_code=edinet_code)

    annual = []
    for r in results:
        if dl.get_doc_type(r.ordinanceCode, r.formCode) == "annual":
            annual.append(r)

    # submitDateTime が取れればそれで降順ソート（無ければ docID で降順）
    def sort_key(r):
        sdt = safe_get(r, "submitDateTime", "submitDatetime", "submit_date_time")
        did = safe_get(r, "docID", "docId", "doc_id")
        return (sdt or "", did or "")
    annual.sort(key=sort_key, reverse=True)

    # doc_id 重複排除して上位 n 本
    picked = []
    seen = set()
    for r in annual:
        did = safe_get(r, "docID", "docId", "doc_id")
        if not did or did in seen:
            continue
        seen.add(did)
        picked.append(r)
        if len(picked) >= n:
            break

    out_dir = Path(out_root) / edinet_code
    out_dir.mkdir(parents=True, exist_ok=True)

    # メタ情報を保存（後で年度ラベル付けや検算に使える）
    meta_path = out_dir / "filings.csv"
    with meta_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["doc_id", "submitDateTime", "docDescription"])
        w.writeheader()
        for r in picked:
            doc_id = safe_get(r, "docID", "docId", "doc_id")
            w.writerow({
                "doc_id": doc_id,
                "submitDateTime": safe_get(r, "submitDateTime", "submitDatetime", "submit_date_time"),
                "docDescription": safe_get(r, "docDescription", "doc_description"),
            })

    # TSVをDL（downloader本体は type=5 で doc_id.tsv を保存する実装） 
    for r in picked:
        doc_id = safe_get(r, "docID", "docId", "doc_id")
        dl.download_document(doc_id, file_type="tsv", output_dir=str(out_dir))

    print("Saved:", meta_path)
    print("Downloaded TSVs:", [safe_get(r, "docID", "docId", "doc_id") for r in picked])

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--edinet_code", required=True)
    p.add_argument("--start_date", default="2020-04-01")
    p.add_argument("--end_date", default="2025-12-31")
    p.add_argument("--n", type=int, default=5)
    p.add_argument("--out_root", default="data")
    args = p.parse_args()
    main(args.edinet_code, args.start_date, args.end_date, args.n, args.out_root)
