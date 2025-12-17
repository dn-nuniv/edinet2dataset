from pathlib import Path
import csv
import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from edinet2dataset.downloader import Downloader

def get_attr(x, *names):
    for n in names:
        if hasattr(x, n):
            return getattr(x, n)
    return None

def is_good_annual(dl, r):
    # doc_type 判定（annual）
    if dl.get_doc_type(r.ordinanceCode, r.formCode) != "annual":
        return False
    # docDescription が取れるなら「有価証券報告書」だけ & 「訂正」除外で安定化
    desc = (get_attr(r, "docDescription", "doc_description") or "")
    if desc:
        if ("有価証券報告書" not in desc):
            return False
        if ("訂正" in desc):
            return False
    return True

def main(edinet_code: str, n: int = 5, start_year: int = None, end_year: int = None,
         month_from: int = 5, month_to: int = 7, out_root: str = "data"):

    today = datetime.date.today()
    if end_year is None:
        end_year = today.year
    if start_year is None:
        start_year = end_year - (n - 1)

    dl = Downloader()

    out_dir = Path(out_root) / edinet_code
    out_dir.mkdir(parents=True, exist_ok=True)

    picked = []

    for y in range(end_year, start_year - 1, -1):  # 新しい年から
        start_date = f"{y}-{month_from:02d}-01"
        end_date = f"{y}-{month_to:02d}-31"
        results = dl.get_results(start_date, end_date, edinet_code=edinet_code)

        annuals = [r for r in results if is_good_annual(dl, r)]
        if not annuals:
            # 取りこぼし保険：その年だけ 4〜8月に拡張して再検索（必要な場合だけ）
            start_date2 = f"{y}-04-01"
            end_date2 = f"{y}-08-31"
            results2 = dl.get_results(start_date2, end_date2, edinet_code=edinet_code)
            annuals = [r for r in results2 if is_good_annual(dl, r)]

        if not annuals:
            print(f"[{y}] annual not found")
            continue

        # submitDateTime があればそれで新しい順、無ければ docID
        def k(r):
            sdt = get_attr(r, "submitDateTime", "submitDatetime", "submit_date_time") or ""
            did = get_attr(r, "docID", "docId", "doc_id") or ""
            return (sdt, did)

        annuals.sort(key=k, reverse=True)
        r = annuals[0]
        doc_id = get_attr(r, "docID", "docId", "doc_id")
        if not doc_id:
            continue

        picked.append(r)
        if len(picked) >= n:
            break

    # メタ保存
    meta_path = out_dir / "filings.csv"
    with meta_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["doc_id", "submitDateTime", "docDescription"])
        w.writeheader()
        for r in picked:
            w.writerow({
                "doc_id": get_attr(r, "docID", "docId", "doc_id"),
                "submitDateTime": get_attr(r, "submitDateTime", "submitDatetime", "submit_date_time"),
                "docDescription": get_attr(r, "docDescription", "doc_description"),
            })

    # TSV DL
    for r in picked:
        doc_id = get_attr(r, "docID", "docId", "doc_id")
        dl.download_document(doc_id, file_type="tsv", output_dir=str(out_dir))

    print("Saved:", meta_path)
    print("Downloaded:", [get_attr(r, "docID", "docId", "doc_id") for r in picked])

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--edinet_code", required=True)
    p.add_argument("--n", type=int, default=5)
    p.add_argument("--start_year", type=int, default=None)
    p.add_argument("--end_year", type=int, default=None)
    p.add_argument("--month_from", type=int, default=5)
    p.add_argument("--month_to", type=int, default=7)
    p.add_argument("--out_root", default="data")
    args = p.parse_args()
    main(args.edinet_code, args.n, args.start_year, args.end_year, args.month_from, args.month_to, args.out_root)
