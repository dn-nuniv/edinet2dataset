# -*- coding: utf-8 -*-
"""
build_mj_inputs.py - 修正ジョーンズモデル用財務データ抽出スクリプト

目的:
    EDINET年次TSVから対象企業の2015〜2024年度の修正ジョーンズモデル用
    最小財務項目を抽出し、会社×年度=1行のテーブルを作成する。
    売上債権(REC)は受取手形/売掛金ベース + 電子記録債権を合成する。

入力:
    - annual_dir: EDINET年次TSVディレクトリ (デフォルト: edinet_corpus/annual)
    - target_companies: 対象企業CSVファイル (デフォルト: target_companies.csv)

出力:
    - out/mj_inputs_2015_2024.csv: 修正ジョーンズ入力データ（完全ケースのみ）
    - out/mj_inputs_2015_2024.parquet: 同上 (parquet形式)
    - out/mj_missing_log.csv: 欠損データログ
    - out/duplicates_log.csv: 重複TSV対応ログ
    - out/rec_debug_log.csv: REC合成のデバッグログ

使い方:
    python build_mj_inputs.py --target_companies target_companies.csv --out_dir out

実行例:
    python scripts/build_mj_inputs.py --target_companies target_companies.csv
    python scripts/build_mj_inputs.py --annual_dir C:\\dev\\edinet2dataset\\edinet_corpus\\annual --num_workers 8
"""

import argparse
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple, Any
import warnings

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False
    import pandas as pd

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    def tqdm(iterable, **kwargs):
        return iterable

# =============================================================================
# 設定・定数
# =============================================================================
FISCAL_YEAR_MIN = 2015
FISCAL_YEAR_MAX = 2024

# 除外する業種（現在はなし。建設業会計科目はfallbackで対応）
EXCLUDE_INDUSTRIES = [
    # "建設業",  # 建設業会計科目をfallbackに追加したため除外不要
]

# 単位変換マップ (円に統一)
UNIT_MAP = {
    "円": 1,
    "千円": 1000,
    "百万円": 1000000,
    "億円": 100000000,
    "JPY": 1,
}

# DEI要素ID
DEI_ACCOUNTING_STANDARDS = "jpdei_cor:AccountingStandardsDEI"
DEI_FISCAL_YEAR_START = "jpdei_cor:CurrentFiscalYearStartDateDEI"
DEI_FISCAL_YEAR_END = "jpdei_cor:CurrentFiscalYearEndDateDEI"

# DEI提出日時候補 (複数候補で探す)
DEI_SUBMIT_DATETIME_CANDIDATES = [
    "jpdei_cor:FilingDateDEI",
    "jpdei_cor:SubmissionDateTimeDEI",
    "jpcrp_cor:FilingDateCoverPage",
]

# 財務項目要素ID (jppfs_cor:プレフィックスは後で追加)
# 期間項目(期間・時点=="期間")
ELEMENT_NET_SALES = [
    "NetSales",
    "NetSalesOfCompletedConstructionContracts",  # 完成工事高（建設業）
]  # 売上高
ELEMENT_PROFIT_LOSS = ["ProfitLoss", "ProfitLossAttributableToOwnersOfParent"]  # 当期純利益
ELEMENT_OPERATING_CF = ["NetCashProvidedByUsedInOperatingActivities"]  # 営業CF

# 時点項目(期間・時点=="時点")
ELEMENT_ASSETS = ["Assets"]  # 総資産

# 売上債権(REC)ベース（最初に見つかったものを採用）
ELEMENT_REC_BASE = [
    "NotesAndAccountsReceivableTrade",
    "NotesAndAccountsReceivableTradeAndContractAssets",
    "ReceivablesTradeAndContractAssets",
    "AccountsReceivableTrade",
    "TradeAndOtherReceivables",
    "NotesReceivableTrade",
    "AccountsReceivableOther",
    "AccountsReceivableFromCompletedConstructionContracts",
    "NotesReceivableAccountsReceivableFromCompletedConstructionContractsAndOtherCNS",
]

# 電子記録債権（加算候補）
ELEMENT_ECLAIMS = [
    "ElectronicallyRecordedMonetaryClaimsOperatingCA",
    "ElectronicallyRecordedMonetaryClaimsOperating",
]

ELEMENT_PPE = [
    "PropertyPlantAndEquipment",
    "PropertyPlantAndEquipmentNet",
]  # 有形固定資産

# =============================================================================
# データクラス
# =============================================================================
@dataclass
class TSVMetadata:
    """TSVファイルのDEIメタデータ"""
    tsv_path: Path
    edinet_code: str
    accounting_standard: Optional[str] = None
    fiscal_year: Optional[int] = None
    fiscal_year_start: Optional[str] = None
    fiscal_year_end: Optional[str] = None
    submit_datetime: Optional[str] = None
    doc_id: Optional[str] = None
    has_ifrs_us_gaap: bool = False  # IFRS/US-GAAPを含むか


@dataclass
class MJInput:
    """修正ジョーンズモデル入力データ"""
    edinet_code: str
    sec_code: Optional[str] = None
    company_name: Optional[str] = None
    industry: Optional[str] = None
    market: Optional[str] = None
    fiscal_year: Optional[int] = None
    period_end: Optional[str] = None
    submit_datetime: Optional[str] = None
    doc_id: Optional[str] = None
    tsv_path: Optional[str] = None
    is_consolidated: Optional[bool] = None  # True=連結, False=個別
    # 財務項目
    assets_prev_end: Optional[float] = None  # 前期末総資産
    rev_t: Optional[float] = None  # 当期売上高
    rev_t1: Optional[float] = None  # 前期売上高
    ar_end_t: Optional[float] = None  # 当期末売掛金(PLUS_E)
    ar_end_t1: Optional[float] = None  # 前期末売掛金(PLUS_E)
    ar_end_t_no_e: Optional[float] = None  # 当期末売掛金(電子記録債権を足さない)
    ar_end_t1_no_e: Optional[float] = None  # 前期末売掛金(電子記録債権を足さない)
    eclaims_end_t: Optional[float] = None  # 当期末電子記録債権
    eclaims_end_t1: Optional[float] = None  # 前期末電子記録債権
    rec_rule: Optional[str] = None  # REC合成ルール
    rec_used_base_eid: Optional[str] = None  # RECベース採用要素名
    rec_used_eclaims_eid: Optional[str] = None  # 電子記録債権採用要素名
    ppe_end_t: Optional[float] = None  # 当期末PPE
    ni_t: Optional[float] = None  # 当期純利益
    cfo_t: Optional[float] = None  # 営業CF
    # 計算項目
    ta_t: Optional[float] = None  # 発生高(ni - cfo)
    d_rev: Optional[float] = None  # 売上高変化
    d_ar: Optional[float] = None  # 売掛金変化


@dataclass
class MissingLog:
    """欠損ログ"""
    edinet_code: str
    fiscal_year: Optional[int]
    tsv_path: str
    missing_reason: str


@dataclass
class DuplicateLog:
    """重複ログ"""
    edinet_code: str
    fiscal_year: int
    chosen_tsv: str
    chosen_submit_datetime: Optional[str]
    skipped_tsv: str
    skipped_reason: str


@dataclass
class RecDebugLog:
    """REC合成デバッグログ"""
    edinet_code: str
    fiscal_year: Optional[int]
    tsv_path: str
    is_consolidated: Optional[bool]
    rec_rule: Optional[str]
    rec_base_end_t: Optional[float]
    rec_base_end_t1: Optional[float]
    rec_eclaims_end_t: Optional[float]
    rec_eclaims_end_t1: Optional[float]
    rec_plus_e_end_t: Optional[float]
    rec_plus_e_end_t1: Optional[float]
    rec_used_base_eid_end_t: Optional[str]
    rec_used_base_eid_end_t1: Optional[str]
    rec_used_eclaims_eid_end_t: Optional[str]
    rec_used_eclaims_eid_end_t1: Optional[str]


# =============================================================================
# ユーティリティ関数
# =============================================================================
def normalize_value(value_str: str, unit: str) -> Optional[float]:
    """
    TSVの値を数値化して円に統一する。
    - '－' / 空欄は欠損（NaN）
    - カンマ除去
    - 括弧 () はマイナス
    - 単位により円に統一
    """
    if value_str is None:
        return None
    
    value_str = str(value_str).strip()
    
    # 欠損パターン
    if value_str in ("", "－", "-", "―", "ー", "−", "nan", "None"):
        return None
    
    try:
        # カンマ除去
        value_str = value_str.replace(",", "")
        
        # 括弧はマイナス: (1234) -> -1234
        is_negative = False
        if value_str.startswith("(") and value_str.endswith(")"):
            is_negative = True
            value_str = value_str[1:-1]
        elif value_str.startswith("△") or value_str.startswith("▲"):
            is_negative = True
            value_str = value_str[1:]
        
        # 数値変換
        value = float(value_str)
        if is_negative:
            value = -value
        
        # 単位変換
        unit_multiplier = UNIT_MAP.get(unit, 1)
        value = value * unit_multiplier
        
        return value
    except (ValueError, TypeError):
        return None


def extract_fiscal_year(date_str: str) -> Optional[int]:
    """日付文字列から年度を取得 (年度開始日のyear)"""
    if not date_str:
        return None
    try:
        # YYYY-MM-DD or YYYY/MM/DD 形式を想定
        match = re.match(r"(\d{4})[-/](\d{2})[-/](\d{2})", str(date_str))
        if match:
            return int(match.group(1))
    except (ValueError, TypeError):
        pass
    return None


def contains_member(context_id: str) -> bool:
    """コンテキストIDにMemberが含まれるか"""
    if not context_id:
        return False
    return "Member" in str(context_id)


def has_ifrs_or_usgaap(element_id: str) -> bool:
    """要素IDにifrs-full:またはus-gaap:が含まれるか"""
    if not element_id:
        return False
    elem = str(element_id).lower()
    return "ifrs-full:" in elem or "us-gaap:" in elem


# =============================================================================
# TSV読み込み関連
# =============================================================================
def read_tsv_polars(tsv_path: Path) -> pl.DataFrame:
    """polarsでTSVを読み込む"""
    return pl.read_csv(
        tsv_path,
        separator="\t",
        encoding="utf-16",
        truncate_ragged_lines=True,
        ignore_errors=True,
        infer_schema_length=0,  # 全列を文字列として読み込む
    )


def read_tsv_pandas(tsv_path: Path):
    """pandasでTSVを読み込む"""
    return pd.read_csv(
        tsv_path,
        sep="\t",
        encoding="utf-16",
        on_bad_lines="skip",
    )


def read_tsv(tsv_path: Path):
    """TSVを読み込む (polars優先、なければpandas)"""
    if HAS_POLARS:
        return read_tsv_polars(tsv_path)
    else:
        return read_tsv_pandas(tsv_path)


def read_tsv_dei_only(tsv_path: Path) -> Optional[TSVMetadata]:
    """
    TSVからDEI情報のみを高速に取得する。
    必要な列だけ読み、DEI行のみフィルタ。
    """
    try:
        if HAS_POLARS:
            # polarsで必要な列のみ読み込み（全て文字列として）
            df = pl.read_csv(
                tsv_path,
                separator="\t",
                encoding="utf-16",
                truncate_ragged_lines=True,
                ignore_errors=True,
                columns=["要素ID", "値"],
                infer_schema_length=0,  # 全列を文字列として読み込む
            )
            
            # IFRS/US-GAAP含むか確認
            has_ifrs = False
            if "要素ID" in df.columns:
                elem_col = df["要素ID"].cast(pl.Utf8)
                ifrs_check = elem_col.str.to_lowercase()
                has_ifrs = (
                    ifrs_check.str.contains("ifrs-full:").any()
                    or ifrs_check.str.contains("us-gaap:").any()
                )
            
            # DEI行のみフィルタ
            dei_df = df.filter(pl.col("要素ID").str.contains("jpdei_cor:|jpcrp_cor:"))
            
            # DEI値を辞書化
            dei_dict = {}
            for row in dei_df.iter_rows(named=True):
                elem_id = row["要素ID"]
                val = row["値"]
                if elem_id:
                    dei_dict[elem_id] = val
        else:
            # pandas fallback
            df = pd.read_csv(
                tsv_path,
                sep="\t",
                encoding="utf-16",
                usecols=["要素ID", "値"],
                on_bad_lines="skip",
            )
            
            # IFRS/US-GAAP含むか
            has_ifrs = (
                df["要素ID"].str.lower().str.contains("ifrs-full:", na=False).any()
                or df["要素ID"].str.lower().str.contains("us-gaap:", na=False).any()
            )
            
            # DEI行のみ
            dei_df = df[df["要素ID"].str.contains("jpdei_cor:|jpcrp_cor:", na=False, regex=True)]
            dei_dict = dict(zip(dei_df["要素ID"], dei_df["値"]))
        
        # メタデータ構築
        meta = TSVMetadata(
            tsv_path=tsv_path,
            edinet_code=tsv_path.parent.name,
            accounting_standard=dei_dict.get(DEI_ACCOUNTING_STANDARDS),
            fiscal_year_start=dei_dict.get(DEI_FISCAL_YEAR_START),
            fiscal_year_end=dei_dict.get(DEI_FISCAL_YEAR_END),
            has_ifrs_us_gaap=has_ifrs,
            doc_id=tsv_path.stem,
        )
        
        # 年度計算
        meta.fiscal_year = extract_fiscal_year(meta.fiscal_year_start)
        
        # 提出日時を取得 (複数候補から)
        for dt_key in DEI_SUBMIT_DATETIME_CANDIDATES:
            if dt_key in dei_dict and dei_dict[dt_key]:
                meta.submit_datetime = dei_dict[dt_key]
                break
        
        return meta
    
    except Exception as e:
        warnings.warn(f"Failed to read DEI from {tsv_path}: {e}")
        return None


def select_best_tsv(
    metas: List[TSVMetadata],
    edinet_code: str,
    fiscal_year: int,
) -> Tuple[Optional[TSVMetadata], List[DuplicateLog]]:
    """
    同一edinet_code × fiscal_yearで複数TSVがある場合、1つを選択。
    - まずsubmit_datetimeが最も新しいものを採用
    - submit_datetime取得不可なら、ファイル名辞書順で最後を採用
    """
    duplicate_logs = []
    
    if not metas:
        return None, duplicate_logs
    
    if len(metas) == 1:
        return metas[0], duplicate_logs
    
    # submit_datetimeでソート (降順)
    metas_with_dt = [(m, m.submit_datetime or "") for m in metas]
    metas_with_dt.sort(key=lambda x: (x[1], str(x[0].tsv_path)), reverse=True)
    
    chosen = metas_with_dt[0][0]
    
    for m, _ in metas_with_dt[1:]:
        duplicate_logs.append(DuplicateLog(
            edinet_code=edinet_code,
            fiscal_year=fiscal_year,
            chosen_tsv=str(chosen.tsv_path.name),
            chosen_submit_datetime=chosen.submit_datetime,
            skipped_tsv=str(m.tsv_path.name),
            skipped_reason="duplicate_skipped: newer submit_datetime or filename chosen",
        ))
    
    return chosen, duplicate_logs


def extract_value_with_fallback(
    df,
    element_ids: List[str],
    relative_year: str,
    period_type: str,
    consolidated: str = "連結",
    prefix: str = "jppfs_cor:",
) -> Optional[float]:
    """
    複数の要素IDからfallbackで値を取得。
    セグメント混入対策：
    - コンテキストIDにMemberを含む行を除外
    - 複数行あれば最大値を採用
    """
    if HAS_POLARS:
        return _extract_value_polars(df, element_ids, relative_year, period_type, consolidated, prefix)
    else:
        return _extract_value_pandas(df, element_ids, relative_year, period_type, consolidated, prefix)


def _extract_value_polars(
    df: pl.DataFrame,
    element_ids: List[str],
    relative_year: str,
    period_type: str,
    consolidated: str,
    prefix: str,
) -> Optional[float]:
    """polars版の値抽出"""
    for elem_id in element_ids:
        full_elem_id = f"{prefix}{elem_id}"
        
        try:
            # フィルタ
            filtered = df.filter(
                (pl.col("要素ID") == full_elem_id)
                & (pl.col("相対年度") == relative_year)
                & (pl.col("期間・時点") == period_type)
                & (pl.col("連結・個別") == consolidated)
            )
            
            # セグメントMember除外 (NonConsolidatedMember/ConsolidatedMemberは除外しない)
            if "コンテキストID" in df.columns:
                # Memberを含むが、NonConsolidatedMember/ConsolidatedMemberでない場合のみ除外
                filtered = filtered.filter(
                    ~(
                        pl.col("コンテキストID").str.contains("Member")
                        & ~pl.col("コンテキストID").str.contains("NonConsolidatedMember")
                        & ~pl.col("コンテキストID").str.contains("ConsolidatedMember")
                    )
                )
            
            if filtered.height == 0:
                continue
            
            # 値を数値化して最大を取得
            values = []
            for row in filtered.iter_rows(named=True):
                val = normalize_value(row.get("値", ""), row.get("単位", "円"))
                if val is not None:
                    values.append(val)
            
            if values:
                return max(values)
        
        except Exception:
            continue
    
    return None


def _extract_value_pandas(
    df,
    element_ids: List[str],
    relative_year: str,
    period_type: str,
    consolidated: str,
    prefix: str,
) -> Optional[float]:
    """pandas版の値抽出"""
    for elem_id in element_ids:
        full_elem_id = f"{prefix}{elem_id}"
        
        try:
            # フィルタ
            mask = (
                (df["要素ID"] == full_elem_id)
                & (df["相対年度"] == relative_year)
                & (df["期間・時点"] == period_type)
                & (df["連結・個別"] == consolidated)
            )
            filtered = df[mask]
            
            # セグメントMember除外 (NonConsolidatedMember/ConsolidatedMemberは除外しない)
            if "コンテキストID" in df.columns:
                # Memberを含むが、NonConsolidatedMember/ConsolidatedMemberでない場合のみ除外
                mask_member = filtered["コンテキストID"].str.contains("Member", na=False)
                mask_non_cons = filtered["コンテキストID"].str.contains("NonConsolidatedMember", na=False)
                mask_cons = filtered["コンテキストID"].str.contains("ConsolidatedMember", na=False)
                filtered = filtered[~(mask_member & ~mask_non_cons & ~mask_cons)]
            
            if len(filtered) == 0:
                continue
            
            # 値を数値化して最大を取得
            values = []
            for _, row in filtered.iterrows():
                val = normalize_value(row.get("値", ""), row.get("単位", "円"))
                if val is not None:
                    values.append(val)
            
            if values:
                return max(values)
        
        except Exception:
            continue
    
    return None


def extract_value_with_fallback_and_eid(
    df,
    element_ids: List[str],
    relative_year: str,
    period_type: str,
    consolidated: str = "連結",
    prefix: str = "jppfs_cor:",
) -> Tuple[Optional[float], Optional[str]]:
    """
    複数の要素IDからfallbackで値を取得し、採用した要素名も返す。
    """
    if HAS_POLARS:
        return _extract_value_polars_with_eid(df, element_ids, relative_year, period_type, consolidated, prefix)
    else:
        return _extract_value_pandas_with_eid(df, element_ids, relative_year, period_type, consolidated, prefix)


def _extract_value_polars_with_eid(
    df: pl.DataFrame,
    element_ids: List[str],
    relative_year: str,
    period_type: str,
    consolidated: str,
    prefix: str,
) -> Tuple[Optional[float], Optional[str]]:
    """polars版の値抽出（採用要素名つき）"""
    for elem_id in element_ids:
        full_elem_id = f"{prefix}{elem_id}"
        
        try:
            filtered = df.filter(
                (pl.col("要素ID") == full_elem_id)
                & (pl.col("相対年度") == relative_year)
                & (pl.col("期間・時点") == period_type)
                & (pl.col("連結・個別") == consolidated)
            )
            
            if "コンテキストID" in df.columns:
                filtered = filtered.filter(
                    ~(
                        pl.col("コンテキストID").str.contains("Member")
                        & ~pl.col("コンテキストID").str.contains("NonConsolidatedMember")
                        & ~pl.col("コンテキストID").str.contains("ConsolidatedMember")
                    )
                )
            
            if filtered.height == 0:
                continue
            
            values = []
            for row in filtered.iter_rows(named=True):
                val = normalize_value(row.get("値", ""), row.get("単位", "円"))
                if val is not None:
                    values.append(val)
            
            if values:
                return max(values), elem_id
        
        except Exception:
            continue
    
    return None, None


def _extract_value_pandas_with_eid(
    df,
    element_ids: List[str],
    relative_year: str,
    period_type: str,
    consolidated: str,
    prefix: str,
) -> Tuple[Optional[float], Optional[str]]:
    """pandas版の値抽出（採用要素名つき）"""
    for elem_id in element_ids:
        full_elem_id = f"{prefix}{elem_id}"
        
        try:
            mask = (
                (df["要素ID"] == full_elem_id)
                & (df["相対年度"] == relative_year)
                & (df["期間・時点"] == period_type)
                & (df["連結・個別"] == consolidated)
            )
            filtered = df[mask]
            
            if "コンテキストID" in df.columns:
                mask_member = filtered["コンテキストID"].str.contains("Member", na=False)
                mask_non_cons = filtered["コンテキストID"].str.contains("NonConsolidatedMember", na=False)
                mask_cons = filtered["コンテキストID"].str.contains("ConsolidatedMember", na=False)
                filtered = filtered[~(mask_member & ~mask_non_cons & ~mask_cons)]
            
            if len(filtered) == 0:
                continue
            
            values = []
            for _, row in filtered.iterrows():
                val = normalize_value(row.get("値", ""), row.get("単位", "円"))
                if val is not None:
                    values.append(val)
            
            if values:
                return max(values), elem_id
        
        except Exception:
            continue
    
    return None, None


def check_has_consolidated_data(df, element_ids: List[str] = None) -> bool:
    """
    TSV内に連結データが存在するかを確認する。
    主要な財務項目（売上高など）で連結データが取れるかで判定。
    """
    if element_ids is None:
        element_ids = ELEMENT_NET_SALES  # デフォルトは売上高で判定
    
    if HAS_POLARS:
        try:
            # 連結の行が存在するかチェック
            consolidated_rows = df.filter(
                (pl.col("連結・個別") == "連結")
            )
            if consolidated_rows.height == 0:
                return False
            
            # 主要項目が連結で取れるかチェック
            for elem_id in element_ids:
                full_elem_id = f"jppfs_cor:{elem_id}"
                matched = consolidated_rows.filter(
                    (pl.col("要素ID") == full_elem_id)
                    & (pl.col("相対年度") == "当期")
                )
                if matched.height > 0:
                    return True
            return False
        except Exception:
            return False
    else:
        try:
            consolidated_rows = df[df["連結・個別"] == "連結"]
            if len(consolidated_rows) == 0:
                return False
            
            for elem_id in element_ids:
                full_elem_id = f"jppfs_cor:{elem_id}"
                matched = consolidated_rows[
                    (consolidated_rows["要素ID"] == full_elem_id)
                    & (consolidated_rows["相対年度"] == "当期")
                ]
                if len(matched) > 0:
                    return True
            return False
        except Exception:
            return False


def extract_mj_data_from_tsv(
    tsv_path: Path,
    meta: TSVMetadata,
    company_info: Dict[str, Any],
) -> Tuple[Optional[MJInput], List[str], Optional[RecDebugLog]]:
    """
    TSVから修正ジョーンズモデル用データを抽出。
    連結優先、連結がなければ個別にフォールバック。
    Japan GAAPのみ対象。
    """
    missing_reasons = []
    
    try:
        df = read_tsv(tsv_path)
    except Exception as e:
        return None, [f"TSV read error: {e}"], None
    
    # 連結データがあるかチェック
    has_consolidated = check_has_consolidated_data(df, ELEMENT_NET_SALES)
    
    # 連結優先、なければ個別
    if has_consolidated:
        consolidated_type = "連結"
        is_consolidated = True
    else:
        consolidated_type = "個別"
        is_consolidated = False
    
    mj = MJInput(
        edinet_code=meta.edinet_code,
        sec_code=company_info.get("sec_code"),
        company_name=company_info.get("company_name"),
        industry=company_info.get("industry"),
        market=company_info.get("market"),
        fiscal_year=meta.fiscal_year,
        period_end=meta.fiscal_year_end,
        submit_datetime=meta.submit_datetime,
        doc_id=meta.doc_id,
        tsv_path=str(tsv_path),
        is_consolidated=is_consolidated,
    )
    
    # 期間項目 (期間・時点=="期間")
    # 売上高: 当期
    mj.rev_t = extract_value_with_fallback(df, ELEMENT_NET_SALES, "当期", "期間", consolidated_type)
    if mj.rev_t is None:
        missing_reasons.append(f"rev_t (NetSales 当期 {consolidated_type})")
    
    # 売上高: 前期
    mj.rev_t1 = extract_value_with_fallback(df, ELEMENT_NET_SALES, "前期", "期間", consolidated_type)
    if mj.rev_t1 is None:
        missing_reasons.append(f"rev_t1 (NetSales 前期 {consolidated_type})")
    
    # 当期純利益: 当期
    mj.ni_t = extract_value_with_fallback(df, ELEMENT_PROFIT_LOSS, "当期", "期間", consolidated_type)
    if mj.ni_t is None:
        missing_reasons.append(f"ni_t (ProfitLoss 当期 {consolidated_type})")
    
    # 営業CF: 当期
    mj.cfo_t = extract_value_with_fallback(df, ELEMENT_OPERATING_CF, "当期", "期間", consolidated_type)
    if mj.cfo_t is None:
        missing_reasons.append(f"cfo_t (OperatingCF 当期 {consolidated_type})")
    
    # 時点項目 (期間・時点=="時点")
    # 総資産: 前期末
    mj.assets_prev_end = extract_value_with_fallback(df, ELEMENT_ASSETS, "前期末", "時点", consolidated_type)
    if mj.assets_prev_end is None:
        missing_reasons.append(f"assets_prev_end (Assets 前期末 {consolidated_type})")
    
    # 売上債権(REC): 当期末/前期末
    rec_base_t, rec_base_eid_t = extract_value_with_fallback_and_eid(
        df, ELEMENT_REC_BASE, "当期末", "時点", consolidated_type
    )
    rec_eclaims_t, rec_eclaims_eid_t = extract_value_with_fallback_and_eid(
        df, ELEMENT_ECLAIMS, "当期末", "時点", consolidated_type
    )
    rec_base_t1, rec_base_eid_t1 = extract_value_with_fallback_and_eid(
        df, ELEMENT_REC_BASE, "前期末", "時点", consolidated_type
    )
    rec_eclaims_t1, rec_eclaims_eid_t1 = extract_value_with_fallback_and_eid(
        df, ELEMENT_ECLAIMS, "前期末", "時点", consolidated_type
    )
    
    def add_eclaims(base: Optional[float], eclaims: Optional[float]) -> Optional[float]:
        if base is None:
            return None
        return base + (eclaims or 0)
    
    mj.ar_end_t_no_e = rec_base_t
    mj.ar_end_t1_no_e = rec_base_t1
    mj.eclaims_end_t = rec_eclaims_t
    mj.eclaims_end_t1 = rec_eclaims_t1
    mj.ar_end_t = add_eclaims(rec_base_t, rec_eclaims_t)
    mj.ar_end_t1 = add_eclaims(rec_base_t1, rec_eclaims_t1)
    mj.rec_rule = "PLUS_E" if rec_base_t is not None else "NO_BASE"
    mj.rec_used_base_eid = rec_base_eid_t or ""
    mj.rec_used_eclaims_eid = rec_eclaims_eid_t or ""
    
    if mj.ar_end_t is None:
        missing_reasons.append(f"ar_end_t (REC base 当期末 {consolidated_type})")
    if mj.ar_end_t1 is None:
        missing_reasons.append(f"ar_end_t1 (REC base 前期末 {consolidated_type})")
    
    # PPE: 当期末
    mj.ppe_end_t = extract_value_with_fallback(df, ELEMENT_PPE, "当期末", "時点", consolidated_type)
    if mj.ppe_end_t is None:
        missing_reasons.append(f"ppe_end_t (PPE 当期末 {consolidated_type})")
    
    # 計算項目
    if mj.ni_t is not None and mj.cfo_t is not None:
        mj.ta_t = mj.ni_t - mj.cfo_t
    
    if mj.rev_t is not None and mj.rev_t1 is not None:
        mj.d_rev = mj.rev_t - mj.rev_t1
    
    if mj.ar_end_t is not None and mj.ar_end_t1 is not None:
        mj.d_ar = mj.ar_end_t - mj.ar_end_t1
    
    if mj.ta_t is None:
        missing_reasons.append("ta_t (ni_t - cfo_t)")
    if mj.d_rev is None:
        missing_reasons.append("d_rev (rev_t - rev_t1)")
    if mj.d_ar is None:
        missing_reasons.append("d_ar (ar_end_t - ar_end_t1)")
    
    rec_debug = RecDebugLog(
        edinet_code=meta.edinet_code,
        fiscal_year=meta.fiscal_year,
        tsv_path=str(tsv_path),
        is_consolidated=is_consolidated,
        rec_rule=mj.rec_rule,
        rec_base_end_t=rec_base_t,
        rec_base_end_t1=rec_base_t1,
        rec_eclaims_end_t=rec_eclaims_t,
        rec_eclaims_end_t1=rec_eclaims_t1,
        rec_plus_e_end_t=mj.ar_end_t,
        rec_plus_e_end_t1=mj.ar_end_t1,
        rec_used_base_eid_end_t=rec_base_eid_t or "",
        rec_used_base_eid_end_t1=rec_base_eid_t1 or "",
        rec_used_eclaims_eid_end_t=rec_eclaims_eid_t or "",
        rec_used_eclaims_eid_end_t1=rec_eclaims_eid_t1 or "",
    )
    
    return mj, missing_reasons, rec_debug


# =============================================================================
# メイン処理
# =============================================================================
def scan_tsvs_for_company(
    company_dir: Path,
    edinet_code: str,
) -> Tuple[List[TSVMetadata], Dict[int, str]]:
    """
    企業ディレクトリ内のTSVをスキャンしてメタデータを取得。
    Returns:
        metas: Japan GAAPのTSVメタデータリスト
        non_japan_gaap_by_year: 年度ごとの非Japan GAAP会計基準 (e.g., {2024: "IFRS"})
    """
    metas = []
    non_japan_gaap_by_year: Dict[int, str] = {}
    
    if not company_dir.exists():
        return metas, non_japan_gaap_by_year
    
    for tsv_path in company_dir.glob("*.tsv"):
        meta = read_tsv_dei_only(tsv_path)
        if meta is None:
            continue
        
        # 対象年度のみ
        if meta.fiscal_year is None:
            continue
        if not (FISCAL_YEAR_MIN <= meta.fiscal_year <= FISCAL_YEAR_MAX):
            continue
        
        # Japan GAAPのみ抽出、それ以外は会計基準を記録
        acc_std = meta.accounting_standard
        is_japan_gaap = acc_std is None or "Japan GAAP" in str(acc_std)
        
        # IFRS/US-GAAP除外
        if meta.has_ifrs_us_gaap:
            is_japan_gaap = False
            # 会計基準を特定
            if acc_std:
                non_japan_gaap_by_year[meta.fiscal_year] = str(acc_std)
            else:
                non_japan_gaap_by_year[meta.fiscal_year] = "IFRS/US-GAAP"
            continue
        
        if not is_japan_gaap:
            non_japan_gaap_by_year[meta.fiscal_year] = str(acc_std) if acc_std else "Unknown"
            continue
        
        metas.append(meta)
    
    return metas, non_japan_gaap_by_year


def process_company(
    edinet_code: str,
    company_info: Dict[str, Any],
    annual_dir: Path,
) -> Tuple[List[MJInput], List[MissingLog], List[DuplicateLog], List[RecDebugLog]]:
    """1社分の処理"""
    mj_inputs = []
    missing_logs = []
    duplicate_logs = []
    rec_debug_logs = []
    
    company_dir = annual_dir / edinet_code
    
    # Phase A: DEIスキャンで採用TSVを決定
    metas, non_japan_gaap_by_year = scan_tsvs_for_company(company_dir, edinet_code)
    
    # fiscal_yearごとにグループ化して重複処理
    by_year: Dict[int, List[TSVMetadata]] = {}
    for m in metas:
        fy = m.fiscal_year
        if fy not in by_year:
            by_year[fy] = []
        by_year[fy].append(m)
    
    # Phase B: 各年度について採用TSVから財務データ抽出
    for fy in range(FISCAL_YEAR_MIN, FISCAL_YEAR_MAX + 1):
        year_metas = by_year.get(fy, [])
        
        if not year_metas:
            # Japan GAAPのTSVがない場合、会計基準を確認
            if fy in non_japan_gaap_by_year:
                reason = f"accounting_standard: {non_japan_gaap_by_year[fy]}"
            else:
                reason = "no_tsv_found_for_year"
            missing_logs.append(MissingLog(
                edinet_code=edinet_code,
                fiscal_year=fy,
                tsv_path="",
                missing_reason=reason,
            ))
            continue
        
        chosen_meta, dup_logs = select_best_tsv(year_metas, edinet_code, fy)
        duplicate_logs.extend(dup_logs)
        
        if chosen_meta is None:
            continue
        
        mj, missing_reasons, rec_debug = extract_mj_data_from_tsv(
            chosen_meta.tsv_path,
            chosen_meta,
            company_info,
        )
        
        if rec_debug is not None:
            rec_debug_logs.append(rec_debug)
        
        if mj is None:
            if missing_reasons:
                missing_logs.append(MissingLog(
                    edinet_code=edinet_code,
                    fiscal_year=fy,
                    tsv_path=str(chosen_meta.tsv_path),
                    missing_reason="; ".join(missing_reasons),
                ))
            continue
        
        if missing_reasons:
            missing_logs.append(MissingLog(
                edinet_code=edinet_code,
                fiscal_year=fy,
                tsv_path=str(chosen_meta.tsv_path),
                missing_reason="; ".join(missing_reasons),
            ))
        else:
            mj_inputs.append(mj)
    
    return mj_inputs, missing_logs, duplicate_logs, rec_debug_logs


def main():
    parser = argparse.ArgumentParser(
        description="修正ジョーンズモデル用財務データ抽出"
    )
    parser.add_argument(
        "--annual_dir",
        type=str,
        default=r"C:\dev\edinet2dataset\edinet_corpus\annual",
        help="EDINET年次TSVディレクトリ",
    )
    parser.add_argument(
        "--target_companies",
        type=str,
        default="target_companies.csv",
        help="対象企業CSVファイル",
    )
    parser.add_argument(
        "--out_dir",
        type=str,
        default="out",
        help="出力ディレクトリ",
    )
    parser.add_argument(
        "--num_workers",
        type=int,
        default=4,
        help="並列数",
    )
    
    args = parser.parse_args()
    
    annual_dir = Path(args.annual_dir)
    target_path = Path(args.target_companies)
    out_dir = Path(args.out_dir)
    
    # 出力ディレクトリ作成
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # 対象企業読み込み
    print(f"Reading target companies from {target_path}...")
    if HAS_POLARS:
        target_df = pl.read_csv(target_path)
        companies = [
            {
                "edinet_code": row["edinet_code"],
                "sec_code": row.get("sec_code"),
                "company_name": row.get("company_name"),
                "industry": row.get("industry"),
                "market": row.get("market"),
            }
            for row in target_df.iter_rows(named=True)
        ]
    else:
        target_df = pd.read_csv(target_path)
        companies = target_df.to_dict(orient="records")
    
    print(f"Found {len(companies)} target companies")
    
    # 除外業種をフィルタ
    if EXCLUDE_INDUSTRIES:
        original_count = len(companies)
        companies = [c for c in companies if c.get("industry") not in EXCLUDE_INDUSTRIES]
        excluded_count = original_count - len(companies)
        print(f"Excluded {excluded_count} companies in excluded industries: {EXCLUDE_INDUSTRIES}")
        print(f"Processing {len(companies)} companies after filter")
    
    # 全結果格納用
    all_mj_inputs: List[MJInput] = []
    all_missing_logs: List[MissingLog] = []
    all_duplicate_logs: List[DuplicateLog] = []
    all_rec_debug_logs: List[RecDebugLog] = []
    
    # 並列処理
    print(f"Processing companies with {args.num_workers} workers...")
    
    def process_one(company_info):
        edinet_code = company_info["edinet_code"]
        return process_company(edinet_code, company_info, annual_dir)
    
    with ThreadPoolExecutor(max_workers=args.num_workers) as executor:
        futures = {
            executor.submit(process_one, c): c["edinet_code"]
            for c in companies
        }
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Companies"):
            try:
                mj_inputs, missing, dups, rec_debugs = future.result()
                all_mj_inputs.extend(mj_inputs)
                all_missing_logs.extend(missing)
                all_duplicate_logs.extend(dups)
                all_rec_debug_logs.extend(rec_debugs)
            except Exception as e:
                edinet_code = futures[future]
                warnings.warn(f"Error processing {edinet_code}: {e}")
    
    # 結果出力
    print(f"\nWriting results to {out_dir}...")
    
    # mj_inputs（完全ケースのみ）
    mj_records = []
    for mj in all_mj_inputs:
        mj_records.append({
            "edinet_code": mj.edinet_code,
            "sec_code": mj.sec_code,
            "company_name": mj.company_name,
            "industry": mj.industry,
            "market": mj.market,
            "fiscal_year": mj.fiscal_year,
            "period_end": mj.period_end,
            "submit_datetime": mj.submit_datetime,
            "doc_id": mj.doc_id,
            "tsv_path": mj.tsv_path,
            "is_consolidated": mj.is_consolidated,  # True=連結, False=個別
            "assets_prev_end": mj.assets_prev_end,
            "rev_t": mj.rev_t,
            "rev_t1": mj.rev_t1,
            "ar_end_t": mj.ar_end_t,
            "ar_end_t1": mj.ar_end_t1,
            "ar_end_t_no_e": mj.ar_end_t_no_e,
            "ar_end_t1_no_e": mj.ar_end_t1_no_e,
            "eclaims_end_t": mj.eclaims_end_t,
            "eclaims_end_t1": mj.eclaims_end_t1,
            "rec_rule": mj.rec_rule,
            "rec_used_base_eid": mj.rec_used_base_eid,
            "rec_used_eclaims_eid": mj.rec_used_eclaims_eid,
            "ppe_end_t": mj.ppe_end_t,
            "ni_t": mj.ni_t,
            "cfo_t": mj.cfo_t,
            "ta_t": mj.ta_t,
            "d_rev": mj.d_rev,
            "d_ar": mj.d_ar,
        })
    
    if HAS_POLARS:
        mj_df = pl.DataFrame(mj_records)
        mj_df.write_csv(out_dir / "mj_inputs_2015_2024.csv")
        try:
            mj_df.write_parquet(out_dir / "mj_inputs_2015_2024.parquet")
        except Exception as e:
            warnings.warn(f"Could not write parquet: {e}")
    else:
        mj_df = pd.DataFrame(mj_records)
        mj_df.to_csv(out_dir / "mj_inputs_2015_2024.csv", index=False)
        try:
            mj_df.to_parquet(out_dir / "mj_inputs_2015_2024.parquet", index=False)
        except Exception:
            pass
    
    print(f"  - mj_inputs: {len(mj_records)} records")
    
    # missing_log
    missing_records = [
        {
            "edinet_code": m.edinet_code,
            "fiscal_year": m.fiscal_year,
            "tsv_path": m.tsv_path,
            "missing_reason": m.missing_reason,
        }
        for m in all_missing_logs
    ]
    if HAS_POLARS:
        pl.DataFrame(missing_records).write_csv(out_dir / "mj_missing_log.csv")
    else:
        pd.DataFrame(missing_records).to_csv(out_dir / "mj_missing_log.csv", index=False)
    
    print(f"  - missing_log: {len(missing_records)} records")
    
    # duplicates_log
    dup_records = [
        {
            "edinet_code": d.edinet_code,
            "fiscal_year": d.fiscal_year,
            "chosen_tsv": d.chosen_tsv,
            "chosen_submit_datetime": d.chosen_submit_datetime,
            "skipped_tsv": d.skipped_tsv,
            "skipped_reason": d.skipped_reason,
        }
        for d in all_duplicate_logs
    ]
    if HAS_POLARS:
        pl.DataFrame(dup_records).write_csv(out_dir / "duplicates_log.csv")
    else:
        pd.DataFrame(dup_records).to_csv(out_dir / "duplicates_log.csv", index=False)
    
    print(f"  - duplicates_log: {len(dup_records)} records")
    
    # rec_debug_log
    rec_debug_records = [
        {
            "edinet_code": r.edinet_code,
            "fiscal_year": r.fiscal_year,
            "tsv_path": r.tsv_path,
            "is_consolidated": r.is_consolidated,
            "rec_rule": r.rec_rule,
            "rec_base_end_t": r.rec_base_end_t,
            "rec_base_end_t1": r.rec_base_end_t1,
            "rec_eclaims_end_t": r.rec_eclaims_end_t,
            "rec_eclaims_end_t1": r.rec_eclaims_end_t1,
            "rec_plus_e_end_t": r.rec_plus_e_end_t,
            "rec_plus_e_end_t1": r.rec_plus_e_end_t1,
            "rec_used_base_eid_end_t": r.rec_used_base_eid_end_t,
            "rec_used_base_eid_end_t1": r.rec_used_base_eid_end_t1,
            "rec_used_eclaims_eid_end_t": r.rec_used_eclaims_eid_end_t,
            "rec_used_eclaims_eid_end_t1": r.rec_used_eclaims_eid_end_t1,
        }
        for r in all_rec_debug_logs
    ]
    rec_debug_columns = [
        "edinet_code",
        "fiscal_year",
        "tsv_path",
        "is_consolidated",
        "rec_rule",
        "rec_base_end_t",
        "rec_base_end_t1",
        "rec_eclaims_end_t",
        "rec_eclaims_end_t1",
        "rec_plus_e_end_t",
        "rec_plus_e_end_t1",
        "rec_used_base_eid_end_t",
        "rec_used_base_eid_end_t1",
        "rec_used_eclaims_eid_end_t",
        "rec_used_eclaims_eid_end_t1",
    ]
    if HAS_POLARS:
        if rec_debug_records:
            pl.DataFrame(rec_debug_records).write_csv(out_dir / "rec_debug_log.csv")
        else:
            pl.DataFrame({c: [] for c in rec_debug_columns}).write_csv(out_dir / "rec_debug_log.csv")
    else:
        if rec_debug_records:
            pd.DataFrame(rec_debug_records).to_csv(out_dir / "rec_debug_log.csv", index=False)
        else:
            pd.DataFrame(columns=rec_debug_columns).to_csv(out_dir / "rec_debug_log.csv", index=False)
    
    print(f"  - rec_debug_log: {len(rec_debug_records)} records")
    
    print("\nDone!")


if __name__ == "__main__":
    main()


# =============================================================================
# 実行例
# =============================================================================
# python scripts/build_mj_inputs.py --target_companies target_companies.csv
# python scripts/build_mj_inputs.py --annual_dir C:\dev\edinet2dataset\edinet_corpus\annual --num_workers 8
# python scripts/build_mj_inputs.py --out_dir output --num_workers 16

