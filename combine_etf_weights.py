# -*- coding: utf-8 -*-
"""
批量读取多个 ETF 成分股表，清洗权重列，按“ETF 等权”（或自定义）合并为整体持仓权重。
仅依赖 pandas + openpyxl；对 WPS 等导出的“坏样式 .xlsx”内置修复读取。
"""

from __future__ import annotations
from pathlib import Path
import pandas as pd
import zipfile, tempfile, os
from typing import Dict, Optional

# ========= 配置（按需修改） =========
DATA_FOLDER = Path("/Users/yangliping/Documents/etf_data")  # 你的数据目录
SHEET_NAME  = 0                                              # 不确定表名用 0；或 "Sheet1"
CODE_COL    = "代码"                                          # 成分股代码列
WEIGHT_COL  = "估算权重"                                       # 成分股在该 ETF 中的权重列

# ETF 在你投资组合中的权重：
#   - 均等：ETF_WEIGHTS = "equal"
#   - 自定义：ETF_WEIGHTS = {"159338成分股":0.2,"510050成分股":0.3,...}  # 和不必=1，会自动归一化
ETF_WEIGHTS = "equal"

OUTPUT_CSV  = DATA_FOLDER / "portfolio_component_weights.csv"
TOPN        = 20                                             # 控制台展示前 N 名
PCT_OUTPUT  = True                                           # 另出百分比列

# ========= 工具 =========
def _read_xlsx_without_styles(path: Path, sheet) -> pd.DataFrame:
    """把 .xlsx 当 zip 去掉 xl/styles.xml 与 xl/theme* 后再读，绕过坏样式。"""
    with zipfile.ZipFile(path, "r") as zin, tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        with zipfile.ZipFile(tmp.name, "w", compression=zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                if item.filename in ("xl/styles.xml",) or item.filename.startswith("xl/theme"):
                    continue
                zout.writestr(item, zin.read(item.filename))
        tmp_path = tmp.name
    try:
        return pd.read_excel(tmp_path, sheet_name=sheet, engine="openpyxl")
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

def _normalize_code(s: pd.Series) -> pd.Series:
    """代码列：转字符串、去两端空格、去除末尾 .0（防止被当作浮点）。"""
    return (
        s.astype(str)
         .str.strip()
         .str.replace(r"\.0$", "", regex=True)
    )

def _to_fraction(s: pd.Series) -> pd.Series:
    """把权重统一成 0~1：支持 '12.3%' / '12.3' / 0.123 / 空值。"""
    s1 = s.astype(str).str.strip().str.replace("%", "", regex=False)
    s1 = pd.to_numeric(s1, errors="coerce")
    maxv = s1.max(skipna=True)
    if pd.notna(maxv) and maxv > 1.0:  # 形如 12.3 → 0.123
        s1 = s1 / 100.0
    return s1.fillna(0.0)

# ========= 读取单个文件 =========
def read_one(path: Path) -> Optional[pd.DataFrame]:
    try:
        if path.suffix.lower() == ".csv":
            df = pd.read_csv(path, dtype={CODE_COL: str})
        else:
            df = pd.read_excel(
                path, sheet_name=SHEET_NAME, engine="openpyxl",
                dtype={CODE_COL: str}, engine_kwargs={"data_only": True}
            )
    except Exception:
        try:
            df = _read_xlsx_without_styles(path, SHEET_NAME)
            if CODE_COL in df.columns:
                df[CODE_COL] = df[CODE_COL].astype(str)
        except Exception as e:
            print(f"❌ 读取失败：{path.name} | {e}")
            return None

    if CODE_COL not in df.columns or WEIGHT_COL not in df.columns:
        print(f"⚠️  {path.name} 缺少列：需要[{CODE_COL}, {WEIGHT_COL}]，已有：{list(df.columns)[:10]} ...")
        return None

    out = df[[CODE_COL, WEIGHT_COL]].copy()
    out[CODE_COL]   = _normalize_code(out[CODE_COL])
    out[WEIGHT_COL] = _to_fraction(out[WEIGHT_COL])

    # 过滤空代码/零权重，合并重复代码
    out = out[(out[CODE_COL] != "") & (out[WEIGHT_COL] > 0)]
    out = out.groupby(CODE_COL, as_index=False)[WEIGHT_COL].sum()
    return out

# ========= 批量读取 =========
def load_all(folder: Path) -> Dict[str, pd.DataFrame]:
    files = sorted(list(folder.glob("*.xlsx")) + list(folder.glob("*.csv")))
    print(f"📂 {folder} 文件数：{len(files)}")
    tables: Dict[str, pd.DataFrame] = {}
    for p in files:
        df = read_one(p)
        if df is not None and not df.empty:
            name = p.stem  # 例：'159338成分股'
            tables[name] = df
            print(f"  ✔ {p.name}: {df.shape[0]} 行")
        else:
            print(f"  ✖ 跳过 {p.name}")
    return tables

# ========= 合并：Σ(权重_成分股|ETF × 权重_ETF|组合) =========
def normalize_weights(etf_names: list[str], setting: str | dict[str, float]) -> dict[str, float]:
    if setting == "equal":
        n = len(etf_names) or 1
        return {name: 1.0 / n for name in etf_names}
    w = {name: float(setting.get(name, 0.0)) for name in etf_names}
    s = sum(w.values())
    if s <= 0:
        raise ValueError("ETF_WEIGHTS 自定义后总权重为 0，请正确设置。")
    return {k: v / s for k, v in w.items()}  # 归一化到和=1

def combine_weighted(tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    if not tables:
        return pd.DataFrame(columns=[CODE_COL, "total_weight"])

    etf_names = list(tables.keys())
    w = normalize_weights(etf_names, ETF_WEIGHTS)

    print("\n🧮 使用的 ETF 权重：")
    for k in etf_names:
        print(f"   {k}: {w.get(k, 0):.6f}")

    # 以代码为键做外连接，列名改成各 ETF 名
    merged = None
    for name, df in tables.items():
        tmp = df.rename(columns={WEIGHT_COL: name})
        merged = tmp if merged is None else pd.merge(merged, tmp, how="outer", on=CODE_COL)
    merged = merged.fillna(0.0)

    etf_cols = [c for c in merged.columns if c != CODE_COL]

    # total_weight = Σ(列 * 对应 ETF 权重)
    merged["total_weight"] = 0.0
    for c in etf_cols:
        merged["total_weight"] += merged[c] * w.get(c, 0.0)

    merged["appear_in"] = (merged[etf_cols] > 0).sum(axis=1)
    merged = merged.sort_values("total_weight", ascending=False).reset_index(drop=True)

    if PCT_OUTPUT:
        merged["total_weight_pct"] = (merged["total_weight"] * 100).round(4)

    cols = [CODE_COL, "total_weight"]
    if PCT_OUTPUT:
        cols.append("total_weight_pct")
    cols += ["appear_in"] + etf_cols
    return merged[cols]

# ========= 主流程 =========
def main():
    tables = load_all(DATA_FOLDER)
    if not tables:
        print("❌ 没有读到有效成分股表。")
        return
    result = combine_weighted(tables)
    result.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"\n✅ 已保存：{OUTPUT_CSV}")

    topn = min(TOPN, len(result))
    if topn > 0:
        print(f"\n📈 前 {topn} 大持仓：")
        with pd.option_context("display.max_rows", topn, "display.max_columns", None):
            print(result.head(topn))

if __name__ == "__main__":
    main()
