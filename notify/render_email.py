import os, glob, json
from pathlib import Path
import pandas as pd
import numpy as np

OUT_DIR = Path("out"); OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH = OUT_DIR / "anomaly_email.md"

ASSUME_TZ = os.getenv("ASSUME_TZ", "Asia/Ho_Chi_Minh")

# ---- Helpers ----
ALIASES = {
    "city": ["city", "thanh_pho", "tinh", "province", "location"],
    "time": ["timestamp","time","datetime","date"],
    "aqi":  ["AQI","aqi","aqi_value"],
    "wind": ["wind_speed","wind","windspeed","gio","gió"],
    "flag": ["flag","is_anomaly","anomaly","is_outlier","label"],
    "method":["method","algo","algorithm","detector","source"],
}

def pick_col(df, cands):
    cols = {c.lower(): c for c in df.columns}
    for n in cands:
        if n in df.columns: return n
        if n.lower() in cols: return cols[n.lower()]
    for c in df.columns:
        if any(n.lower() in c.lower() for n in cands): return c
    return None

def read_csv(p):
    try:
        df = pd.read_csv(p)
        df["__source"] = p
        return df
    except Exception as e:
        print(f"[WARN] skip {p}: {e}"); return None

def read_json(p):
    try:
        obj = json.loads(Path(p).read_text(encoding="utf-8"))
        rows = obj["data"] if isinstance(obj, dict) and isinstance(obj.get("data"), list) else (obj if isinstance(obj, list) else None)
        if rows is None: return None
        df = pd.DataFrame(rows); df["__source"] = p
        return df
    except Exception as e:
        print(f"[WARN] skip {p}: {e}"); return None

def parse_flag(series):
    # Chuẩn chung: numeric == -1 là bất thường; string: một số token hay dùng
    sn = pd.to_numeric(series, errors="coerce")
    out = pd.Series(False, index=series.index)
    mask = sn.notna()
    out[mask] = (sn[mask] == -1)
    st = series.astype(str).str.strip().str.lower()
    tokens = {"-1","true","yes","y","t","anomaly","outlier","abnormal","alert"}
    out[~mask] = st[~mask].isin(tokens)
    return out

def to_epoch_utc(series):
    # Dùng để so sánh "mốc mới nhất" (không dùng để hiển thị)
    dt = pd.to_datetime(series, errors="coerce", utc=False)
    if hasattr(dt, "dt"):
        try:
            has_tz = dt.dt.tz is not None
        except Exception:
            has_tz = False
        if not has_tz:
            # Naive -> localize theo ASSUME_TZ để tránh lệch khi so sánh
            dt = dt.dt.tz_localize(ASSUME_TZ)
        dt = dt.dt.tz_convert("UTC")
        return dt.view("int64")  # ns since epoch
    return pd.to_datetime(series, errors="coerce", utc=True).view("int64")

def detect_methods_row(df_row):
    methods = []
    if "zscore_flag_aqi" in df_row and parse_flag(pd.Series([df_row["zscore_flag_aqi"]])).iloc[0]:
        methods.append("Z-score AQI")
    if "zscore_flag_wind" in df_row and parse_flag(pd.Series([df_row["zscore_flag_wind"]])).iloc[0]:
        methods.append("Z-score Wind")
    if "anomaly" in df_row and parse_flag(pd.Series([df_row["anomaly"]])).iloc[0]:
        methods.append("IsolationForest")
    # generic flag (nếu có)
    c_flag = pick_col(pd.DataFrame([df_row]), ALIASES["flag"])
    if c_flag and c_flag not in ["zscore_flag_aqi","zscore_flag_wind","anomaly"]:
        if parse_flag(pd.Series([df_row[c_flag]])).iloc[0]:
            methods.append("GenericFlag")
    return methods

def canonicalize(df):
    c_city = pick_col(df, ALIASES["city"])
    c_time = pick_col(df, ALIASES["time"])
    if not c_city or not c_time: return None

    c_aqi  = pick_col(df, ALIASES["aqi"])
    c_wind = pick_col(df, ALIASES["wind"])

    out = pd.DataFrame()
    out["city"] = df[c_city].astype(str)
    out["time_raw"] = df[c_time].astype(str)  # giữ nguyên để hiển thị
    out["time_key"] = to_epoch_utc(df[c_time])  # để so sánh mốc mới nhất

    # Lưu phiên bản datetime chuyển về Asia/Ho_Chi_Minh để hiển thị
    dt = pd.to_datetime(df[c_time], errors="coerce", utc=False)
    if hasattr(dt, "dt"):
        try:
            has_tz = dt.dt.tz is not None
        except Exception:
            has_tz = False
        if not has_tz:
            dt = dt.dt.tz_localize(ASSUME_TZ)
        dt = dt.dt.tz_convert(ASSUME_TZ)
        out["time_vn"] = dt.dt.strftime("%Y-%m-%d %H:%M")
    else:
        out["time_vn"] = df[c_time].astype(str)

    if c_aqi:  out["aqi"]  = pd.to_numeric(df[c_aqi], errors="coerce")
    if c_wind:
        # lấy số nếu có đơn vị dạng "12 km/h"
        w = df[c_wind].astype(str).str.extract(r"([\d.]+)", expand=False)
        out["wind"] = pd.to_numeric(w, errors="coerce")

    # flag + method
    out["__source"] = df["__source"]
    # Tạo cờ tổng hợp nhanh theo từng hàng
    flags = []
    methods = []
    for _, row in df.iterrows():
        ms = detect_methods_row(row)
        methods.append(", ".join(ms))
        flags.append(bool(ms))
    out["method"] = methods
    out["flag"] = flags
    return out.dropna(subset=["time_key"])

# ---- Main ----
def main():
    raw = os.getenv("CHANGED_FILES","").replace("%0D","").replace("%0A","\n").replace("%25","%")
    parts = []
    for ln in raw.replace(",", "\n").splitlines():
        ln = ln.strip()
        if ln:
            parts.append(ln)

    paths = parts
    if not paths:
        paths = []
        paths += glob.glob("result_anomaly/**/*.csv", recursive=True)
        paths += glob.glob("result_anomaly/**/*.json", recursive=True)
        # nếu muốn render luôn notebook đã execute
        nb = Path("detection/detection_output.ipynb")
        if nb.exists():
            paths.append(str(nb))

    paths = [p for p in paths if p.lower().endswith((".csv",".json")) and Path(p).exists()]
    if not paths:
        print("No anomaly files."); return


    frames = []
    for p in paths:
        df = read_csv(p) if p.lower().endswith(".csv") else read_json(p)
        if df is None or df.empty: continue
        cdf = canonicalize(df)
        if cdf is not None and not cdf.empty: frames.append(cdf)

    if not frames:
        print("No usable rows."); return

    big = pd.concat(frames, ignore_index=True)

    # 1) Lấy mốc thời gian MỚI NHẤT toàn cục (UTC epoch)
    latest_key = big["time_key"].max()

    # 2) Giữ nguyên CHỈ các dòng thuộc mốc đó & có flag bất thường
    cur = big[(big["time_key"] == latest_key) & (big["flag"] == True)].copy()
    if cur.empty:
        print("No anomalies at latest timestamp."); return

    # 3) Gộp theo (city, time_raw, time_vn) để hợp nhất trùng nhau giữa các detector
    def agg_first_nonnull(s): 
        return next((x for x in s if pd.notna(x) and str(x) != ""), "")
    cur = (cur
           .groupby(["city","time_raw","time_vn"], as_index=False)
           .agg({
               "aqi": agg_first_nonnull,
               "wind": agg_first_nonnull,
               "method": lambda s: ", ".join(sorted(set(", ".join(m.split(", ")).strip() for m in s if m))),
               "__source": lambda s: "; ".join(sorted(set(s)))
           }))

    # 4) Render bảng: hiển thị thời gian theo Asia/Ho_Chi_Minh
    lines = [
        f"#  Cảnh báo bất thường AQI/Gió (tại mốc mới nhất)",
        "",
        "|  Thành phố  | Thời điểm (UTC+7) | AQI | Gió | Phương pháp | Nguồn |",
        "|-------------|-------------------|-----|-----|-------------|-------|",
    ]
    for _, r in cur.sort_values(["city", "time_vn"]).iterrows():
        aqi  = "" if pd.isna(r.get("aqi")) else f"{float(r['aqi']):.0f}"
        wind = "" if pd.isna(r.get("wind")) else f"{float(r['wind']):.2f}"
        meth = r.get("method","")
        src  = r.get("__source","")
        time_vn = r.get("time_vn", r.get("time_raw", ""))
        lines.append(f"| {r['city']} | {time_vn} | {aqi} | {wind} | {meth} | {src} |")

    OUT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT_PATH}")

if __name__ == "__main__":
    main()