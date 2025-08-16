# notify/notify.py
import os, glob, json, re
from pathlib import Path
import pandas as pd
import numpy as np
from zoneinfo import ZoneInfo

OUT_DIR = Path("out"); OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH = OUT_DIR / "anomaly_email.md"

# Luôn dùng UTC+7 (Asia/Ho_Chi_Minh) để hiển thị
LOCAL_TZ_NAME = os.getenv("LOCAL_TZ", "Asia/Ho_Chi_Minh")
LOCAL_TZ = ZoneInfo(LOCAL_TZ_NAME)

ALIASES = {
    "city": ["city", "thanh_pho", "tinh", "province", "location"],
    "time": ["time", "timestamp", "datetime", "date", "DateTime", "created_at"],
    "aqi":  ["AQI", "aqi", "aqi_value"],
    "wind": ["wind_speed", "wind", "windspeed", "gio", "gió"],
    "flag": ["is_anomaly", "anomaly_flag", "flag", "label", "is_outlier"],
    "method": ["method", "algo", "algorithm", "detector", "source"],
}

TRUE_STRS = {"1","true","yes","y","t","on"}

def pick_col(df, candidates):
    cols = {c.lower(): c for c in df.columns}
    for n in candidates:
        if n in df.columns: return n
        if n.lower() in cols: return cols[n.lower()]
    for c in df.columns:
        if any(n.lower() in c.lower() for n in candidates): return c
    return None

def num_from_str(x):
    if pd.isna(x): return np.nan
    m = re.search(r"[-+]?\d*\.?\d+", str(x))
    return float(m.group()) if m else np.nan

def coerce_time(s):
    # Chuẩn về UTC tz-aware; nếu đã có tz, pandas sẽ giữ đúng tz
    # Sau đó khi render sẽ convert sang LOCAL_TZ (UTC+7)
    try:
        ts = pd.to_datetime(s, errors="coerce", utc=True)
    except Exception:
        ts = pd.to_datetime(s, errors="coerce")
    return ts

def infer_city_from_path(p: str):
    name = Path(p).stem  # ví dụ: anomalies_ho_chi_minh_2025  /  ho_chi_minh_zscore
    # ISO: anomalies_<city>_<year>
    m = re.match(r"anomalies_(.+?)_\d{4}$", name)
    if m: return m.group(1)
    # Z-score: <city>_zscore
    m = re.match(r"(.+?)_zscore$", name)
    if m: return m.group(1)
    # Fallback: tên thư mục trước stem
    parts = Path(p).parts
    for i in range(len(parts)-1, -1, -1):
        if parts[i] in {"z_score","isolation_forest"} and i+1 < len(parts)-1:
            return parts[i+1]
    return name

def infer_method_from_path(p: str):
    p_low = p.replace("\\","/").lower()
    if "/z_score/" in p_low or p_low.endswith("_zscore.csv"):
        return "Z-score"
    if "/isolation_forest/" in p_low or p_low.startswith("anomalies_"):
        return "IsolationForest"
    return ""

def read_csv_any(p):
    try:
        df = pd.read_csv(p)
        df["__source"] = p
        return df
    except Exception as e:
        print(f"[WARN] skip {p}: {e}")
        return None

def read_json_any(p):
    try:
        obj = json.loads(Path(p).read_text(encoding="utf-8"))
        rows = obj["data"] if isinstance(obj, dict) and isinstance(obj.get("data"), list) else (
               obj if isinstance(obj, list) else None)
        if rows is None: return None
        df = pd.DataFrame(rows); df["__source"] = p; return df
    except Exception as e:
        print(f"[WARN] skip {p}: {e}")
        return None

def build_flag_cols(df: pd.DataFrame):
    # Kết hợp điều kiện “bất thường” cho cả 2 detector
    flag = np.zeros(len(df), dtype=bool)

    # Isolation Forest: anomaly == -1
    if "anomaly" in df.columns:
        try:
            flag = flag | (pd.to_numeric(df["anomaly"], errors="coerce") == -1)
        except Exception:
            flag = flag | (df["anomaly"].astype(str).str.strip() == "-1")

    # Z-score: zscore_flag_aqi == -1 hoặc zscore_flag_wind == -1
    for col in ["zscore_flag_aqi","zscore_flag_wind"]:
        if col in df.columns:
            flag = flag | (pd.to_numeric(df[col], errors="coerce") == -1)

    # Generic boolean flags nếu có
    for col in ALIASES["flag"]:
        if col in df.columns and col not in {"anomaly","zscore_flag_aqi","zscore_flag_wind"}:
            flag = flag | df[col].astype(str).str.lower().isin(TRUE_STRS)

    return flag

def to_canonical(df: pd.DataFrame):
    c_city = pick_col(df, ALIASES["city"])
    c_time = pick_col(df, ALIASES["time"])
    c_aqi  = pick_col(df, ALIASES["aqi"])
    c_wind = pick_col(df, ALIASES["wind"])
    c_meth = pick_col(df, ALIASES["method"])

    if c_time is None:
        return None  # bắt buộc cần time để chọn bản ghi mới nhất

    out = pd.DataFrame()
    # city
    if c_city is not None:
        out["city"] = df[c_city].astype(str)
    else:
        # lấy từ __source
        out["city"] = df["__source"].apply(infer_city_from_path)

    # time
    out["time"] = coerce_time(df[c_time])

    # AQI
    if c_aqi is not None:
        out["aqi"] = pd.to_numeric(df[c_aqi], errors="coerce")

    # wind (chuẩn hoá numeric kể cả khi chứa “ km/h”)
    if c_wind is not None:
        out["wind"] = df[c_wind].apply(num_from_str)

    # method
    if c_meth is not None:
        out["method"] = df[c_meth].astype(str)
    else:
        out["method"] = df["__source"].apply(infer_method_from_path)

    # flag
    out["flag"] = build_flag_cols(df)
    out["__source"] = df["__source"]

    # bỏ các dòng time không parse được
    out = out.dropna(subset=["time"])
    return out

def main():
    # Danh sách file được thay đổi (nếu có), nếu không thì quét toàn bộ result_anomaly
    changed = [f for f in os.getenv("CHANGED_FILES","").splitlines() if f.strip()]
    if not changed:
        changed = glob.glob("result_anomaly/**/*", recursive=True)

    paths = [p for p in changed if p.lower().endswith((".csv",".json"))]
    if not paths:
        print("No anomaly files to process."); return

    frames=[]
    for p in paths:
        df = read_csv_any(p) if p.lower().endswith(".csv") else read_json_any(p)
        if df is None or df.empty: 
            continue
        canon = to_canonical(df)
        if canon is not None and not canon.empty:
            frames.append(canon)

    if not frames:
        print("No usable rows."); return

    big = pd.concat(frames, ignore_index=True)
    big = big[big["flag"] == True].copy()
    if big.empty:
        print("No positive anomalies by current detectors."); return

    # Sắp xếp theo thời gian, lấy bản ghi mới nhất mỗi city
    big = big.sort_values("time")
    latest = big.groupby("city", as_index=False).tail(1)

    # Tạo label UTC+offset cố định (UTC+7)
    offset_hours = int((pd.Timestamp.now(tz=LOCAL_TZ).utcoffset().total_seconds() // 3600))
    utc_label = f"UTC+{offset_hours:+d}".replace("++", "+")

    now_local = pd.Timestamp.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M %Z")
    lines = [f"#  Cảnh báo bất thường AQI/Gió — {now_local}",
             "",
             f"| Thành phố | Thời điểm ({utc_label}) | AQI | Gió | Phương pháp | Nguồn |",
             "|---|---:|---:|---:|---|---|"]

    for _, r in latest.iterrows():
        ts = (pd.to_datetime(r["time"], utc=True)
                .tz_convert(LOCAL_TZ)
                .strftime("%Y-%m-%d %H:%M"))
        aqi  = "" if pd.isna(r.get("aqi")) else f"{r['aqi']:.0f}"
        wind = "" if pd.isna(r.get("wind")) else f"{r['wind']:.2f}"
        meth = "" if pd.isna(r.get("method")) else str(r["method"])
        src  = r.get("__source","")
        lines.append(f"| {r['city']} | {ts} | {aqi} | {wind} | {meth} | {src} |")

    OUT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT_PATH}")

if __name__ == "__main__":
    main()
