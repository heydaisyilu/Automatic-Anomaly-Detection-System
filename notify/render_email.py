# notify/notify.py
import os, glob, json, re
from pathlib import Path
import pandas as pd
import numpy as np
from zoneinfo import ZoneInfo

OUT_DIR = Path("out"); OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH = OUT_DIR / "anomaly_email.md"

# Hiển thị theo UTC+7 (có thể đổi bằng env LOCAL_TZ)
LOCAL_TZ_NAME = os.getenv("LOCAL_TZ", "Asia/Ho_Chi_Minh")
LOCAL_TZ = ZoneInfo(LOCAL_TZ_NAME)

# Cửa sổ thời gian tính từ hiện tại (giờ)
RECENT_HOURS = int(os.getenv("RECENT_HOURS", "3"))

# Alias các cột thường gặp
ALIASES = {
    "city": ["city", "thanh_pho", "tinh", "province", "location"],
    "time": ["time", "timestamp", "datetime", "date", "DateTime", "created_at"],
    "aqi":  ["AQI", "aqi", "aqi_value"],
    "wind": ["wind_speed", "wind", "windspeed", "gio", "gió"],
}

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
    # Chuẩn tz-aware về UTC; nếu input đã có tz, pandas sẽ convert
    try: return pd.to_datetime(s, errors="coerce", utc=True)
    except: return pd.to_datetime(s, errors="coerce")

def infer_city_from_path(p: str):
    name = Path(p).stem
    m = re.match(r"anomalies_(.+?)_\d{4}$", name)   # isolation_forest: anomalies_<city>_YYYY
    if m: return m.group(1)
    m = re.match(r"(.+?)_zscore$", name)           # z_score: <city>_zscore
    if m: return m.group(1)
    return name

def detect_method_from_path(p: str):
    pl = p.replace("\\","/").lower()
    if "/z_score/" in pl or pl.endswith("_zscore.csv"): return "Z-score"
    if "/isolation_forest/" in pl or pl.startswith("anomalies_"): return "IsolationForest"
    return "Unknown"

def read_csv_any(p):
    try:
        df = pd.read_csv(p); df["__source"] = p; return df
    except Exception as e:
        print(f"[WARN] skip {p}: {e}"); return None

def read_json_any(p):
    try:
        obj = json.loads(Path(p).read_text(encoding="utf-8"))
        rows = obj["data"] if isinstance(obj, dict) and isinstance(obj.get("data"), list) else (obj if isinstance(obj, list) else None)
        if rows is None: return None
        df = pd.DataFrame(rows); df["__source"] = p; return df
    except Exception as e:
        print(f"[WARN] skip {p}: {e}"); return None

def zscore_label(row):
    tags = []
    if "zscore_flag_aqi" in row.index:
        try:
            if pd.to_numeric(row["zscore_flag_aqi"], errors="coerce") == -1: tags.append("AQI")
        except: pass
    if "zscore_flag_wind" in row.index:
        try:
            if pd.to_numeric(row["zscore_flag_wind"], errors="coerce") == -1: tags.append("Wind")
        except: pass
    if not tags: return "", False
    if len(tags) == 2: return "Z-score[AQI,Wind]", True
    return f"Z-score[{tags[0]}]", True

def iso_hit(row):
    if "anomaly" not in row.index: return False
    try:
        return pd.to_numeric(row["anomaly"], errors="coerce") == -1
    except:
        return str(row["anomaly"]).strip() == "-1"

def to_rows(df: pd.DataFrame):
    """
    Trả về list các hàng chuẩn hoá (city, time, aqi, wind, method, source) CHỈ CHO NHỮNG DÒNG BẤT THƯỜNG.
    - Z-score: dựa vào cờ zscore_flag_aqi / zscore_flag_wind.
    - ISO: anomaly == -1.
    """
    rows = []
    c_city = pick_col(df, ALIASES["city"])
    c_time = pick_col(df, ALIASES["time"])
    c_aqi  = pick_col(df, ALIASES["aqi"])
    c_wind = pick_col(df, ALIASES["wind"])

    if c_time is None:
        return rows  # không có thời gian thì bỏ qua để đảm bảo "mới nhất" theo hiện tại

    for _, r in df.iterrows():
        method_from_path = detect_method_from_path(r["__source"]) if "__source" in df.columns else "Unknown"

        # Z-score: chỉ xét nếu file thuộc Z-score hoặc có cờ zscore_*
        zs_label, zs_is_hit = zscore_label(r) if (method_from_path == "Z-score" or any(k in r.index for k in ["zscore_flag_aqi","zscore_flag_wind"])) else ("", False)
        # ISO
        iso_is_hit = iso_hit(r) if (method_from_path == "IsolationForest" or "anomaly" in r.index) else False

        # union: bất cứ cái nào hit đều báo
        if not (zs_is_hit or iso_is_hit):
            continue

        # city
        city = str(r[c_city]) if c_city is not None else infer_city_from_path(r["__source"])
        # time
        t = coerce_time(r[c_time])
        if pd.isna(t):  # cần thời gian hợp lệ
            continue
        # aqi
        aqi = pd.to_numeric(r[c_aqi], errors="coerce") if c_aqi is not None else np.nan
        # wind (có thể là chuỗi '12 km/h')
        wind = num_from_str(r[c_wind]) if c_wind is not None else np.nan
        # method label
        if zs_is_hit and iso_is_hit:
            method = "Z-score & IsolationForest"
        elif zs_is_hit:
            method = zs_label
        else:
            method = "IsolationForest"

        rows.append({
            "city": city,
            "time_utc": pd.to_datetime(t, utc=True),
            "aqi": aqi,
            "wind": wind,
            "method": method,
            "__source": r.get("__source","")
        })
    return rows

def main():
    # Lấy danh sách file thay đổi (nếu có); nếu không, quét toàn bộ result_anomaly
    changed = [f for f in os.getenv("CHANGED_FILES","").splitlines() if f.strip()]
    if not changed:
        changed = glob.glob("result_anomaly/**/*", recursive=True)

    paths = [p for p in changed if p.lower().endswith((".csv",".json"))]
    if not paths:
        print("No anomaly files to process."); return

    all_rows = []
    for p in paths:
        df = read_csv_any(p) if p.lower().endswith(".csv") else read_json_any(p)
        if df is None or df.empty: 
            continue
        # đảm bảo có __source
        if "__source" not in df.columns:
            df["__source"] = p
        rows = to_rows(df)
        all_rows.extend(rows)

    if not all_rows:
        print("No positive anomalies by detectors."); return

    big = pd.DataFrame(all_rows)
    # Lọc theo thời gian hiện tại trong cửa sổ RECENT_HOURS
    now_utc = pd.Timestamp.utcnow().tz_localize("UTC")
    cutoff  = now_utc - pd.Timedelta(hours=RECENT_HOURS)
    big = big[big["time_utc"] >= cutoff]
    if big.empty:
        print(f"No anomalies in the last {RECENT_HOURS} hours."); return

    # Sắp xếp mới → cũ (gần hiện tại trước)
    big = big.sort_values("time_utc", ascending=False).reset_index(drop=True)

    # Render
    offset_hours = int(pd.Timestamp.now(tz=LOCAL_TZ).utcoffset().total_seconds() // 3600)
    utc_label = f"UTC+{offset_hours:+d}".replace("++","+")
    now_local = pd.Timestamp.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M %Z")

    lines = [f"#  Cảnh báo bất thường AQI/Gió — {now_local}",
             "",
             f"| Thành phố | Thời điểm ({utc_label}) | AQI | Gió | Phương pháp | Nguồn |",
             "|---|---:|---:|---:|---|---|"]

    for _, r in big.iterrows():
        ts = r["time_utc"].tz_convert(LOCAL_TZ).strftime("%Y-%m-%d %H:%M")
        aqi  = "" if pd.isna(r.get("aqi")) else f"{r['aqi']:.0f}"
        wind = "" if pd.isna(r.get("wind")) else f"{r['wind']:.2f}"
        meth = r.get("method", "")
        src  = r.get("__source","")
        lines.append(f"| {r['city']} | {ts} | {aqi} | {wind} | {meth} | {src} |")

    OUT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT_PATH}")

if __name__ == "__main__":
    main()
