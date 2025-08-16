# notify/render_email.py
import os, glob, json
from pathlib import Path
import pandas as pd
import numpy as np

OUT_DIR = Path("out"); OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH = OUT_DIR / "anomaly_email.md"

ALIASES = {
    "city": ["city", "thanh_pho", "tinh", "province", "location"],
    "time": ["time", "timestamp", "datetime", "date"],
    "aqi": ["aqi", "AQI", "aqi_value"],
    "wind": ["wind", "wind_speed", "windspeed", "gio", "gió"],
    "flag": ["is_anomaly", "anomaly", "flag", "is_outlier", "label"],
    "method": ["method", "algo", "algorithm", "detector", "source"],
}

def pick_col(df, names):
    cols = {c.lower(): c for c in df.columns}
    for n in names:
        if n in df.columns: return n
        if n.lower() in cols: return cols[n.lower()]
    for c in df.columns:
        if any(n in c.lower() for n in names): return c
    return None

def coerce_time(s):
    try: return pd.to_datetime(s, errors="coerce", utc=True)
    except: return pd.to_datetime(s, errors="coerce")

def read_csv_any(p):
    try: df = pd.read_csv(p); df["__source"] = p; return df
    except Exception as e: print(f"[WARN] skip {p}: {e}"); return None

def read_json_any(p):
    try:
        obj = json.loads(Path(p).read_text(encoding="utf-8"))
        rows = obj["data"] if isinstance(obj, dict) and isinstance(obj.get("data"), list) else (obj if isinstance(obj, list) else None)
        if rows is None: return None
        df = pd.DataFrame(rows); df["__source"] = p; return df
    except Exception as e: print(f"[WARN] skip {p}: {e}"); return None

def to_canonical(df):
    c_city = pick_col(df, ALIASES["city"])
    c_time = pick_col(df, ALIASES["time"])
    c_aqi  = pick_col(df, ALIASES["aqi"])
    c_wind = pick_col(df, ALIASES["wind"])
    c_flag = pick_col(df, ALIASES["flag"])
    c_meth = pick_col(df, ALIASES["method"])
    if c_city is None or c_time is None: return None
    out = pd.DataFrame()
    out["city"] = df[c_city].astype(str)
    out["time"] = coerce_time(df[c_time])
    if c_aqi in df:  out["aqi"] = pd.to_numeric(df[c_aqi], errors="coerce")
    if c_wind in df: out["wind"] = pd.to_numeric(df[c_wind], errors="coerce")
    out["flag"] = (df[c_flag].astype(str).str.lower().isin(["1","true","yes","y","t"])) if c_flag in df else True
    out["method"] = df[c_meth] if c_meth in df else np.nan
    out["__source"] = df["__source"]
    return out.dropna(subset=["time"])

def main():
    changed = [f for f in os.getenv("CHANGED_FILES","").splitlines() if f.strip()]
    if not changed: changed = glob.glob("result_anomaly/**/*", recursive=True)
    paths = [p for p in changed if p.lower().endswith((".csv",".json"))]
    if not paths: print("No changed anomaly files."); return

    frames=[]
    for p in paths:
        df = read_csv_any(p) if p.endswith(".csv") else read_json_any(p)
        if df is None or df.empty: continue
        cdf = to_canonical(df)
        if cdf is not None and not cdf.empty: frames.append(cdf)

    if not frames: print("No usable rows."); return
    big = pd.concat(frames, ignore_index=True); big = big[big["flag"]==True].copy()
    if big.empty: print("No positive flags."); return

    big = big.sort_values("time")
    latest = big.groupby("city", as_index=False).tail(1)

    now = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"# 🚨 Cảnh báo bất thường AQI/Gió ({now})","",
             "| Thành phố | Thời điểm (UTC) | AQI | Gió | Phương pháp | Nguồn |",
             "|---|---:|---:|---:|---|---|"]
    for _, r in latest.iterrows():
        ts = pd.to_datetime(r["time"], utc=True).strftime("%Y-%m-%d %H:%M")
        aqi  = "" if pd.isna(r.get("aqi")) else f"{r['aqi']:.0f}"
        wind = "" if pd.isna(r.get("wind")) else f"{r['wind']:.2f}"
        meth = "" if pd.isna(r.get("method")) else str(r["method"])
        src  = r.get("__source","")
        lines.append(f"| {r['city']} | {ts} | {aqi} | {wind} | {meth} | {src} |")

    OUT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT_PATH}")

if __name__ == "__main__":
    main()
