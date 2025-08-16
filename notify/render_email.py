# notify/render_email.py
import os, glob, json, re
from pathlib import Path
import pandas as pd
import numpy as np

TZ = "Asia/Ho_Chi_Minh"  # UTC+7
OUT_DIR = Path("out"); OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH = OUT_DIR / "anomaly_email.md"

ALIASES = {
    "city":   ["city", "thanh_pho", "tinh", "province", "location"],
    "time":   ["time", "timestamp", "datetime", "date"],
    "aqi":    ["aqi", "AQI", "aqi_value"],
    "wind":   ["wind", "wind_speed", "windspeed", "gio", "gió"],
    "flag":   ["is_anomaly", "anomaly", "flag", "is_outlier", "label"],
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

def to_local_time(series):
    # Chuyển mọi thời gian về UTC+7 (Asia/Ho_Chi_Minh).
    # - Nếu chuỗi có timezone (vd +07:00), convert -> TZ
    # - Nếu chuỗi NAIVE, coi là TZ (localize)
    t = pd.to_datetime(series, errors="coerce", utc=False)
    if getattr(t.dt, "tz", None) is None:
        t = t.dt.tz_localize(TZ)
    else:
        t = t.dt.tz_convert(TZ)
    return t

def num_from_any(series):
    s = series.astype(str).str.extract(r"([-+]?\d*\.?\d+)")[0]
    return pd.to_numeric(s, errors="coerce")

def read_csv_any(p):
    for enc in (None, "utf-8", "utf-8-sig"):
        try:
            df = pd.read_csv(p) if enc is None else pd.read_csv(p, encoding=enc)
            df["__source"] = p
            return df
        except Exception:
            continue
    print(f"[WARN] skip {p}: cannot read csv"); return None

def read_json_any(p):
    try:
        obj = json.loads(Path(p).read_text(encoding="utf-8"))
        rows = obj["data"] if isinstance(obj, dict) and isinstance(obj.get("data"), list) \
               else (obj if isinstance(obj, list) else None)
        if rows is None: return None
        df = pd.DataFrame(rows); df["__source"] = p; return df
    except Exception as e:
        print(f"[WARN] skip {p}: {e}"); return None

def infer_city_series(df, source_path):
    c_city = pick_col(df, ALIASES["city"])
    if c_city:
        return df[c_city].astype(str)
    base = Path(source_path).name
    m = re.search(r"(anomalies_)?(.+?)(_zscore)?(_\d{4})?\.csv$", base, flags=re.I)
    city = m.group(2) if m else Path(source_path).stem
    return pd.Series([city] * len(df))

def to_canonical(df):
    c_time = pick_col(df, ALIASES["time"])
    c_aqi  = pick_col(df, ALIASES["aqi"])
    c_wind = pick_col(df, ALIASES["wind"])
    c_flag = pick_col(df, ALIASES["flag"])
    c_meth = pick_col(df, ALIASES["method"])

    out = pd.DataFrame()
    out["city"] = infer_city_series(df, df["__source"].iloc[0])

    if c_time is None: return None
    out["time"] = to_local_time(df[c_time])  # -> UTC+7

    if c_aqi in df:  out["aqi"]  = pd.to_numeric(df[c_aqi], errors="coerce")
    if c_wind in df: out["wind"] = num_from_any(df[c_wind])
    out["__source"] = df["__source"]

    # ---- flag (khớp detector hiện có) ----
    flag = None
    if c_flag in df:
        s = df[c_flag]
        if np.issubdtype(s.dtype, np.number):
            if c_flag.lower() == "anomaly" or c_flag == "anomaly":
                flag = (s.astype(float) == -1)  # IF: -1 là bất thường
            else:
                flag = (s.astype(float) != 0)
        else:
            flag = s.astype(str).str.strip().str.lower().isin(["1","-1","true","yes","y","t"])
    else:
        zcols = [c for c in df.columns if c.lower().startswith("zscore_flag")]
        if zcols:
            flag = pd.concat([(df[c].astype(str) == "-1") for c in zcols], axis=1).any(axis=1)
        elif "anomaly" in df.columns:
            flag = (pd.to_numeric(df["anomaly"], errors="coerce") == -1)

    out["flag"] = False if flag is None else flag

    # ---- method ----
    if c_meth in df:
        out["method"] = df[c_meth].astype(str)
    else:
        src = df["__source"].astype(str).str.lower()
        out["method"] = np.select(
            [src.str.contains("z_score"), src.str.contains("isolation_forest")],
            ["Z-score", "IsolationForest"],
            default=np.nan
        )

    out = out.dropna(subset=["time"]).copy()
    return out

def main():
    changed = [f for f in os.getenv("CHANGED_FILES","").splitlines() if f.strip()]
    if not changed:
        changed = glob.glob("result_anomaly/**/*", recursive=True)
    paths = [p for p in changed if p.lower().endswith((".csv",".json"))]
    if not paths:
        print("No anomaly files to scan."); return

    frames=[]
    for p in paths:
        df = read_csv_any(p) if p.endswith(".csv") else read_json_any(p)
        if df is None or df.empty: continue
        cdf = to_canonical(df)
        if cdf is not None and not cdf.empty: frames.append(cdf)

    if not frames:
        print("No usable rows."); return

    big = pd.concat(frames, ignore_index=True)
    big = big[big["flag"] == True].copy()
    if big.empty:
        print("No positive anomalies."); return

    # Lấy bản mới nhất theo (city, method)
    big = big.sort_values("time")
    latest = big.groupby(["city","method"], as_index=False).tail(1)

    now_local = pd.Timestamp.now(tz=TZ).strftime("%Y-%m-%d %H:%M (UTC+7)")
    lines = [
        f"# Cảnh báo bất thường AQI/Gió ({now_local})", "",
        "| Thành phố | Thời điểm (UTC+7) | AQI | Gió | Phương pháp | Nguồn |",
        "|---|---:|---:|---:|---|---|"
    ]
    for _, r in latest.iterrows():
        ts = pd.to_datetime(r["time"]).tz_convert(TZ).strftime("%Y-%m-%d %H:%M")
        aqi  = "" if pd.isna(r.get("aqi")) else f"{r['aqi']:.0f}"
        wind = "" if pd.isna(r.get("wind")) else f"{r['wind']:.2f}"
        meth = "" if pd.isna(r.get("method")) else str(r["method"])
        src  = r.get("__source","")
        city = str(r.get("city","")).strip()
        lines.append(f"| {city} | {ts} | {aqi} | {wind} | {meth} | {src} |")

    OUT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT_PATH}")

if __name__ == "__main__":
    main()
