# notify/render_email.py
import os, glob, json
from pathlib import Path
import pandas as pd
import numpy as np

OUT_DIR = Path("out"); OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH = OUT_DIR / "anomaly_email.md"

LOCAL_TZ = os.getenv("LOCAL_TZ", "Asia/Ho_Chi_Minh")
INPUT_TZ = os.getenv("INPUT_TZ")  # nếu timestamp naive là giờ local

ALIASES = {
    "city": ["city", "thanh_pho", "tinh", "province", "location"],
    "time": ["time", "timestamp", "datetime", "date"],
    "aqi": ["aqi", "AQI", "aqi_value"],
    "wind": ["wind", "wind_speed", "windspeed", "gio", "gió"],
    "flag": ["flag", "is_anomaly", "anomaly", "is_outlier", "label"],
    "method": ["method", "algo", "algorithm", "detector", "source"],
}

Z_FLAG_COLS = {
    "Z-score AQI": "zscore_flag_aqi",
    "Z-score Wind": "zscore_flag_wind",
}
IF_FLAG_COL = ("IsolationForest", "anomaly")

def pick_col(df, names):
    cols = {c.lower(): c for c in df.columns}
    for n in names:
        if n in df.columns: return n
        if n.lower() in cols: return cols[n.lower()]
    for c in df.columns:
        if any(n.lower() in c.lower() for n in names): return c
    return None

def coerce_time(series):
    dt = pd.to_datetime(series, errors="coerce", utc=False)
    if hasattr(dt, "dt"):
        try:
            is_tzaware = dt.dt.tz is not None
        except Exception:
            is_tzaware = False
        if not is_tzaware:
            dt = dt.dt.tz_localize(INPUT_TZ or "UTC")
        return dt.dt.tz_convert("UTC")
    return pd.to_datetime(series, errors="coerce", utc=True)

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
        rows = obj["data"] if isinstance(obj, dict) and isinstance(obj.get("data"), list) else (obj if isinstance(obj, list) else None)
        if rows is None: return None
        df = pd.DataFrame(rows)
        df["__source"] = p
        return df
    except Exception as e:
        print(f"[WARN] skip {p}: {e}")
        return None

def parse_flag_generic(series):
    sn = pd.to_numeric(series, errors="coerce")
    out = pd.Series(False, index=series.index)
    num_mask = ~sn.isna()
    out[num_mask] = (sn[num_mask] == -1)  # chuẩn chung: -1 là bất thường
    st = series.astype(str).str.strip().str.lower()
    tokens = {"-1","true","yes","y","t","anomaly","outlier","abnormal","alert"}
    out[~num_mask] = st[~num_mask].isin(tokens)
    return out

def derive_detector_flags(df):
    methods_fired = []
    flags = []

    for label, col in Z_FLAG_COLS.items():
        if col in df.columns:
            f = parse_flag_generic(df[col])
            flags.append(f)
            methods_fired.append(np.where(f, label, ""))

    if IF_FLAG_COL[1] in df.columns:
        f = parse_flag_generic(df[IF_FLAG_COL[1]])
        flags.append(f)
        methods_fired.append(np.where(f, IF_FLAG_COL[0], ""))

    if flags:
        any_flag = np.logical_or.reduce(flags)
        if methods_fired:
            stacked = np.vstack(methods_fired)
            methods = pd.Series(
                [", ".join([m for m in row if m]) if any(row) else "" for row in stacked.T],
                index=df.index
            )
        else:
            methods = pd.Series("", index=df.index)
    else:
        any_flag = pd.Series(False, index=df.index)
        methods = pd.Series("", index=df.index)

    return pd.Series(any_flag, index=df.index), pd.Series(methods, index=df.index)

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
    out["time_utc"] = coerce_time(df[c_time])

    if c_aqi in df.columns:
        out["aqi"] = pd.to_numeric(df[c_aqi], errors="coerce")

    if c_wind in df.columns:
        wind_raw = df[c_wind].astype(str).str.extract(r"([\d.]+)", expand=False)
        out["wind"] = pd.to_numeric(wind_raw, errors="coerce")

    det_flag, det_method = derive_detector_flags(df)

    if c_flag in df.columns:
        gen_flag = parse_flag_generic(df[c_flag])
        out["flag"] = det_flag | gen_flag
    else:
        out["flag"] = det_flag

    base_method = df[c_meth].astype(str) if c_meth in df.columns else pd.Series("", index=df.index)
    meth = base_method.str.strip()
    det_method = det_method.fillna("").astype(str).str.strip()
    both = []
    for m1, m2 in zip(meth, det_method):
        if m1 and m2: both.append(f"{m1}, {m2}")
        elif m1: both.append(m1)
        else: both.append(m2)
    out["method"] = pd.Series(both, index=df.index).replace("", np.nan)

    out["__source"] = df["__source"]
    return out.dropna(subset=["time_utc"])

def decode_changed_files_env(s):
    if not s: return []
    s = s.replace("%0D", "").replace("%0A", "\n").replace("%25", "%")
    return [line.strip() for line in s.splitlines() if line.strip()]

def main():
    raw = os.getenv("CHANGED_FILES", "")
    changed = decode_changed_files_env(raw)
    paths = changed if changed else glob.glob("result_anomaly/**/*", recursive=True)
    paths = [p for p in paths if p.lower().endswith((".csv",".json"))]
    if not paths:
        print("No changed anomaly files.")
        return

    frames=[]
    for p in paths:
        df = read_csv_any(p) if p.lower().endswith(".csv") else read_json_any(p)
        if df is None or df.empty: 
            continue
        cdf = to_canonical(df)
        if cdf is not None and not cdf.empty:
            frames.append(cdf)

    if not frames:
        print("No usable rows.")
        return

    big = pd.concat(frames, ignore_index=True)
    big = big[big["flag"] == True].copy()
    if big.empty:
        print("No positive flags.")
        return

    big = big.sort_values("time_utc")
    latest = big.groupby("city", as_index=False).tail(1).copy()

    def fmt_local(ts):
        ts = pd.to_datetime(ts, utc=True)
        return ts.tz_convert(LOCAL_TZ).strftime("%Y-%m-%d %H:%M")

    now = (pd.Timestamp.utcnow().tz_localize("UTC").tz_convert(LOCAL_TZ))
    tz_label = now.strftime("%Z")
    now_str  = now.strftime("%Y-%m-%d %H:%M %Z")

    lines = [
        f"#  Cảnh báo bất thường AQI/Gió ({now_str})",
        "",
        f"| Thành phố | Thời điểm ({tz_label}) | AQI | Gió | Phương pháp | Nguồn |",
        "|---|---:|---:|---:|---|---|",
    ]

    for _, r in latest.iterrows():
        ts  = fmt_local(r["time_utc"])
        aqi = "" if pd.isna(r.get("aqi")) else f"{r['aqi']:.0f}"
        wnd = "" if pd.isna(r.get("wind")) else f"{r['wind']:.2f}"
        meth = "" if pd.isna(r.get("method")) else str(r["method"])
        src  = r.get("__source", "")
        lines.append(f"| {r['city']} | {ts} | {aqi} | {wnd} | {meth} | {src} |")

    OUT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT_PATH}")

if __name__ == "__main__":
    main()
