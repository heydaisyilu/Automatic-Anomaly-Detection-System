#!/usr/bin/env python3
import os
import re
import glob
from pathlib import Path
import pandas as pd

# Thư mục dữ liệu và kết quả
THU_MUC_DU_LIEU = Path("data")
THU_MUC_KQ = Path("result")
NAM = 2025

# Tần suất resample mặc định: 1 giờ (có thể override bằng ENV RESAMPLE)
TANSUAT = os.getenv("RESAMPLE", "1H")  # "1H" = 1 giờ

# Danh sách cột chuẩn
COT_CHUAN = ["timestamp", "city", "aqi", "weather_icon", "wind_speed", "humidity"]

def _mode_or_last(series: pd.Series):
    """Chọn mode; nếu không có, lấy giá trị cuối cùng không null."""
    if series.empty:
        return None
    m = series.mode(dropna=True)
    if len(m) > 0:
        return m.iloc[0]
    notna = series.dropna()
    return notna.iloc[-1] if len(notna) > 0 else None

# Regex số (lấy số đầu tiên trong chuỗi, ví dụ '7.4 km/h' -> 7.4; '78%' -> 78.0)
_NUM_RE = re.compile(r"[-+]?\d*\.?\d+|\d+")

def strip_units(val):
    """Trích số đầu tiên trong chuỗi; trả về float hoặc pd.NA."""
    if pd.isna(val):
        return pd.NA
    if isinstance(val, (int, float)):
        return float(val)
    m = _NUM_RE.search(str(val))
    if m:
        try:
            return float(m.group())
        except Exception:
            return pd.NA
    return pd.NA

def _lam_sach_va_resample(df: pd.DataFrame, ten_tp: str) -> pd.DataFrame:
    df = df.copy()

    # Đảm bảo tồn tại cột city
    if "city" not in df.columns:
        df["city"] = ten_tp

    # Chuẩn hóa timestamp
    if "timestamp" not in df.columns:
        print(f"Không có cột 'timestamp' trong dữ liệu {ten_tp}.")
        return pd.DataFrame(columns=COT_CHUAN)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    truoc = len(df)
    df = df.dropna(subset=["timestamp"])
    da_xoa = truoc - len(df)
    if da_xoa > 0:
        print(f"Xóa {da_xoa} dòng timestamp không hợp lệ.")

    # Bỏ đơn vị -> về số cho các cột số
    for col in ["aqi", "wind_speed", "humidity"]:
        if col in df.columns:
            df[col] = df[col].map(strip_units)

    # Cắt ngoại lai
    for num_col, low, high in [("aqi", 0, 500), ("wind_speed", 0, 200), ("humidity", 0, 100)]:
        if num_col in df.columns:
            truoc = len(df)
            df = df[(df[num_col].isna()) | ((df[num_col] >= low) & (df[num_col] <= high))]
            if truoc - len(df) > 0:
                print(f"Cắt {truoc - len(df)} dòng ngoại lai {num_col}.")

    # Điền giá trị thiếu AQI bằng trung bình
    if "aqi" in df.columns and df["aqi"].isnull().any():
        mean_aqi = df["aqi"].mean()
        if pd.notna(mean_aqi):
            df["aqi"].fillna(mean_aqi, inplace=True)
            print(f"Điền thiếu AQI = {mean_aqi:.2f}")

    # Giữ đúng 6 cột (thêm cột trống nếu thiếu)
    for col in COT_CHUAN:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[COT_CHUAN]

    # Sắp xếp và resample theo TANSUAT (mặc định 1H)
    df = df.sort_values(by="timestamp").set_index("timestamp")
    agg_map = {
        "aqi": "mean",
        "wind_speed": "mean",
        "humidity": "mean",
        "weather_icon": _mode_or_last,
        "city": _mode_or_last,
    }
    df_res = df.resample(TANSUAT).agg(agg_map)

    # Làm gọn số
    for col in ["aqi", "wind_speed", "humidity"]:
        if col in df_res.columns:
            df_res[col] = df_res[col].round(2)

    df_res = df_res.reset_index()

    # Loại dòng hoàn toàn trống
    all_null_numeric = df_res[["aqi", "wind_speed", "humidity"]].isna().all(axis=1)
    keep_mask = ~all_null_numeric | df_res["weather_icon"].notna() | df_res["city"].notna()
    df_res = df_res[keep_mask]

    print(f"Resample {TANSUAT} → còn {len(df_res)} dòng.")
    return df_res

def gop_mot_thanh_pho(thu_muc_tp: Path):
    ten_tp = thu_muc_tp.name

    # Chỉ lấy file CSV dạng aqi_{city}_{NAM}_*.csv
    pattern = str(thu_muc_tp / f"aqi_{ten_tp}_{NAM}_*.csv")
    files = sorted(glob.glob(pattern))

    if not files:
        print(f"[{ten_tp}] Không tìm thấy file CSV tháng nào.")
        return

    print(f"[{ten_tp}] Tìm thấy {len(files)} file: " + ", ".join(Path(f).name for f in files))

    ds = []
    for f in files:
        try:
            # Đọc an toàn: tự đoán dấu phân tách, bỏ dòng hỏng nếu có
            df = pd.read_csv(f, engine="python", sep=None, on_bad_lines="skip")
            ds.append(df)
        except Exception as e:
            print(f"[{ten_tp}] Lỗi đọc {Path(f).name} -> {e}")

    if not ds:
        print(f"[{ten_tp}] Không có dữ liệu hợp lệ.")
        return

    df_gop = pd.concat(ds, ignore_index=True).drop_duplicates()
    df_kq = _lam_sach_va_resample(df_gop, ten_tp)

    duong_dan = THU_MUC_KQ / f"aqi-{ten_tp}_{NAM}.csv"
    duong_dan.parent.mkdir(parents=True, exist_ok=True)
    df_kq.to_csv(duong_dan, index=False)
    print(f"[{ten_tp}] Đã tạo/cập nhật {duong_dan}")

def main():
    print(f"Tần suất resample: {TANSUAT}")
    if not THU_MUC_DU_LIEU.exists():
        print("Không có thư mục dữ liệu:", THU_MUC_DU_LIEU)
        return
    for thu_muc_tp in sorted(THU_MUC_DU_LIEU.iterdir()):
        if thu_muc_tp.is_dir():
            gop_mot_thanh_pho(thu_muc_tp)

if __name__ == "__main__":
    main()