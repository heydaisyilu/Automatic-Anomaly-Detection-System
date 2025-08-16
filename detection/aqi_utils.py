# aqi_utils.py
from __future__ import annotations
from pathlib import Path
import re
import pandas as pd
import numpy as np

RESULT_DIR = Path("result")

def _infer_city_from_filename(p: Path) -> str:
    """
    Hỗ trợ 2 kiểu tên file:
      - aqi-hanoi_2025.csv
      - aqi-ho-chi-minh-city_2025.csv
    Trả về: 'hanoi', 'ho-chi-minh-city', ...
    """
    name = p.stem  # ví dụ 'aqi-hanoi_2025'
    name = re.sub(r"^aqi-", "", name)      # bỏ 'aqi-'
    name = re.sub(r"_202\d$", "", name)    # bỏ hậu tố _2025
    return name

def load_all_cities(result_dir: Path | str = RESULT_DIR) -> pd.DataFrame:
    """
    Đọc mọi CSV trong /result và gộp thành df.
    - Chuẩn hóa timestamp -> date, hour
    - Thêm cột 'city' nếu thiếu (suy từ tên file)
    - Loại trùng (city, timestamp, aqi) nếu có
    """
    result_dir = Path(result_dir)
    files = sorted(result_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError("Không tìm thấy CSV nào trong thư mục 'result/'.")

    frames = []
    for f in files:
        df = pd.read_csv(f)
        if "city" not in df.columns:
            df["city"] = _infer_city_from_filename(f)

        # ép kiểu aqi
        if "aqi" in df.columns:
            df["aqi"] = pd.to_numeric(df["aqi"], errors="coerce")

        # chuẩn hóa thời gian
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            if "date" not in df.columns:
                df["date"] = df["timestamp"].dt.date
            if "hour" not in df.columns:
                df["hour"] = df["timestamp"].dt.hour

        frames.append(df)

    out = pd.concat(frames, ignore_index=True)

    # loại trùng nhẹ
    subset = [c for c in ["city", "timestamp", "aqi"] if c in out.columns]
    if subset:
        out = out.drop_duplicates(subset=subset)

    return out
