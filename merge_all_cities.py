#!/usr/bin/env python3
import pandas as pd
import glob
from pathlib import Path

DATA_DIR = Path("data")
RESULT_DIR = Path("result")
YEAR = 2025

def merge_city(city_dir: Path):
    city = city_dir.name
    pattern = str(city_dir / f"aqi-{city}_{YEAR}_*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"[{city}] Không tìm thấy file CSV tháng nào.")
        return

    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f)
            df["__source_file"] = Path(f).name
            dfs.append(df)
        except Exception as e:
            print(f"[{city}] Lỗi đọc {f} -> {e}")

    if not dfs:
        print(f"[{city}] Không có dữ liệu hợp lệ.")
        return

    merged = pd.concat(dfs, ignore_index=True)
    merged = merged.drop_duplicates()

    # Sắp xếp theo cột thời gian nếu có
    time_candidates = [c for c in merged.columns if c.lower() in
                       ["time", "timestamp", "datetime", "created_at", "scraped_at", "date", "ngay", "thoi_gian"]]
    if time_candidates:
        tc = time_candidates[0]
        try:
            merged[tc] = pd.to_datetime(merged[tc], errors="coerce")
            merged = merged.sort_values(by=tc, kind="mergesort")
        except Exception as e:
            print(f"[{city}] Không thể parse thời gian theo cột {tc} -> {e}")

    out_path = RESULT_DIR / f"aqi-{city}_{YEAR}.csv"
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False)
    print(f"[{city}] Đã tạo/cập nhật {out_path}")

def main():
    for city_dir in DATA_DIR.iterdir():
        if city_dir.is_dir():
            merge_city(city_dir)

if __name__ == "__main__":
    main()
