import pandas as pd
import glob
from pathlib import Path

# Thư mục chứa dữ liệu đầu vào
THU_MUC_DU_LIEU = Path("data")
# Thư mục lưu kết quả sau khi gộp
THU_MUC_KET_QUA = Path("result")
# Năm cần xử lý
NAM = 2025

# Danh sách tên cột thời gian có thể gặp
CAC_COT_THOI_GIAN = [
    "timestamp", "time", "datetime", "created_at", "scraped_at", "date", "ngay", "thoi_gian"
]

def chon_cot_thoi_gian(df: pd.DataFrame) -> str | None:
    """Chọn cột thời gian phù hợp trong DataFrame."""
    for c in df.columns:
        if c.lower() in CAC_COT_THOI_GIAN:
            return c
    return None

def tien_xu_ly(df: pd.DataFrame) -> pd.DataFrame:
    """Tiền xử lý dữ liệu: chuẩn hóa thời gian, xử lý thiếu, sắp xếp."""
    df = df.copy()

    # 1) Chọn cột thời gian
    cot_tg = chon_cot_thoi_gian(df)
    if cot_tg is None:
        print("Không tìm thấy cột thời gian hợp lệ → Bỏ qua xử lý thời gian")
    else:
        # 2) Chuyển sang datetime
        df[cot_tg] = pd.to_datetime(df[cot_tg], errors="coerce")

        # 3) Xóa các dòng có thời gian không hợp lệ
        so_dong_dau = len(df)
        df = df.dropna(subset=[cot_tg])
        so_dong_xoa = so_dong_dau - len(df)
        if so_dong_xoa > 0:
            print(f"Đã xóa {so_dong_xoa} dòng có {cot_tg} không hợp lệ")

        # 4) Sắp xếp theo thời gian
        df = df.sort_values(by=cot_tg, kind="mergesort")
        print(f"Đã sắp xếp theo {cot_tg}")

    # 5) Xử lý cột aqi nếu có
    if "aqi" in df.columns:
        df["aqi"] = pd.to_numeric(df["aqi"], errors="coerce")
        if df["aqi"].isnull().any():
            gia_tri_tb = df["aqi"].mean()
            if pd.notna(gia_tri_tb):
                df["aqi"] = df["aqi"].fillna(gia_tri_tb)
                print(f"Đã điền giá trị thiếu trong 'aqi' = {gia_tri_tb:.2f}")
            else:
                print("Không thể tính trung bình 'aqi' (toàn NaN) → Bỏ qua fillna")
        else:
            print("Không có giá trị thiếu trong 'aqi'")
    else:
        print("Không có cột 'aqi' → Bỏ qua xử lý aqi")

    return df

def gop_thanh_pho(thu_muc_tp: Path):
    """Gộp dữ liệu của một thành phố."""
    ten_tp = thu_muc_tp.name

    # Hỗ trợ cả dạng tên file aqi_{tp} và aqi-{tp}
    patterns = str(thu_muc_tp / f"aqi_{ten_tp}_{NAM}_*.csv")
    files = []
    for p in patterns:
        files.extend(sorted(glob.glob(p)))

    if not files:
        print(f"[{ten_tp}] Không tìm thấy file CSV tháng nào.")
        return

    print(f"[{ten_tp}] Tìm thấy {len(files)} file.")

    ds_df = []
    for f in files:
        try:
            df = pd.read_csv(f)
            df["__nguon_file"] = Path(f).name
            ds_df.append(df)
        except Exception as e:
            print(f"[{ten_tp}] Lỗi đọc {f} -> {e}")

    if not ds_df:
        print(f"[{ten_tp}] Không có dữ liệu hợp lệ.")
        return

    df_gop = pd.concat(ds_df, ignore_index=True)

    # Loại bỏ trùng lặp toàn bộ hàng
    so_dong_truoc = len(df_gop)
    df_gop = df_gop.drop_duplicates()
    so_dong_xoa = so_dong_truoc - len(df_gop)
    if so_dong_xoa > 0:
        print(f"[{ten_tp}] Đã loại {so_dong_xoa} dòng trùng lặp")

    # Tiền xử lý dữ liệu
    df_gop = tien_xu_ly(df_gop)

    # Lưu kết quả
    duong_dan_kq = THU_MUC_KET_QUA / f"aqi-{ten_tp}_{NAM}.csv"
    duong_dan_kq.parent.mkdir(parents=True, exist_ok=True)
    df_gop.to_csv(duong_dan_kq, index=False)
    print(f"[{ten_tp}] Đã tạo/cập nhật {duong_dan_kq}")

def main():
    if not THU_MUC_DU_LIEU.exists():
        print("Không có thư mục dữ liệu:", THU_MUC_DU_LIEU)
        return
    for thu_muc_tp in sorted(THU_MUC_DU_LIEU.iterdir()):
        if thu_muc_tp.is_dir():
            gop_thanh_pho(thu_muc_tp)

if __name__ == "__main__":
    main()