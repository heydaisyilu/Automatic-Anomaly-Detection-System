#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import traceback
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Optional, List
import pathlib
import csv
import re

# ======================= CẤU HÌNH THÀNH PHỐ =======================
CITIES: List[Dict[str, str]] = [
    {"name": "hanoi",              "display_name": "Hà Nội",         "url": "https://www.iqair.com/vi/vietnam/hanoi/hanoi"},
    {"name": "ho-chi-minh-city",   "display_name": "Hồ Chí Minh",    "url": "https://www.iqair.com/vi/vietnam/ho-chi-minh-city/ho-chi-minh-city"},
    {"name": "da-nang",            "display_name": "Đà Nẵng",        "url": "https://www.iqair.com/vi/vietnam/da-nang/da-nang"},
    {"name": "hai-phong",          "display_name": "Hải Phòng",      "url": "https://www.iqair.com/vi/vietnam/thanh-pho-hai-phong/haiphong"},
    {"name": "nha-trang",          "display_name": "Nha Trang",      "url": "https://www.iqair.com/vi/vietnam/khanh-hoa/nha-trang"},
    {"name": "can-tho",            "display_name": "Cần Thơ",        "url": "https://www.iqair.com/vi/vietnam/thanh-pho-can-tho/can-tho"},
    {"name": "hue",                "display_name": "Huế",            "url": "https://www.iqair.com/vi/vietnam/tinh-thua-thien-hue/hue"},
    {"name": "vinh",               "display_name": "Vinh",           "url": "https://www.iqair.com/vi/vietnam/tinh-nghe-an/vinh"},
]

# ======================= TIỆN ÍCH & VALIDATOR =======================
def get_vietnam_time() -> datetime:
    # Múi giờ VN chuẩn
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))

def validate_aqi(aqi: str) -> Optional[str]:
    try:
        aqi_value = int(re.sub(r"\D", "", aqi or ""))
        if 0 <= aqi_value <= 500:
            return str(aqi_value)
    except (ValueError, TypeError):
        pass
    return None

def validate_weather_icon(icon: str) -> Optional[str]:
    if not icon or not isinstance(icon, str):
        return None
    # Chuẩn hóa đường dẫn icon
    if icon.startswith("/dl/assets/svg/weather/"):
        icon = icon.replace("/dl/assets/svg/weather/", "/dl/web/weather/")
    if icon.startswith("/dl/web/weather/"):
        return icon
    return None

def validate_wind_speed(speed: str) -> Optional[str]:
    """
    Chỉ chấp nhận chuỗi có 'km/h' để tránh 'ăn' nhầm AQI.
    Trả về phần số (không đơn vị), ví dụ '7.4 km/h' -> '7.4'
    """
    try:
        s = (speed or "").strip()
        if "km/h" not in s:
            return None
        m = re.search(r"(\d+(\.\d+)?)", s)
        if m:
            return m.group(1)
    except Exception:
        pass
    return None

def validate_humidity(humidity: str) -> Optional[str]:
    """
    Chỉ chấp nhận chuỗi có '%' để tránh 'ăn' nhầm AQI.
    Trả về phần số (0..100) dạng chuỗi.
    """
    try:
        s = (humidity or "").strip()
        if "%" not in s:
            return None
        m = re.search(r"(\d{1,3})", s)
        if m:
            val = int(m.group(1))
            if 0 <= val <= 100:
                return str(val)
    except Exception:
        pass
    return None

# ======================= CRAWL 1 THÀNH PHỐ =======================
def crawl_city_data(page, city: Dict) -> Optional[Dict]:
    print(f"\nAccessing {city['display_name']} ({city['url']})...", flush=True)
    try:
        page.goto(city['url'], timeout=60_000)
        print("Page loaded", flush=True)

        # AQI (cần chỉnh nếu IQAir đổi DOM)
        aqi_selector = "p.flex.h-full.w-full.flex-col.items-center.justify-center.text-sm.font-medium"
        page.wait_for_selector(aqi_selector, timeout=30_000)
        aqi_raw = (page.query_selector(aqi_selector).text_content() or "").strip()

        # Weather icon
        weather_icon_elem = page.query_selector("img[alt='Biểu tượng thời tiết']")
        weather_icon_raw = weather_icon_elem.get_attribute("src") if weather_icon_elem else None
        if weather_icon_raw and weather_icon_raw.startswith('/dl/assets/svg/weather/'):
            weather_icon_raw = weather_icon_raw.replace('/dl/assets/svg/weather/', '/dl/web/weather/')

        # ---------- WIND ----------
        wind_speed_raw = ""
        try:
            # Cách 1: gần icon gió
            wind_img = page.query_selector("img[src*='ic-wind-s-sm-solid-weather'], img[alt*='Gió'], img[alt*='gio']")
            if wind_img:
                wind_container_js = wind_img.evaluate_handle("n => n.closest('div.flex.flex-col.items-center')")
                wind_container = wind_container_js.as_element() if wind_container_js else None
                if wind_container:
                    # Thử dạng p + p (SỐ + 'km/h')
                    unit_elem = wind_container.query_selector(":text('km/h')")
                    if unit_elem:
                        # Lấy số liền trước
                        num_elem = unit_elem.query_selector("xpath=preceding-sibling::*[1]") or unit_elem
                        num_txt = (num_elem.text_content() or "").strip()
                        if re.fullmatch(r"\d+(\.\d+)?", num_txt):
                            wind_num = validate_wind_speed(f"{num_txt} km/h")
                            wind_speed_raw = f"{float(wind_num):.1f} km/h" if wind_num else ""

            # Cách 2: fallback container có text 'km/h' (bất kỳ thẻ)
            if not wind_speed_raw:
                container = page.query_selector("div.flex.flex-col.items-center:has-text('km/h')")
                if container:
                    txts = [t.strip() for t in container.text_content().split() if t.strip()]
                    # tìm số ngay trước 'km/h'
                    for i, t in enumerate(txts):
                        if "km/h" in t.lower() and i > 0 and re.fullmatch(r"\d+(\.\d+)?", txts[i-1]):
                            wind_num = validate_wind_speed(f"{txts[i-1]} km/h")
                            wind_speed_raw = f"{float(wind_num):.1f} km/h" if wind_num else ""
                            break
        except Exception:
            pass

        # ---------- HUMIDITY: lấy <p.font-medium> có '%' trong container gần icon ----------
        humidity_raw = ""
        try:
            humidity_img = page.query_selector("img[src*='ic-humidity-2-solid-weather'], img[alt*='Độ ẩm'], img[alt*='do am']")
            if humidity_img:
                hum_container_js = humidity_img.evaluate_handle("n => n.closest('div.flex.flex-col.items-center')")
                hum_container = hum_container_js.as_element() if hum_container_js else None
                if hum_container:
                    hum_p = hum_container.query_selector("xpath=.//p[contains(@class,'font-medium')][contains(.,'%')]") \
                           or hum_container.query_selector("xpath=.//p[contains(.,'%')]")
                    hum_txt = (hum_p.text_content() or "").strip() if hum_p else ""
                    if "%" in hum_txt:
                        hum_num = validate_humidity(hum_txt)
                        humidity_raw = f"{hum_num}%" if hum_num else ""
            # Fallback cuối: container có ký tự '%'
            if not humidity_raw:
                container = page.query_selector("div.flex.flex-col.items-center:has-text('%')")
                if container:
                    hum_p = container.query_selector("xpath=.//p[contains(@class,'font-medium')][contains(.,'%')]") \
                            or container.query_selector("xpath=.//p[contains(.,'%')]")
                    hum_txt = (hum_p.text_content() or "").strip() if hum_p else ""
                    if "%" in hum_txt:
                        hum_num = validate_humidity(hum_txt)
                        humidity_raw = f"{hum_num}%" if hum_num else ""
        except Exception:
            pass

        print(f"aqi_raw={aqi_raw}, weather_icon_raw={weather_icon_raw}, wind_speed_raw={wind_speed_raw}, humidity_raw={humidity_raw}", flush=True)

        # Validate cuối
        aqi = validate_aqi(aqi_raw)
        weather_icon = validate_weather_icon(weather_icon_raw)
        wind_speed = wind_speed_raw
        humidity = humidity_raw
        if not all([aqi, weather_icon, wind_speed, humidity]):
            print(f"Invalid data for {city['display_name']}: aqi={aqi}, weather_icon={weather_icon}, wind_speed={wind_speed}, humidity={humidity}", flush=True)
            return None

        now = get_vietnam_time()
        return {
            "timestamp": now.isoformat(),
            "city": city['display_name'],
            "aqi": aqi,                # "68"
            "weather_icon": weather_icon,
            "wind_speed": wind_speed,  # "7.4 km/h"
            "humidity": humidity,      # "78%"
        }

    except Exception as e:
        print(f"Error extracting data for {city['display_name']}: {str(e)}", flush=True)
        traceback.print_exc()
        try:
            print(page.content())
        except Exception:
            pass
        return None

# ======================= LƯU CSV =======================
def save_to_csv(data: Dict, city_name: str) -> pathlib.Path:
    now = get_vietnam_time()
    result_dir = pathlib.Path(f"data/{city_name}")
    result_dir.mkdir(parents=True, exist_ok=True)
    filename = f"aqi_{city_name}_{now.year}_{now.strftime('%b').lower()}.csv"  # aqi_can-tho_2025_aug.csv
    filepath = result_dir / filename

    headers = ["timestamp", "city", "aqi", "weather_icon", "wind_speed", "humidity"]
    file_exists = filepath.exists()
    with open(filepath, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)
    return filepath

# ======================= CHẠY TOÀN BỘ 8 THÀNH PHỐ =======================
def crawl_all_cities() -> List[Dict]:
    results: List[Dict] = []
    for city in CITIES:
        print(f"\n{'='*60}\nProcessing {city['display_name']}...", flush=True)
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
                page = browser.new_page()
                page.set_viewport_size({"width": 1280, "height": 720})
                page.set_default_timeout(20_000)

                data = crawl_city_data(page, city)
                if data:
                    results.append(data)
                    csv_file = save_to_csv(data, city["name"])
                    print(f"→ Saved: {csv_file}", flush=True)
                else:
                    print(f"→ Skipped {city['display_name']} (invalid data)", flush=True)

                browser.close()
        except PWTimeoutError as te:
            print(f"Timeout for {city['display_name']}: {te}", flush=True)
            traceback.print_exc()
        except Exception as e:
            print(f"Playwright error for {city['display_name']}: {e}", flush=True)
            traceback.print_exc()
    return results

# ======================= MAIN =======================
if __name__ == "__main__":
    try:
        print("Starting IQAir data crawler...", flush=True)
        print(f"Vietnam time: {get_vietnam_time().strftime('%Y-%m-%d %H:%M:%S %Z')}", flush=True)
        results = crawl_all_cities()
        print("\nCrawled data:", flush=True)
        for row in results:
            print(row, flush=True)
    except Exception as e:
        print(f"Fatal error: {e}", flush=True)
        traceback.print_exc()
        raise e
