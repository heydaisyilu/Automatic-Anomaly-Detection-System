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

def get_vietnam_time() -> datetime:
    # Múi giờ VN
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

def crawl_city_data(page, city: Dict) -> Optional[Dict]:
    print(f"\nTruy cập {city['display_name']} ({city['url']})", flush=True)
    try:
        page.goto(city["url"], timeout=60_000)
        print("  → Page loaded", flush=True)

        # AQI: selector tổng quát (có thể cần chỉnh nếu IQAir thay DOM)
        aqi_selector = "p.flex.h-full.w-full.flex-col.items-center.justify-center.text-sm.font-medium"
        page.wait_for_selector(aqi_selector, timeout=30_000)
        aqi_raw = page.query_selector(aqi_selector).text_content().strip()

        # Weather icon (alt tiếng Việt theo site hiện tại)
        weather_icon_elem = page.query_selector("img[alt='Biểu tượng thời tiết']")
        weather_icon_raw = weather_icon_elem.get_attribute("src") if weather_icon_elem else None
        if weather_icon_raw and weather_icon_raw.startswith("/dl/assets/svg/weather/"):
            weather_icon_raw = weather_icon_raw.replace("/dl/assets/svg/weather/", "/dl/web/weather/")

        # --- WIND: chỉ lấy p.font-medium có 'km/h' ---
        wind_speed_raw = ""
        wind_img = page.query_selector("img[src*='ic-wind-s-sm-solid-weather']")
        if wind_img:
            container = wind_img.evaluate_handle("""
                node => {
                    let p = node;
                    for (let i = 0; i < 6 && p && p.parentElement; i++) p = p.parentElement;
                    return p;
                }
            """)
            if container:
                ps = container.query_selector_all("p.font-medium")
                for ptag in ps:
                    txt = (ptag.text_content() or "").strip()
                    if "km/h" in txt:
                        wind_num = validate_wind_speed(txt)
                        wind_speed_raw = f"{float(wind_num):.1f} km/h" if wind_num else ""
                        break

        # --- HUMIDITY: chỉ lấy p.font-medium có '%' ---
        humidity_raw = ""
        humidity_img = page.query_selector("img[src*='ic-humidity-2-solid-weather']")
        if humidity_img:
            container = humidity_img.evaluate_handle("""
                node => {
                    let p = node;
                    for (let i = 0; i < 6 && p && p.parentElement; i++) p = p.parentElement;
                    return p;
                }
            """)
            if container:
                ps = container.query_selector_all("p.font-medium")
                for ptag in ps:
                    txt = (ptag.text_content() or "").strip()
                    if "%" in txt:
                        hum_num = validate_humidity(txt)
                        humidity_raw = f"{hum_num}%" if hum_num else ""
                        break

        print(f"  RAW → AQI='{aqi_raw}' | ICON='{weather_icon_raw}' | WIND='{wind_speed_raw}' | HUMID='{humidity_raw}'", flush=True)

        aqi = validate_aqi(aqi_raw)
        weather_icon = validate_weather_icon(weather_icon_raw)
        wind_speed = wind_speed_raw
        humidity = humidity_raw

        if not all([aqi, weather_icon, wind_speed, humidity]):
            print(f"Dữ liệu không hợp lệ: aqi={aqi}, icon={weather_icon}, wind='{wind_speed}', humid='{humidity}'", flush=True)
            return None

        now = get_vietnam_time()
        row = {
            "timestamp": now.isoformat(),
            "city": city["display_name"],
            "aqi": aqi,  # lưu chuỗi số, ví dụ "68"
            "weather_icon": weather_icon,
            "wind_speed": wind_speed,  # ví dụ "7.4 km/h"
            "humidity": humidity,      # ví dụ "78%"
        }
        print(f"OK → {row}", flush=True)
        return row

    except Exception as e:
        print(f"Lỗi extract {city['display_name']}: {e}", flush=True)
        traceback.print_exc()
        try:
            print(page.content())
        except Exception:
            pass
        return None

def save_to_csv(data: Dict, city_name: str) -> pathlib.Path:
    now = get_vietnam_time()
    result_dir = pathlib.Path(f"data/{city_name}")
    result_dir.mkdir(parents=True, exist_ok=True)
    filename = f"aqi_{city_name}_{now.year}_{now.strftime('%b').lower()}.csv" 
    filepath = result_dir / filename

    headers = ["timestamp", "city", "aqi", "weather_icon", "wind_speed", "humidity"]
    file_exists = filepath.exists()
    with open(filepath, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)
    return filepath

def crawl_all_cities() -> List[Dict]:
    results: List[Dict] = []
    for city in CITIES:
        print(f"\n{'='*60}\nXử lý {city['display_name']}...", flush=True)
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
                    print(f"Đã lưu: {csv_file}", flush=True)
                else:
                    print(f"Bỏ qua {city['display_name']} (dữ liệu chưa hợp lệ)", flush=True)

                browser.close()
        except PWTimeoutError as te:
            print(f"Timeout Playwright cho {city['display_name']}: {te}", flush=True)
            traceback.print_exc()
        except Exception as e:
            print(f"Lỗi Playwright {city['display_name']}: {e}", flush=True)
            traceback.print_exc()
    return results

if __name__ == "__main__":
    try:
        print("Bắt đầu crawler IQAir...", flush=True)
        print(f"Giờ Việt Nam: {get_vietnam_time().strftime('%Y-%m-%d %H:%M:%S %Z')}", flush=True)
        results = crawl_all_cities()
        print("\nKết quả crawl:", flush=True)
        for row in results:
            print(row, flush=True)
    except Exception as e:
        print(f"Lỗi tổng: {e}", flush=True)
        traceback.print_exc()
        raise e