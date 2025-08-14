import traceback
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Optional, List
import pathlib
import csv
import re

CITIES: List[Dict[str, str]] = [
    {
        "name": "hanoi",
        "display_name": "Hà Nội",
        "url": "https://www.iqair.com/vi/vietnam/hanoi/hanoi"
    },
    {
        "name": "ho-chi-minh-city",
        "display_name": "Hồ Chí Minh",
        "url": "https://www.iqair.com/vi/vietnam/ho-chi-minh-city/ho-chi-minh-city"
    },
    {
        "name": "da-nang",
        "display_name": "Đà Nẵng",
        "url": "https://www.iqair.com/vi/vietnam/da-nang/da-nang"
    },
    {
        "name": "hai-phong",
        "display_name": "Hải Phòng",
        "url": "https://www.iqair.com/vi/vietnam/thanh-pho-hai-phong/haiphong"
    },
    {
        "name": "nha-trang",
        "display_name": "Nha Trang",
        "url": "https://www.iqair.com/vi/vietnam/khanh-hoa/nha-trang"
    },
    {
        "name": "can-tho",
        "display_name": "Cần Thơ",
        "url": "https://www.iqair.com/vi/vietnam/thanh-pho-can-tho/can-tho"
    },
    {
        "name": "hue",
        "display_name": "Huế",
        "url": "https://www.iqair.com/vi/vietnam/tinh-thua-thien-hue/hue"
    },
    {
        "name": "vinh",
        "display_name": "Vinh",
        "url": "https://www.iqair.com/vi/vietnam/tinh-nghe-an/vinh"
    }
]

def get_vietnam_time():
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))

def validate_aqi(aqi: str) -> Optional[str]:
    try:
        aqi_value = int(re.sub(r'\D', '', aqi or ""))
        if 0 <= aqi_value <= 500:
            return str(aqi_value)
    except (ValueError, TypeError):
        pass
    return None

def validate_weather_icon(icon: str) -> Optional[str]:
    if not icon or not isinstance(icon, str):
        return None
    if icon.startswith('/dl/assets/svg/weather/'):
        return icon.replace('/dl/assets/svg/weather/', '/dl/web/weather/')
    if icon.startswith('/dl/web/weather/'):
        return icon
    return None

def validate_wind_speed(speed: str) -> Optional[str]:
    try:
        s = (speed or "").strip()
        if "km/h" not in s:
            return None
        m = re.search(r'(\d+(\.\d+)?)', s)
        if m:
            return m.group(1)
    except Exception:
        pass
    return None

def validate_humidity(humidity: str) -> Optional[str]:
    try:
        s = (humidity or "").strip()
        if "%" not in s:
            return None
        m = re.search(r'(\d{1,3})', s)
        if m:
            val = int(m.group(1))
            if 0 <= val <= 100:
                return str(val)
    except Exception:
        pass
    return None

def crawl_city_data(page, city: Dict) -> Optional[Dict]:
    print(f"\nAccessing {city['display_name']} ({city['url']})...", flush=True)
    try:
        page.goto(city['url'], timeout=60000)
        print("Page loaded", flush=True)

        # AQI
        aqi_selector = "p.flex.h-full.w-full.flex-col.items-center.justify-center.text-sm.font-medium"
        page.wait_for_selector(aqi_selector, timeout=30000)
        aqi_raw = (page.query_selector(aqi_selector).text_content() or "").strip()

        # Weather icon
        weather_icon_elem = page.query_selector("img[alt='Biểu tượng thời tiết']")
        weather_icon_raw = weather_icon_elem.get_attribute("src") if weather_icon_elem else None
        if weather_icon_raw and weather_icon_raw.startswith('/dl/assets/svg/weather/'):
            weather_icon_raw = weather_icon_raw.replace('/dl/assets/svg/weather/', '/dl/web/weather/')

        # ---------- WIND (đa chiến lược + kiểm tra 'km/h') ----------
        wind_speed_raw = ""
        try:
            wind_container = None

            # 1) theo src icon gió
            cand = page.query_selector("img[src*='ic-wind-s-sm-solid-weather']")
            if cand:
                wind_container = cand.evaluate_handle("n => n.closest('div.flex.flex-col.items-center')")

            # 2) fallback theo alt có chữ 'Gió'
            if not wind_container:
                cand = page.query_selector("img[alt*='Gió']")
                if cand:
                    wind_container = cand.evaluate_handle("n => n.closest('div.flex.flex-col.items-center')")

            # 3) fallback theo container có text 'km/h'
            if not wind_container:
                wind_container = page.query_selector("div.flex.flex-col.items-center:has-text('km/h')")

            if wind_container:
                num_el = None
                unit_txt = ""
                ps = wind_container.query_selector_all("p")
                if ps:
                    # p số (font-medium, chỉ chứa số)
                    for ptag in ps:
                        cls = (ptag.get_attribute("class") or "")
                        txt = (ptag.text_content() or "").strip()
                        if "font-medium" in cls and re.fullmatch(r"\d+(\.\d+)?", txt or ""):
                            num_el = ptag
                            break
                    # p đơn vị
                    for ptag in ps:
                        txt = (ptag.text_content() or "").strip().lower()
                        if "km/h" in txt:
                            unit_txt = txt
                            break

                num_txt = (num_el.text_content() if num_el else "").strip() if num_el else ""
                if num_txt and "km/h" in unit_txt:
                    wind_num = validate_wind_speed(f"{num_txt} km/h")
                    wind_speed_raw = f"{float(wind_num):.1f} km/h" if wind_num else ""
        except Exception:
            # Không kill toàn bộ record chỉ vì gió lỗi
            pass

        # ---------- HUMIDITY (đa chiến lược + bắt buộc có '%') ----------
        humidity_raw = ""
        try:
            hum_container = None

            # 1) theo src icon ẩm
            cand = page.query_selector("img[src*='ic-humidity-2-solid-weather']")
            if cand:
                hum_container = cand.evaluate_handle("n => n.closest('div.flex.flex-col.items-center')")

            # 2) fallback theo alt có 'Độ ẩm'
            if not hum_container:
                cand = page.query_selector("img[alt*='Độ ẩm']")
                if cand:
                    hum_container = cand.evaluate_handle("n => n.closest('div.flex.flex-col.items-center')")

            # 3) fallback theo container có '%'
            if not hum_container:
                hum_container = page.query_selector("div.flex.flex-col.items-center:has-text('%')")

            if hum_container:
                hum_el = hum_container.query_selector("p.font-medium") or hum_container.query_selector("p")
                hum_txt = (hum_el.text_content() if hum_el else "").strip()
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
            "aqi": aqi,
            "weather_icon": weather_icon,
            "wind_speed": wind_speed,
            "humidity": humidity
        }

    except Exception as e:
        print(f"Error extracting data for {city['display_name']}: {str(e)}", flush=True)
        traceback.print_exc()
        try:
            print(page.content())
        except Exception:
            pass
        return None

def save_to_csv(data: Dict, city_name: str):
    now = get_vietnam_time()
    result_dir = pathlib.Path(f"data/{city_name}")
    result_dir.mkdir(parents=True, exist_ok=True)
    filename = f"aqi_{city_name}_{now.year}_{now.strftime('%b').lower()}.csv"
    filepath = result_dir / filename
    headers = ["timestamp", "city", "aqi", "weather_icon", "wind_speed", "humidity"]
    file_exists = filepath.exists()
    with open(filepath, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)
    return filepath

def crawl_all_cities():
    results = []
    for city in CITIES:
        print(f"\n{'='*50}\nProcessing {city['display_name']}...", flush=True)
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
                page = browser.new_page()
                page.set_viewport_size({"width": 1280, "height": 720})
                page.set_default_timeout(20000)
                data = crawl_city_data(page, city)
                if data:
                    results.append(data)
                    csv_file = save_to_csv(data, city['name'])
                    print(f"Data saved to: {csv_file}", flush=True)
                else:
                    print(f"Skipping invalid data for {city['display_name']}", flush=True)
                browser.close()
        except Exception as e:
            print(f"Playwright error for {city['display_name']}: {str(e)}", flush=True)
            traceback.print_exc()
            continue
    return results

if __name__ == "__main__":
    try:
        print("Starting IQAir data crawler...", flush=True)
        print(f"Current time in Vietnam: {get_vietnam_time().strftime('%Y-%m-%d %H:%M:%S %Z')}", flush=True)
        results = crawl_all_cities()
        print("\nCrawled data:", flush=True)
        for row in results:
            print(row, flush=True)
    except Exception as e:
        print(f"Error occurred: {str(e)}", flush=True)
        traceback.print_exc()
        raise e