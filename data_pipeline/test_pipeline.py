from backend.services.weather_service import fetch_weather
from backend.services.air_quality_service import fetch_air_quality
from data_pipeline.cities import CITIES


def main():
    for city in CITIES:
        city_name = city["name"]

        print(f"\n--- {city_name} ---")

        try:
            weather = fetch_weather(city_name)
            print("Weather:", weather)
        except Exception as e:
            print("Weather fetch failed:", e)

        try:
            air = fetch_air_quality(city_name)
            print("Air Quality:", air)
        except Exception as e:
            print("Air quality fetch failed:", e)


if __name__ == "__main__":
    main()