# [ELT] New York City - "Citi Bike"

**Data Pipeline | End-to-End | Medallion architecture**

`API --> Airflow --> GCS --> BigQuery <-- dbt`

Projekt przedstawiający cały cykl operacji na danych - od pobrania ich ze źródeł zewnętrznych, przez składowanie w Google Cloud Storage, aż po przetwarzanie w BigQuery. Jego celem jest praktyczne wykorzystanie kluczowych technologii i wzorców stosowanych w data engineeringu.


1. Pobranie surowych danych z API (Citi Bike System Data + OpenWeatherMap) przy użyciu `Airflow DAG` i zapis w `GCS`
2. Załadowanie danych do tabeli `BigQuery` (***bronze***) przy użyciu `Airflow DAG`
3. Transformacja danych na poziom ***silver*** oraz ***gold*** przy użyciu `dbt`

---

**[E]LT** - Extract

* Pobranie danych pogodowych: [nyc_weather.py](https://github.com/jonaszwalkowiak/nyc-citi-bike-gbfs/blob/master/nyc_weather.py)

* Pobranie danych o stacjach i całym systemie: [nyc_citi_bike_gbfs_dynamic.py](https://github.com/jonaszwalkowiak/nyc-citi-bike-gbfs/blob/master/nyc_citi_bike_gbfs_dynamic.py) | [nyc_citi_bike_gbfs_static.py](https://github.com/jonaszwalkowiak/nyc-citi-bike-gbfs/blob/master/nyc_citi_bike_gbfs_static.py)

**E[L]T** - Load

* Przekształcenie dynamicznych danych o stacjach rowerowych z formatu `JSON` na format `Parquet` (pod External Table w BigQuery): [nyc_citi_bike_gbfs_dynamic_converter_json_to_parquet.py](https://github.com/jonaszwalkowiak/nyc-citi-bike-gbfs/blob/master/nyc_citi_bike_gbfs_dynamic_converter_json_to_parquet.py)
* Stworzenie `External Table` w BigQuery ze źródłem w GCS, automatycznym schematem oraz partycjonowaniem (`Hive`)
* Stworzenie `Standard Table` w BigQuery i bezpośrednie załadowanie danych pogodowych: [nyc_weather_direct_bq_load.py](https://github.com/jonaszwalkowiak/nyc-citi-bike-gbfs/blob/master/nyc_weather_direct_bq_load.py)

**EL[T]** - Transform

`Silver`
* Przetworzenie danych o stacjach rowerowych do warstwy silver – czyszczenie, rzutowanie typów oraz `partycjonowanie` i `klastrowanie` danych pod zapytania analityczne.
* Przetworzenie danych pogodowych do warstwy silver – spłaszczenie zagnieżdżonych struktur JSON, konwersja jednostek (np. timestampy) oraz wdrożenie strategii `incremental load` z nadpisywaniem partycji.
* `Dokumentacja` i `testy jakości` danych – definicja testów (not_null, unique, accepted_range) oraz opis pól dla modeli warstwy silver.

`Gold`
* Zintegrowanie danych o stacjach i pogodzie w ujęciu godzinowym – agregacja danych z warstwy silver do pełnych godzin, złączenie obu modeli w jedną tabelę docelową z wdrożonym `partycjonowaniem` (kolumna: data_hour) oraz `klastrowaniem` (kolumna: station_id).
* Przetworzenie zagregowanych danych do dziennego podsumowania – wyliczenie dobowych statystyk i metryk (m.in. średnia, minimalna i maksymalna dostępność rowerów, średnia temperatura oraz flaga opadów), z materializacją jako tabela stosująca analogiczne `partycjonowanie` i `klastrowanie` pod analitykę.