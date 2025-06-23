# 💱 Exchange Rate ETL Pipeline

A simple ETL project built using **Apache Airflow** on **Astronomer** that fetches foreign exchange rates from an external API and stores the data into a PostgreSQL database.

Inspired by Krish Naik’s content — YouTube and GitHub references at the bottom.

---

## 📌 Overview

The DAG named `exchange_rate_etl_pipeline` performs the following steps:

- **Extract**: Fetch exchange rate data from the **FreeCurrency API** for selected currency pairs.
- **Transform**: Clean and format the data to match the database schema.
- **Load**: Insert the data into a PostgreSQL table named `exchange_rates`.
- **Scheduler**: Runs **daily**

---

## 🛠️ Tech Stack

- **Apache Airflow** (via Astronomer)
- **PostgreSQL**
- **FreeCurrency API**
- **Python**

---

## 🧪 Example Output

Data is stored in a table named `exchange_rates`:

| Date       | BaseCurrency | TargetCurrency | ExchangeRate | Timestamp           |
|------------|--------------|----------------|---------------|---------------------|
| 2025-06-23 | USD          | IDR            | 15432.12      | 2025-06-23 10:00:00 |
| 2025-06-23 | SGD          | IDR            | 11542.54      | 2025-06-23 10:00:00 |

---

## 📚 Reference

- YouTube: [Krish Naik - Airflow ETL Project](https://www.youtube.com/watch?v=Y_vQyMljDsE)  
- GitHub: [Krish Naik’s Repo](https://github.com/krishnaik06/ETLWeather)

---

