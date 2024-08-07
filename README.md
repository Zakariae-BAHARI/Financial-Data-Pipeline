﻿# Financial-Data-Pipeline

## Project Overview
This project sets up a data pipeline to fetch real-time stock data from the Alpha Vantage API, stream it through Kafka, store it in PostgreSQL, process it with Pandas, and finally export it to a CSV file. Using Docker, we containerize Kafka and PostgreSQL to ensure consistency and portability.

## Architecture 
![Architecture](architecture.png)

### Tools Used
- **Alpha Vantage API**: To fetch real-time stock data.
- **Apache Kafka**: For data streaming.
- **Apache Zookeeper**: To manage Kafka.
- **PostgreSQL**: To store the stock data.
- **Pandas**: For data processing and exporting to CSV.
- **Docker**: To containerize Kafka and PostgreSQL.


