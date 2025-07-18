# Kafka Using Avro (MySQL → Kafka → Avro Serialization)

This project demonstrates a simple end-to-end data pipeline that fetches data from a MySQL database, serializes it using **Apache Avro**, and publishes it to **Apache Kafka**. The data is then consumed from Kafka and deserialized back into readable form.

---

## 📌 Key Components

- **MySQL** – Source of customer data
- **Apache Kafka** – Message broker for streaming data
- **Apache Avro** – Serialization format for compact, fast data exchange
- **Python (Confluent Kafka client)** – Used for producer/consumer
- **customer.avsc** – Avro schema file used for serialization/deserialization

---

## 🔄 Pipeline Flow

```text
MySQL → Python Producer → Avro Serialization → Kafka Topic → Python Consumer → Avro Deserialization
