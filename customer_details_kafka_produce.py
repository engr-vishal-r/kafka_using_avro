import pymysql
import pandas as pd
from datetime import datetime
from confluent_kafka import Producer
import os
import avro.schema
import avro.io
import io
import json
from dotenv import load_dotenv
from pandas import Timestamp

load_dotenv() 
config = {'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS")}
producer = Producer(config)

def delivery_report(err, msg):
    if err is not None:
        print('Delivery failed:', err)
    else:
        print('Message delivered to', msg.topic(), msg.partition())

def serialize_avro(data,schema):
    bytes_writer=io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer=avro.io.DatumWriter(schema)
    writer.write(data, encoder)
    return bytes_writer.getvalue()
    

def fetch_data_from_mysql():
    mysql_config = {
        'host': os.getenv("MYSQL_HOST"),
        'port': int(os.getenv("MYSQL_PORT")),
        'user': os.getenv("MYSQL_USER"),
        'password': os.getenv("MYSQL_PASSWORD"),
        'database': os.getenv("MYSQL_DB")
    }

    connection = pymysql.connect(**mysql_config)
    try:
        query = "SELECT * FROM customer WHERE defaulter='Y'"
        df = pd.read_sql(query, connection)
        print('Connected to database successfully!!')
        return df
    finally:
        connection.close()

def transform_data(df):
    df_transformed = df[df['defaulter'] == 'Y']
    print('Data transformed successfully!')
    return df_transformed

def clean_avro_record(record):
    def to_str_or_none(value):
        if isinstance(value, Timestamp):
            return str(value)
        return str(value) if value else None

    return {
        "customer_name": str(record.get("customer_name", "")),
        "card_number": int(record.get("card_number", 0)),
        "mobile_no": int(record.get("mobile_no", 0)),
        "address_line1": str(record.get("address_line1", "")),
        "address_line2": record.get("address_line2") or None,
        "area": int(record.get("area", 0)),
        "defaulter": str(record.get("defaulter", "N")),
        "pending_amount": float(record.get("pending_amount", 0.0)),
        "status": str(record.get("status", "")),
        "enrolled_date": to_str_or_none(record.get("enrolled_date")),
        "updated_date": to_str_or_none(record.get("updated_date")),
        "billing_frequency": str(record.get("billing_frequency", ""))
    }

def write_data_to_file(df):
    output_dir = 'F:/E2E_Projects/extract'
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    file_name = f'etl_output_{timestamp}.csv'
    file_path = os.path.join(output_dir, file_name)
    df.to_csv(file_path, index=False)
    print(f'Data written to {file_path}')

#required_fields={"customer_name", "card_number", "defaulter"}
def send_to_kafka(df):
    schema_path=os.getenv("AVRO_SCHEMA_PATH")
    with open(schema_path, "r") as f:
        schema = avro.schema.parse(f.read())
    for _, row in df.iterrows():
        message_dict = clean_avro_record(row.to_dict())
        #filtered_data = {k: v for k, v in message_dict.items() if k in required_fields}
        avro_bytes=serialize_avro(message_dict, schema)
        producer.produce(
        os.getenv("KAFKA_TOPIC"),
        key=str(message_dict["card_number"]),
        value=avro_bytes,
        callback=delivery_report
    )
        print('Messages published successfully!!')
    producer.flush()

def etl_process():
    df = fetch_data_from_mysql()
    df_transformed = transform_data(df)
    write_data_to_file(df_transformed)
    send_to_kafka(df_transformed)

if __name__ == "__main__":
    etl_process()