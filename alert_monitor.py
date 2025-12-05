#!/usr/bin/env python3
import json
from kafka import KafkaConsumer
import sys

class AlertMonitor:
    def __init__(self):
        print("Запускаем Alert Monitor...")
        print("Слушаем алерты...")
        print("-" * 50)
        
        self.consumer = KafkaConsumer(
            'alerts-topic',
            bootstrap_servers='localhost:29092',
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            api_version=(2, 0, 2)
        )
    
    def run(self):
        try:
            for message in self.consumer:
                alert = message.value
                print("\n" + "-" * 50)
                print("NEW ALERT RECEIVED!")
                print("-" * 50)
                print(f"Продукт: {alert['product_name']} (ID: {alert['product_id']})")
                print(f"Итоговая сумма покупок: ${alert['revenue_total']:.2f}")
                print(f"Лимит: ${alert['threshold']}")
                print(f"Время: {alert['timestamp']}")
                print(f"Сообщение: {alert['message']}")
                print("-" * 50)
        
        except KeyboardInterrupt:
            print("\nStopping monitor...")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    monitor = AlertMonitor()
    monitor.run()