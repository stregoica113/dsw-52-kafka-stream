#!/usr/bin/env python3
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import sys

class DataGenerator:
    def __init__(self):
        print("Инициализируем Kafka Producer...")
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:29092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(2, 0, 2)
        )
        self.products = {}
        
    def create_products(self):
        print("Создаем продукты...")
        for i in range(100):
            product = {
                'id': i,
                'name': f'Product_{i}',
                'description': f'Description for product {i}',
                'price': round(random.uniform(50, 70), 2)
            }
            self.products[i] = product
            self.producer.send('products-topic', product)
            if i % 20 == 0:
                print(f"  Создано {i+1} из 100 продуктов")
        
        self.producer.flush()
        print("!!!Продукты созданы и отправлены в Kafka!!!")
        
    def generate_purchases(self):
        print("Создаем покупки...")
        print("Нажмите Ctrl+C для остановки процесса")
        
        purchase_id = 0
        try:
            while True:
                # Выбираем случайный продукт
                # 30% покупок - продукт 13, 70% - случайные
                if random.random() < 0.3:  # 30% вероятность
                    product_id = 13  # Фиксированный продукт для теста
                else:
                    product_id = random.randint(0, 100)  # 70% случайных
                quantity = random.randint(1, 11)
                
                purchase = {
                    'id': purchase_id,
                    'quantity': quantity,
                    'productid': product_id
                }

                self.producer.send('purchases-topic', purchase)
                
                # Каждые 10 покупок выводим информацию
                #if purchase_id % 10 == 0:
                #Выводим информацию о покупках
                product = self.products.get(product_id, {})
                price = product.get('price', 60)
                amount = quantity * price
                print(f"Покупка #{purchase_id}: продукт={product_id}, количество={quantity}, цена={amount}")
                
                purchase_id += 1
                time.sleep(0.5)  # 2 покупки в секунду
                
        except KeyboardInterrupt:
            print("\n\nОстанавливаем генератор...")
        finally:
            self.producer.close()
            print("Генератор остановлен")

if __name__ == "__main__":
    print("=" * 50)
    print("KAFKA DATA GENERATOR")
    print("=" * 50)
    
    generator = DataGenerator()
    generator.create_products()
    generator.generate_purchases()