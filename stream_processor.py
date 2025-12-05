#!/usr/bin/env python3
import json
import time
from datetime import datetime, timedelta
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

class KafkaStreamProcessor:
    def __init__(self):
        logger.info("Инициализация Kafka Stream Processor...")
        
        # Настройка подключения к Kafka
        self.bootstrap_servers = 'localhost:29092'
        
        # Хранилища данных
        self.products = {}  # id -> product
        self.revenue_windows = defaultdict(list)  # product_id -> list of (time, amount)
        
        # Настройки
        self.window_minutes = 1
        self.alert_threshold = 3000
        
        # Создаем консьюмер для двух топиков
        logger.info("Connecting to Kafka topics...")
        self.consumer = KafkaConsumer(
            'products-topic',
            'purchases-topic',
            bootstrap_servers=self.bootstrap_servers,
            group_id='mac-stream-group',
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            api_version=(2, 0, 2),
            enable_auto_commit=True
        )
        
        # Создаем продюсера для алертов
        self.alert_producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        logger.info("Kafka Stream Processor инициализирован")
    
    def process_product(self, product_data):
        """Обработка сообщения о продукте"""
        product_id = product_data['id']
        self.products[product_id] = product_data
        logger.debug(f"Продукт {product_id}: ${product_data['price']}")
    
    def process_purchase(self, purchase_data):
        """Обработка покупки"""
        product_id = purchase_data['productid']
        
        # Проверяем, есть ли информация о продукте
        if product_id not in self.products:
            logger.warning(f"!!!Неизвестный продукт {product_id}, не покупаем")
            return
        
        # Получаем продукт
        product = self.products[product_id]
        
        # Вычисляем сумму покупки
        quantity = purchase_data['quantity']
        price = product['price']
        purchase_amount = quantity * price
        
        # Текущее время
        current_time = datetime.now()
        
        # Добавляем покупку в окно
        self.revenue_windows[product_id].append({
            'time': current_time,
            'amount': purchase_amount,
            'quantity': quantity
        })
        
        # Очищаем старые записи (старше N минут)
        self._clean_old_records(product_id, current_time)
        
        # Проверяем сумму за окно
        self._check_alert_threshold(product_id, current_time)
        
        # Логируем информацию
        window_sum = sum(item['amount'] for item in self.revenue_windows[product_id])
        logger.info(f"Покупка: продукт={product_id}, количество=${purchase_amount:.2f}, итого за минуту=${window_sum:.2f}")
    
    def _clean_old_records(self, product_id, current_time):
        """Удаление записей старше заданного окна"""
        cutoff_time = current_time - timedelta(minutes=self.window_minutes)
        self.revenue_windows[product_id] = [
            item for item in self.revenue_windows[product_id]
            if item['time'] > cutoff_time
        ]
    
    def _check_alert_threshold(self, product_id, current_time):
        """Проверка превышения порога и отправка алерта"""
        window_items = self.revenue_windows[product_id]
        if not window_items:
            return
        
        total_amount = sum(item['amount'] for item in window_items)
        
        if total_amount > self.alert_threshold:
            # Создаем сообщение об алерте
            product = self.products[product_id]
            alert_message = {
                'type': 'REVENUE_ALERT',
                'timestamp': current_time.isoformat(),
                'product_id': product_id,
                'product_name': product['name'],
                'revenue_total': round(total_amount, 2),
                'threshold': self.alert_threshold,
                'window_minutes': self.window_minutes,
                'purchase_count': len(window_items),
                'message': f'АЛЯРМ: Продукт {product_id} превысил сумму ${self.alert_threshold} в {self.window_minutes} минуту'
            }
            
            # Отправляем алерт
            self.alert_producer.send('alerts-topic', alert_message)
            
            # Логируем алерт
            logger.warning(f"АЛЯРМ ОТПРАВЛЕН: {alert_message['message']}")
            logger.warning(f"    Итого: ${total_amount:.2f}, Лимит: ${self.alert_threshold}")
            
            # Очищаем окно после алерта
            self.revenue_windows[product_id].clear()
    
    def run(self):
        """Запуск обработки"""
        logger.info("Starting stream processing...")
        logger.info(f"Alert threshold: ${self.alert_threshold}")
        logger.info(f"Time window: {self.window_minutes} minute(s)")
        logger.info("Waiting for messages...")
        
        try:
            for message in self.consumer:
                try:
                    data = message.value
                    
                    if message.topic == 'products-topic':
                        self.process_product(data)
                    
                    elif message.topic == 'purchases-topic':
                        self.process_purchase(data)
                
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        
        except KeyboardInterrupt:
            logger.info("\Останавливаем Kafka stream processor...")
        
        finally:
            self.consumer.close()
            self.alert_producer.close()
            logger.info("Kafka stream processor остановлен")

if __name__ == "__main__":
    print("=" * 50)
    print("KAFKA STREAMS PROCESSOR")
    print("=" * 50)
    
    processor = KafkaStreamProcessor()
    processor.run()