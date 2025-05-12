import base64
from datetime import datetime
import io
import json
import queue
import threading

from confluent_kafka import Consumer
import cv2
import numpy as np
import torch

from help_functions.database_work import DatabaseWork
from help_functions.minio_work import MinioWork
from help_functions.exceptions import (
    SaveDetectionError,
    SaveFindedError,
    SaveOriginalError, 
    SaveParsedError, 
)


class KafkaMessageWorker(threading.Thread):
    def __init__(self, worker_id, message_queue, minio_client, consumer):
        super().__init__()
        self.worker_id = worker_id
        self.message_queue = message_queue
        self.minio_client = minio_client
        self.consumer = consumer
        self.daemon = True
        self.running = True
        self.compression_params = [cv2.IMWRITE_WEBP_QUALITY, 75]  # [cv2.IMWRITE_JPEG_QUALITY, 70]
        self.path_video_images = 'video-images'
        self.path_detections = 'detections'      
        self.path_video_images_original = 'original'
        self.path_video_images_parsed = 'parsed'
        self.path_video_images_finded = 'finded'


    def handle_message(self, input_data):
        try:
            json_data = json.loads(input_data.decode('utf-8'))
        except Exception as e:
            print(f'HANDLE_MESSAGE: {len(input_data) / 1024 // 1024} MB')
            return None
        try:
            session_id = json_data["session_id"]
            datetime_image = datetime.fromisoformat(json_data["datetime_image"])
            number_image = json_data["number_image"]
            states_classes = json_data["states_classes"]
            original_bytes = base64.b64decode(json_data["original_bytes"])
            parsed_bytes = base64.b64decode(json_data["parsed_bytes"])
            finded_bytes = base64.b64decode(json_data["finded_bytes"])
            # height = json_data["height"]
            # width = json_data["width"]
            if "detections_data_torch" in json_data.keys():
                binary_data = base64.b64decode(json_data["detections_data_torch"])
                buffer = io.BytesIO(binary_data)
                detections_data = torch.load(buffer)
                detections_cls = json_data["detections_cls"]
                detections_conf = json_data["detections_conf"]
                detections_id = json_data["detections_id"]
                detections = (detections_data, detections_cls, detections_conf, detections_id)
            else:
                detections = None
            return session_id, datetime_image, number_image, states_classes, original_bytes, parsed_bytes, finded_bytes, detections
        except Exception as e:
            print('HANDLE_MESSAGE:', e)
            return None

    def insert_video_image(self, session_id, current_datetime, strftime_datetime, number, original_bytes, parsed_bytes, finded_bytes):
        try:
            path_original = f'{self.path_video_images_original}/session_{session_id}/{strftime_datetime}.webp'
            path_parsed = f'{self.path_video_images_parsed}/session_{session_id}/{strftime_datetime}.webp'
            path_finded = f'{self.path_video_images_finded}/session_{session_id}/{strftime_datetime}.webp'

            data_stream_original = io.BytesIO(original_bytes)
            data_stream_parsed = io.BytesIO(parsed_bytes)
            data_stream_finded = io.BytesIO(finded_bytes)
            self.minio_client.put_object(
                self.path_video_images,
                path_original,
                data_stream_original,
                length=len(original_bytes)
            )
            self.minio_client.put_object(
                self.path_video_images,
                path_parsed,
                data_stream_parsed,
                length=len(parsed_bytes)
            )
            self.minio_client.put_object(
                self.path_video_images,
                path_finded,
                data_stream_finded,
                length=len(finded_bytes)
            )
            anonymization_id = DatabaseWork.get_field("""
                SELECT anonymization_id 
                FROM industrial.anonymizations 
                ORDER BY set_datetime DESC
                LIMIT 1
            """)
            query= f"""
                INSERT INTO industrial.video_images 
                (datetime, session_id, number, path_image_original, path_image_parsed, path_image_finded, anonymization_id) 
                VALUES (
                '{current_datetime}', {session_id}, {number}, '{path_original}', '{path_parsed}', '{path_finded}', {anonymization_id}
                )
            """
            DatabaseWork.row_add_edit(query)
        except SaveOriginalError as o:
            print("INSERT_VIDEO_IMAGE SaveOriginalError:", o)
        except SaveParsedError as p:
            print("INSERT_VIDEO_IMAGE SaveParsedError:", p)
        except SaveFindedError as f:
            print("INSERT_VIDEO_IMAGE SaveFindedError:", f)
        except Exception as e:
            print("INSERT_VIDEO_IMAGE Exception:", e)

    def insert_detections(self, session_id, current_datetime, strftime_datetime, detections, states_classes):
        try:
            masks_data, boxes_cls, boxes_conf, boxes_id = detections
            zipped = sorted(list(zip(masks_data, boxes_cls, boxes_conf, boxes_id)), key=lambda x: x[2], reverse=True)
            masks_data, boxes_cls, boxes_conf, boxes_id = zip(*zipped)
            query_start = f"""
                INSERT INTO industrial.detections 
                (detection_id, datetime, session_id, is_active, probability, path_mask, class_number, element_id) 
                VALUES
            """
            query = query_start
            for i in range(len(boxes_conf)):
                class_number = int(boxes_cls[i])
                if True: # class_number in [0]: #[0,67]
                    detection_id = int(boxes_id[i])
                    path_mask = f'session_{session_id}/{strftime_datetime}__{detection_id}.webp'
                    mask_white = masks_data[i].numpy().astype(np.uint8) * 255

                    _, compressed_mask = cv2.imencode('.webp', mask_white, self.compression_params)
                    compressed_mask_bytes = compressed_mask.tobytes()
                    data_stream = io.BytesIO(compressed_mask_bytes)
                    try:
                        self.minio_client.put_object(
                            self.path_detections,
                            path_mask,
                            data_stream,
                            length=len(compressed_mask_bytes)
                        )
                    except Exception as d:
                        print('INSERT_DETECTIONS SaveDetectionError:', d)
                        continue
                    is_active = states_classes.get(f'{class_number}_{detection_id}', False)
                    probability = boxes_conf[i]
                    element_id = DatabaseWork.get_field(f"""
                        SELECT element_id 
                        FROM industrial.classes 
                        WHERE class_number = {class_number} 
                            AND status = True
                    """)
                    query += f" ({detection_id}, '{current_datetime}', {session_id}, {is_active}, {probability}, '{path_mask}', {class_number}, {element_id}),"
            if query != query_start:
                DatabaseWork.row_add_edit(query[:-1])
        except Exception as e:
            print('INSERT_DETECTIONS Exception:', e)

    def process_message(self, msg):
        print(f"Worker-{self.worker_id} обрабатывает")
        data = msg.value()
        print(round(len(data) / 1024 / 1024, 2))
        session_id, datetime_image, number_image, states_classes, original_bytes, parsed_bytes, finded_bytes, detections = self.handle_message(data)
        strftime_datetime = datetime_image.strftime("%Y%m%d_%H%M%S") + f'_{datetime_image.microsecond // 1000:03d}'
        self.insert_video_image(session_id, datetime_image, strftime_datetime, number_image, original_bytes, parsed_bytes, finded_bytes)
        if detections is not None: 
            self.insert_detections(session_id, datetime_image, strftime_datetime, detections, states_classes)

    def stop(self):
        self.running = False

    def run(self):
        while self.running:
            msg = self.message_queue.get()
            if msg is None:
                break
            try:
                self.process_message(msg)
                self.consumer.commit(msg)
            except Exception as e:
                print(f"Worker-{self.worker_id} ошибка: {e}")
            finally:
                self.message_queue.task_done()


class KafkaConsumerManager:
    def __init__(self, topic, num_workers=5, queue_size=1000):
        self.topic = topic
        self.num_workers = num_workers
        self.queue_size = queue_size
        self.message_queue = queue.Queue(maxsize=queue_size)
        self.workers = []
        self.minio_client = MinioWork.minio_connection()
        self.consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'my-threaded-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })
        self.consumer.subscribe([self.topic])

    def start_workers(self):
        for i in range(self.num_workers):
            worker = KafkaMessageWorker(
                worker_id=i,
                message_queue=self.message_queue,
                minio_client=self.minio_client,
                consumer=self.consumer
            )
            worker.start()
            self.workers.append(worker)

    def run(self):
        try:
            self.start_workers()
            while True:
                msg = self.consumer.poll(0.1)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Ошибка Kafka: {msg.error()}")
                    continue
                self.message_queue.put(msg, block=True)
        except KeyboardInterrupt:
            print("\nПолучен сигнал остановки...")
        finally:
            self.shutdown()

    def shutdown(self):
        print("Остановка рабочих потоков...")
        for _ in range(self.num_workers):
            self.message_queue.put(None)
        for worker in self.workers:
            worker.stop()
            worker.join()
        self.consumer.close()
        print("Все потоки остановлены.")


if __name__ == "__main__":
    manager = KafkaConsumerManager(topic='NewTopic', num_workers=5)
    manager.run()
