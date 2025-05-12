import base64
from datetime import datetime
import io
import json
import queue
import socket
import threading

from confluent_kafka import Producer
import cv2
import numpy as np
import torch
from ultralytics import YOLO

from help_functions.database_work import DatabaseWork
from help_functions.minio_work import MinioWork
from help_functions.exceptions import LengthError


class Server:
    def __init__(self, num_workers, host, port):
        self.lock = threading.Lock()
        self.connections = queue.Queue(num_workers)
        self.host = host
        self.port = port
        self.classes_priorities = None
        self.method = None
        self.model = None
        self.compression_params = [cv2.IMWRITE_WEBP_QUALITY, 75]  # [cv2.IMWRITE_JPEG_QUALITY, 70]
        self.path_video_images = 'video-images'
        self.path_detections = 'detections'   
        self.producer = Producer({
            'bootstrap.servers': 'localhost:9092', 
            'client.id': f'{self.host}_{self.port}',
            'message.max.bytes': 1024*1024*32,  # 32MB (по умолчанию 1MB)
            # 'compression.type': 'gzip',      # Сжатие для уменьшения размера
            'queue.buffering.max.messages': 100000,
            'queue.buffering.max.kbytes': 1024*128  # 128MB буфер
        })    
        self.minio_client = MinioWork.minio_connection()

    @staticmethod
    def delivery_report(err, msg):
        if err is not None:
            print(f'Ошибка доставки сообщения: {err}')
        else:
            print(f'Сообщение доставлено в {msg.topic()} [{msg.partition()}]')

    def handler_session(self, data):
        try:
            current_time = datetime.now()
            if isinstance(data, int):
                query = f"""
                    UPDATE industrial.sessions 
                    SET 
                        finish_datetime = '{current_time}' 
                    WHERE session_id = {data}
                """
                DatabaseWork.row_add_edit(query)
                return None
            else:
                info_session = json.loads(data.decode('utf-8'))
                query = f"""
                    INSERT INTO industrial.sessions 
                    (width_camera, height_camera, start_datetime, user_id) 
                    VALUES ({info_session['width_camera']}, {info_session['height_camera']}, '{current_time}', {info_session['user_id']})
                """
                DatabaseWork.row_add_edit(query)
                session_id = DatabaseWork.get_field(f"""
                    SELECT session_id 
                    FROM industrial.sessions 
                    WHERE user_id = {info_session['user_id']} 
                    ORDER BY start_datetime DESC 
                    LIMIT 1
                """)
                return session_id, info_session['width_camera'], info_session['height_camera']
        except Exception as e:
            print('HANDLER SESSION:', e)

    def handler_video_image(self, data, info_classes, width, height):
        try:
            decompressed_image = cv2.imdecode(np.frombuffer(data, dtype=np.uint8), cv2.IMREAD_COLOR)
            frame = np.reshape(decompressed_image, (height, width, 3))
            results = self.model.track(frame, persist=True, tracker='bytetrack_my.yaml')[0]
            dir_masks = dir(results.masks)
            dir_boxes = dir(results.boxes)
            parsed = None
            if 'data' in dir_masks \
                    and 'cls' in dir_boxes \
                    and 'conf' in dir_boxes \
                    and 'id' in dir_boxes \
                    and results.boxes.id is not None:
                if len(results.boxes.cls) == len(results.boxes.id):
                    masks_data = results.masks.data
                    boxes_cls = list(map(int, results.boxes.cls.tolist()))
                    boxes_conf = results.boxes.conf.tolist()
                    boxes_id = list(map(int, results.boxes.id.tolist()))
                    boxes = [{f'{boxes_cls[i]}_{boxes_id[i]}': [boxes_conf[i], masks_data[i]]} for i in range(len(boxes_cls))]
                    priorities = [self.classes_priorities[item] for item in boxes_cls if item in self.classes_priorities]
                    if len(priorities):
                        parsed = self.handler_detections(frame, boxes, priorities, info_classes)
                        detections = [masks_data, boxes_cls, boxes_conf, boxes_id]
                    else:
                        self.change_states(info_classes)
                        detections = None     
                else:
                    raise LengthError     
            else:
                self.change_states(info_classes)
                detections = None
            back_frame = parsed if parsed is not None else frame
            success_parsed, compressed_parsed = cv2.imencode('.webp', back_frame, self.compression_params)
            compressed_parsed_bytes = compressed_parsed.tobytes()  
            img_b64 = base64.b64encode(compressed_parsed) #.decode("utf-8")
            success_finded, compressed_finded = cv2.imencode('.webp', results.plot(), self.compression_params)
            compressed_finded_bytes = compressed_finded.tobytes() 
            return img_b64, compressed_parsed_bytes, compressed_finded_bytes, detections
        except LengthError as l:
            print("HANDLER_VIDEO_IMAGE LengthError:", l)   
            return None
        except Exception as e:
            print("HANDLER_VIDEO_IMAGE Exception:", e)   
            return None

    def make_anonymization(self, frame, mask):
        try:
            mask = mask.data.cpu().numpy().squeeze()
            mask = (mask * 255).astype(np.uint8)
            if self.method == 'block':  # blur
                inverted_mask = cv2.bitwise_not(mask)
                result = cv2.bitwise_and(frame, frame, mask=inverted_mask)
            elif self.method == 'pixels':  # 'hard_pixels'
                small_image = cv2.resize(frame, (32, 32), interpolation=cv2.INTER_LINEAR)
                pixelated_image = cv2.resize(small_image, (frame.shape[1], frame.shape[0]), interpolation=cv2.INTER_NEAREST)
                result = np.where(mask[:, :, None] == 255, pixelated_image, frame)
            elif self.method == 'gaussian blur':  # 'soft_pixels'
                blurred_image = cv2.GaussianBlur(frame, (51, 51), 0)
                result = np.where(mask[:, :, None] == 255, blurred_image, frame) 
            elif self.method == 'wb pixels':  # 'hard_wb_pixels'
                gray_image = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                gray_image = cv2.cvtColor(gray_image, cv2.COLOR_GRAY2BGR)
                small_gray_image = cv2.resize(gray_image, (32, 32), interpolation=cv2.INTER_LINEAR)
                pixelated_gray_image = cv2.resize(small_gray_image, (frame.shape[1], frame.shape[0]), interpolation=cv2.INTER_NEAREST)
                result = np.where(mask[:, :, None] == 255, pixelated_gray_image, frame)
            elif self.method == 'wb gaussian blur':  # 'soft_wb_pixels' 
                gray_image = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                gray_image = cv2.cvtColor(gray_image, cv2.COLOR_GRAY2BGR)
                blurred_gray_image = cv2.GaussianBlur(gray_image, (51, 51), 0)
                result = np.where(mask[:, :, None] == 255, blurred_gray_image, frame)
            else:
                return frame
            return result
        except Exception as e:
            print('MAKE_ANONYMIZATION:', e)
            return frame

    def handler_detections(self, frame, boxes, priorities, info_classes):
        try:
            zipped = sorted(list(zip(boxes, priorities)), key=lambda x: x[1], reverse=True)
            boxes, _ = zip(*zipped)
        except Exception as e:
            print('HANDLER_DETECTIONS ZIPPED:', e)
        try:
            dct_boxes = {}
            for box in boxes:
                dct_boxes.update(box)
            for key in info_classes.keys():
                if key in dct_boxes.keys() and dct_boxes[key][0] > 0.25:
                    if info_classes[key]['state'] == False and info_classes[key]['is_'] < 3:
                        info_classes[key]['is_'] += 1
                    elif info_classes[key]['state'] == False:
                        info_classes[key]['is_'] = 0
                        info_classes[key]['state'] = True
                        if info_classes[key]['category'] == "Запрещен":
                            frame = self.make_anonymization(frame, dct_boxes[key][1])
                    else:
                        if info_classes[key]['category'] == "Запрещен":
                            frame = self.make_anonymization(frame, dct_boxes[key][1])
                else:
                    if info_classes[key]['state'] == True and info_classes[key]['no_'] < 10:
                        info_classes[key]['no_'] += 1
                        if info_classes[key]['category'] == "Запрещен":
                            frame = self.make_anonymization(frame, dct_boxes[key][1])
                    elif info_classes[key]['state'] == True:
                        info_classes[key]['no_'] = 0
                        info_classes[key]['state'] = False
                    else:
                        info_classes[key]['is_'] = 0
            for key in dct_boxes.keys():
                if key not in info_classes.keys():
                    current_class = key.split('_')[0]
                    category_name = DatabaseWork.get_field(f"""
                        SELECT categories.name 
                        FROM industrial.classes classes
                        INNER JOIN industrial.elements elements
                            ON classes.element_id=elements.element_id 
                                AND classes.status=true 
                                AND classes.class_number={current_class}
                                AND elements.status=true
                        INNER JOIN industrial.categories categories
                            ON categories.category_id=elements.category_id 
                                AND categories.status=true
                    """)
                    info_classes[key] = {'state': False, 'is_': 0, 'no_': 0, 'category': category_name}#'Запрещен'} 
                    if dct_boxes[key][0] > 0.25:
                        info_classes[key]['is_'] = 1    
            return frame               
        except Exception as e:
            print('HANDLER_DETECTIONS FRAME:', e)

    def change_states(self, info_classes):
        try:
            for key in info_classes.keys():
                if info_classes[key]['state'] == True and info_classes[key]['no_'] < 10:
                    info_classes[key]['no_'] += 1
                elif info_classes[key]['state'] == True:
                    info_classes[key]['no_'] = 0
                    info_classes[key]['state'] = False
                else:
                    info_classes[key]['is_'] = 0
        except Exception as e:
            print('CHANGE_STATES:', e)

    def update_info_from_db(self):
        list_of_tuples = DatabaseWork.get_rows("""
            SELECT class_number, priority 
            FROM (
                SELECT element_id, priority 
                FROM industrial.elements 
                WHERE status = True
            ) elements
            INNER JOIN (
                SELECT class_number, element_id 
                FROM industrial.classes 
                WHERE status = True
            ) classes
                ON elements.element_id = classes.element_id
        """)
        classes_priorities = {item[0]: item[1] for item in list_of_tuples}
        if self.classes_priorities is None or self.classes_priorities != classes_priorities:
            self.classes_priorities = classes_priorities
        method, model, _ = DatabaseWork.get_row("""
            SELECT methods.name method, models.name model, anonymizations.set_datetime
            FROM industrial.anonymizations anonymizations
            INNER JOIN industrial.methods methods
                ON anonymizations.method_id=methods.method_id 
                    AND methods.status=true
            INNER JOIN industrial.models models
                ON anonymizations.model_id=models.model_id 
                    AND models.status=true	
            ORDER BY anonymizations.set_datetime DESC
            LIMIT 1
        """)
        if self.method is None or self.method != method:
            self.method = method
        if self.model is None or self.model != model:
            bucket_name = 'models'
            object_name = model
            file_path = 'tmp/model.pt'
            self.minio_client.fget_object(
                bucket_name,
                object_name,
                file_path,
            )
            self.model = YOLO(file_path)
            
    def worker(self):
        while self.connections.empty():
            continue
        self.lock.acquire()
        conn = self.connections.get()
        self.lock.release()
        input_data = conn.recv(1024)
        session_id, session_width, session_height = self.handler_session(input_data)
        number_image = 0
        last_time = 0
        info_classes = {}
        while True:
            try:
                input_data = conn.recv(1024*1024)
                if not input_data:
                    break
                img_b64, compressed_parsed_bytes, compressed_finded_bytes, detections = self.handler_video_image(
                    input_data, info_classes, session_width, session_height
                )
                conn.sendall(img_b64)
                datetime_image = datetime.now()
                number_image += 1
                if int(datetime_image.timestamp()) > last_time:
                    output_data = {
                        "session_id": session_id,
                        "datetime_image": datetime_image.isoformat(),
                        "number_image": number_image,
                        "states_classes": {key: value['state'] for key, value in info_classes.items()},
                        "original_bytes": base64.b64encode(input_data).decode('utf-8'),
                        "parsed_bytes": base64.b64encode(compressed_parsed_bytes).decode('utf-8'),
                        "finded_bytes": base64.b64encode(compressed_finded_bytes).decode('utf-8'),
                        # "height": session_height,
                        # "width": session_width,
                    }
                    if detections is not None:
                        buffer = io.BytesIO()
                        torch.save(detections[0], buffer)
                        mask_bits = buffer.getvalue()
                        output_data['detections_data_torch'] = base64.b64encode(mask_bits).decode('utf-8')
                        output_data['detections_cls'] = detections[1]
                        output_data['detections_conf'] = detections[2]
                        output_data['detections_id'] = detections[3]
                    self.producer.produce(
                       "NewTopic", 
                       key=f'{self.host}_{self.port}', 
                       value=json.dumps(output_data).encode('utf-8'), 
                       callback=Server.delivery_report
                    )
                    self.producer.poll(0.1)
                    self.producer.flush()
                    last_time = datetime_image.timestamp()
                if datetime_image.second == 0:
                    self.update_info_from_db()
            except ConnectionResetError as reset:
                print("WORKER ConnectionResetError:", reset)
                break
            except Exception as e:
                print("WORKER Exception:", e)
                continue
        self.handler_session(session_id)
        conn.close()

    def master(self, count):
        if not self.minio_client.bucket_exists(self.path_video_images):
           self.minio_client.make_bucket(self.path_video_images)
        if not self.minio_client.bucket_exists(self.path_detections):
           self.minio_client.make_bucket(self.path_detections)
        self.update_info_from_db()
        with socket.socket() as server:
            server.bind((self.host, self.port))
            server.listen()
            while count:
                try:
                    count -= 1
                    conn, _ = server.accept()
                    self.connections.put(conn)
                except Exception as e:
                    print("MASTER:", e)
                    continue


class CreateThreads:
    def __init__(self, num_workers, host, port):
        self.obj = Server(num_workers, host, port)
        self.master = threading.Thread(
            target=self.obj.master,
            args=(num_workers,)
        )
        self.workers = [
            threading.Thread(
                target=self.obj.worker,
            )
            for _ in range(num_workers)
        ]

    def run(self):
        self.master.start()
        for worker in self.workers:
            worker.start()

        self.master.join()
        for worker in self.workers:
            worker.join()


if __name__ == "__main__":
    main = CreateThreads(
        num_workers=1, 
        host="127.0.0.1", 
        port=65432
    )
    main.run()
