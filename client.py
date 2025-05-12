import base64
import json
# import keyboard
import socket
import time

import cv2
#import numpy as np
import obswebsocket
#from skimage.metrics import structural_similarity as ssim


class Client:
    def __init__(self, host, port, user_id):
        self.host = host
        self.port = port
        self.user_id = user_id
        self.cap = cv2.VideoCapture(0)
        self.width_camera = int(self.cap.get(3))
        self.height_camera = int(self.cap.get(4))
        self.compression_params = [cv2.IMWRITE_WEBP_QUALITY, 75]  # [cv2.IMWRITE_JPEG_QUALITY, 70]

    def run(self):
        ws = obswebsocket.obsws("localhost", 4455, "YU8CyaGeoT601QED")
        ws.connect()
        # with open ('test_results.txt', 'a+') as ft:
        #     ft.write(f'PSNR\tSSIM\n')
        with socket.socket() as cli:
            while True:    
                try:
                    cli.connect((self.host, self.port))
                    break
                except ConnectionRefusedError as error:
                    print(error)
                    continue
                except Exception as exp:
                    print(f'Exception: {exp}')
                    raise Exception(exp)
            init_session = {
                "user_id": self.user_id, 
                "width_camera": self.width_camera,
                "height_camera": self.height_camera
            }
            cli.sendall(json.dumps(init_session).encode())
            while True:
                ret, frame = self.cap.read()
                if (not ret) or (cv2.waitKey(1) & 0xFF == ord('q')): # or keyboard.read_key() == "q":
                    cli.close()
                    break
                success, compressed_image = cv2.imencode('.webp', frame, self.compression_params)
                res_to_db = compressed_image.tobytes()
                cli.sendall(res_to_db)
                data = cli.recv(1024*1024)
                # print(f'Отправлено {len(res_to_db)}, Получено {len(data)}')

                image_data = base64.b64decode(data)
                with open("output_image.webp", "wb") as f:
                    f.write(image_data)
                
                # cv2.imwrite('test_original.webp', frame)
                # cv2.imwrite('test_compressed.webp', frame, self.compression_params)

                # original = cv2.imread('test_original.webp')
                # original = cv2.cvtColor(original, cv2.COLOR_BGR2GRAY)
                # compressed = cv2.imread('test_compressed.webp')
                # compressed = cv2.cvtColor(compressed, cv2.COLOR_BGR2GRAY)

                # psnr_value = cv2.PSNR(original, compressed)
                # ssim_value, _ = ssim(original, compressed, full=True)

                # with open ('test_results.txt', 'a+') as ft:
                #     ft.write(f'{psnr_value:.2f}\t{ssim_value:.4f}\n')

                ws.call(obswebsocket.requests.SetInputSettings(
                    inputName="MyImage",
                    inputSettings={
                        "file": "output_image.webp"
                    }
                ))
                time.sleep(0.01)

                # decompressed_image = cv2.imdecode(np.frombuffer(data, dtype=np.uint8), cv2.IMREAD_COLOR)
                # frame = np.reshape(decompressed_image, (self.height_camera, self.width_camera, 3))
                # cv2.imshow('video', frame) 
        ws.disconnect()              


if __name__ == '__main__':
    obj = Client(
        host="127.0.0.1", 
        port=65432,
        user_id=1
    )
    obj.run()
