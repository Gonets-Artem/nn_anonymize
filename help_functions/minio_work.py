import io

from minio import Minio
from minio.commonconfig import CopySource
from minio.error import S3Error


class MinioWork:
    @staticmethod
    def minio_connection(address="localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False):
        try:
            return Minio(address, access_key=access_key, secret_key=secret_key, secure=secure)
        except Exception as e:
            print("connection:", e)
            return None
        
    @staticmethod
    def save_io(bytes, bucket_name, object_name):
        data_stream = io.BytesIO(bytes)
        minio_client = MinioWork.minio_connection()
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=data_stream,
            length=len(bytes)
        )

    @staticmethod
    def get_object(bucket_name, object_name):
        minio_client = MinioWork.minio_connection()
        try:
            response = minio_client.get_object(
                bucket_name=bucket_name,
                object_name=object_name
            )
            return response.read()
        except S3Error as exc:
            print(f"Ошибка получения файла: {exc}")
            return None
        finally:
            response.close()
            response.release_conn()

    @staticmethod
    def get_objects(bucket_name):
        minio_client = MinioWork.minio_connection()
        objects = minio_client.list_objects(
            bucket_name=bucket_name
        )
        return objects

    @staticmethod
    def find_object_name(bucket_name, object_name):
        minio_client = MinioWork.minio_connection()
        try:
            minio_client.stat_object(
                bucket_name=bucket_name, 
                object_name=object_name
            )
            return True
        except S3Error as exc:
            print(f"Ошибка S3: {exc}")
            return False
    
    @staticmethod
    def rename_object_name(bucket_name, old_name, new_name):
        minio_client = MinioWork.minio_connection()
        minio_client.copy_object(
            bucket_name=bucket_name,
            object_name=new_name,
            source=CopySource(bucket_name, old_name)
        )
        minio_client.remove_object(bucket_name, old_name)
