import asyncio
from datetime import datetime, timedelta

import cv2
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st 

from help_functions.database_work import DatabaseWork
from help_functions.minio_work import MinioWork


def settings():
    st.set_page_config(
        page_title="Веб-сервис нейросетевой анонимизации субъектов и объектов в видеоконференции", 
        layout="wide"
    )
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"][aria-expanded="true"] > div:first-child{
            width: 400px;
        }
        [data-testid="stSidebar"][aria-expanded="false"] > div:first-child{
            width: 400px;
            margin-left: -400px;
        }
        """,
        unsafe_allow_html=True,
    )
    st.title("Мониторинг")
    st.divider()


async def monitoring_video_stream(
    cur_session_id, 
    st_time, 
    st_subh_images,
    st_info_original, st_image_original, 
    st_info_parsed, st_image_parsed, 
    st_info_finded, st_image_finded,
    st_subh_detections,
    st_table_21, st_mask_21,
    st_table_22, st_mask_22,
    st_table_23, st_mask_23,
    st_table_24, st_mask_24,
    st_table_25, st_mask_25,
):
    i = 0
    while True:
        
        video_images = DatabaseWork.get_rows(f"""
            SELECT 
                video_images.datetime, 
                video_images.path_image_original, 
                video_images.path_image_parsed, 
                video_images.path_image_finded,
                sessions.width_camera,
                sessions.height_camera,
                detections.detection_id,
                detections.is_active,
                detections.probability,
                detections.path_mask,
                elements.name element_name,
                categories.name category_name,
                types.name type_name
            FROM (
                SELECT 
                    datetime, session_id, path_image_original, path_image_parsed, path_image_finded 
                FROM industrial.video_images
                WHERE session_id={cur_session_id.split(':')[0]}
                ORDER BY datetime DESC
                LIMIT 1
            ) video_images
            INNER JOIN industrial.sessions sessions
                ON sessions.session_id=video_images.session_id
            LEFT JOIN industrial.detections detections
                ON detections.datetime=video_images.datetime
                    AND detections.session_id=video_images.session_id
            LEFT JOIN industrial.elements elements
                ON elements.element_id=detections.element_id
            LEFT JOIN industrial.categories categories
                ON categories.category_id=elements.category_id
            LEFT JOIN industrial.types types
                ON types.type_id=elements.type_id
            ORDER BY probability DESC
        """)

        df_video_images = pd.DataFrame(video_images)

        datetime_ = df_video_images.iloc[0, 0]
        path_image_original = df_video_images.iloc[0, 1]
        path_image_parsed = df_video_images.iloc[0, 2]
        path_image_finded = df_video_images.iloc[0, 3]
        width_camera = df_video_images.iloc[0, 4]
        height_camera = df_video_images.iloc[0, 5]

        many_detection_id = df_video_images.iloc[:, 6].tolist() if df_video_images.iloc[:, 6].tolist() != [None] else []
        many_is_active = df_video_images.iloc[:, 7].tolist() if df_video_images.iloc[:, 7].tolist() != [None] else []
        many_probability = df_video_images.iloc[:, 8].tolist() if df_video_images.iloc[:, 8].tolist() != [None] else []
        many_path_mask = df_video_images.iloc[:, 9].tolist() if df_video_images.iloc[:, 9].tolist() != [None] else []
        many_element_name = df_video_images.iloc[:, 10].tolist() if df_video_images.iloc[:, 10].tolist() != [None] else []
        many_category_name = df_video_images.iloc[:, 11].tolist() if df_video_images.iloc[:, 11].tolist() != [None] else []
        many_type_name = df_video_images.iloc[:, 12].tolist() if df_video_images.iloc[:, 12].tolist() != [None] else []

        if datetime.now() - datetime_ < timedelta(microseconds=50):
            continue

        column_config = {
            "_index": st.column_config.TextColumn("Параметр"),
            " ": st.column_config.TextColumn("Значение"),
        }

        gray_image = np.zeros((height_camera, width_camera), dtype=np.uint8)
        gray_image[:] = 128
        df_empty = pd.DataFrame(
            data=[
                ['ID обнаружения', ' '],
                ['Элемент', ' '],
                ['Активность', ' '],
                ['Вероятность', ' '], 
                ['Категория:', ' '], 
                ['Тип:', ' '],
            ], 
            columns=['Параметр', ' ']
        )
        df_empty.set_index('Параметр', inplace=True)
    
        if datetime.now() - datetime_ > timedelta(seconds=5):
            st_time.info(f'Сеанс завершен, последнее время: {datetime_}')
            st_subh_images.write(' ')
            st_info_original.write(' ')
            st_info_parsed.write(' ')
            st_info_finded.write(' ')
            st_image_original.write(' ')
            st_image_parsed.write(' ')
            st_image_finded.write(' ')
            st_subh_detections.write(' ')
            st_table_21.write(' ')
            st_mask_21.write(' ')
            st_table_22.write(' ')
            st_mask_22.write(' ')
            st_table_23.write(' ')
            st_mask_23.write(' ')
            st_table_24.write(' ')
            st_mask_24.write(' ')
            st_table_25.write(' ')
            st_mask_25.write(' ')   
            break

        st_time.info(datetime_)
        st_subh_images.subheader("Видеоизображения", divider="red")
        st_info_original.subheader('Оригинальное')
        st_info_parsed.subheader('Обработанное')
        st_info_finded.subheader('С обнаружениями')
        st_subh_detections.subheader("Обнаружения (Топ 5 по вероятности)", divider="orange")

        # original = cv2.imread(path_image_original) 
        # parsed = cv2.imread(path_image_parsed)
        original = MinioWork.get_object('video-images', path_image_original)
        parsed = MinioWork.get_object('video-images', path_image_parsed)

        # image_original = np.reshape(np.array(original), (height_camera, width_camera, 3))
        # image_parsed = np.reshape(np.array(parsed), (height_camera, width_camera, 3))
        image_original = np.reshape(
            np.array(cv2.imdecode(np.frombuffer(original, dtype=np.uint8), cv2.IMREAD_COLOR)), 
            (height_camera, width_camera, 3)
        )
        image_parsed = np.reshape(
            np.array(cv2.imdecode(np.frombuffer(parsed, dtype=np.uint8), cv2.IMREAD_COLOR)), 
            (height_camera, width_camera, 3)
        )

        st_image_original.image(image_original, width=500, channels="BGR")
        st_image_parsed.image(image_parsed, width=500, channels="BGR")

        if len(many_detection_id) < 5:
            st_table_25.dataframe(df_empty, width=300, column_config=column_config)
            st_mask_25.image(gray_image, width=300)   
            if len(many_detection_id) < 4:
                st_table_24.dataframe(df_empty, width=300, column_config=column_config)
                st_mask_24.image(gray_image, width=300)   
                if len(many_detection_id) < 3:
                    st_table_23.dataframe(df_empty, width=300, column_config=column_config)
                    st_mask_23.image(gray_image, width=300)   
                    if len(many_detection_id) < 2:
                        st_table_22.dataframe(df_empty, width=300, column_config=column_config)
                        st_mask_22.image(gray_image, width=300)   
                        if len(many_detection_id) < 1:
                            st_image_finded.image(gray_image, width=500)
                            st_table_21.dataframe(df_empty, width=300, column_config=column_config)
                            st_mask_21.image(gray_image, width=300)  

        if len(many_detection_id):
            # finded = cv2.imread(path_image_finded)
            # image_finded = np.reshape(np.array(finded), (height_camera, width_camera, 3))
            finded = MinioWork.get_object('video-images', path_image_finded)
            image_finded = np.reshape(
                np.array(cv2.imdecode(np.frombuffer(finded, dtype=np.uint8), cv2.IMREAD_COLOR)), 
                (height_camera, width_camera, 3)
            )
            st_image_finded.image(image_finded, width=500, channels="BGR")
            for i in range(len(many_category_name)):
                df_mask = pd.DataFrame(
                    data=[
                        ['ID обнаружения', str(many_detection_id[i])],
                        ['Элемент', str(many_element_name[i])],
                        ['Активность', str(many_is_active[i])],
                        ['Вероятность', str(round(many_probability[i], 2))], 
                        ['Категория:', str(many_category_name[i])], 
                        ['Тип:', str(many_type_name[i])],
                    ], 
                    columns=['Параметр', ' ']
                )
                df_mask.set_index('Параметр', inplace=True)
                if i == 0:
                    st_table_21.dataframe(df_mask, width=300, column_config=column_config)
                    # mask_21 = cv2.imread(many_path_mask[i]) 
                    # image_mask_21 = np.reshape(np.array(mask_21), (height_camera, width_camera, 3))
                    mask_21 = MinioWork.get_object('detections', many_path_mask[i])
                    image_mask_21 = np.reshape(
                        np.array(cv2.imdecode(np.frombuffer(mask_21, dtype=np.uint8), cv2.IMREAD_COLOR)), 
                        (height_camera, width_camera, 3)
                    )
                    st_mask_21.image(image_mask_21, width=300)
                elif i == 1:
                    st_table_22.dataframe(df_mask, width=300, column_config=column_config)
                    # mask_22 = cv2.imread(many_path_mask[i]) 
                    # image_mask_22 = np.reshape(np.array(mask_22), (height_camera, width_camera, 3))
                    mask_22 = MinioWork.get_object('detections', many_path_mask[i])
                    image_mask_22 = np.reshape(
                        np.array(cv2.imdecode(np.frombuffer(mask_22, dtype=np.uint8), cv2.IMREAD_COLOR)), 
                        (height_camera, width_camera, 3)
                    )
                    st_mask_22.image(image_mask_22, width=300)
                elif i == 2:
                    st_table_23.dataframe(df_mask, width=300, column_config=column_config)
                    # mask_23 = cv2.imread(many_path_mask[i]) 
                    # image_mask_23 = np.reshape(np.array(mask_23), (height_camera, width_camera, 3))
                    mask_23 = MinioWork.get_object('detections', many_path_mask[i])
                    image_mask_23 = np.reshape(
                        np.array(cv2.imdecode(np.frombuffer(mask_23, dtype=np.uint8), cv2.IMREAD_COLOR)), 
                        (height_camera, width_camera, 3)
                    )
                    st_mask_23.image(image_mask_23, width=300)
                elif i == 3:
                    st_table_24.dataframe(df_mask, width=300, column_config=column_config)
                    # mask_24 = cv2.imread(many_path_mask[i]) 
                    # image_mask_24 = np.reshape(np.array(mask_24), (height_camera, width_camera, 3))
                    mask_24 = MinioWork.get_object('detections', many_path_mask[i])
                    image_mask_24 = np.reshape(
                        np.array(cv2.imdecode(np.frombuffer(mask_24, dtype=np.uint8), cv2.IMREAD_COLOR)), 
                        (height_camera, width_camera, 3)
                    )
                    st_mask_24.image(image_mask_24, width=300)
                elif i == 4:
                    st_table_25.dataframe(df_mask, width=300, column_config=column_config)
                    # mask_25 = cv2.imread(many_path_mask[i]) 
                    # image_mask_25 = np.reshape(np.array(mask_25), (height_camera, width_camera, 3))
                    mask_25 = MinioWork.get_object('detections', many_path_mask[i])
                    image_mask_25 = np.reshape(
                        np.array(cv2.imdecode(np.frombuffer(mask_25, dtype=np.uint8), cv2.IMREAD_COLOR)), 
                        (height_camera, width_camera, 3)
                    )
                    st_mask_25.image(image_mask_25, width=300)
                    break

        _ = await asyncio.sleep(0.2)


async def monitoring_information(
    subh_1, df_1, div_1, subh_2, df_2, div_2, subh_3, df_3, div_3, subh_4, df_4, div_4, 
    subh_5, ch_5, div_5, subh_6, ch_6, div_6, subh_7, ch_7, div_7, subh_8, ch_8, div_8,
):
    i = 0
    while True:

        subh_1.subheader("Последние актуальные модели и методы анонимизации", divider="red")
        last_params = DatabaseWork.get_rows("""
            SELECT 
                anonymization_id,
                set_datetime,
                methods.method_id::text || ': ' || methods.name method,
                models.model_id::text || ': ' || models.name model
            FROM industrial.anonymizations anonymizations
            INNER JOIN industrial.methods methods 
                ON anonymizations.method_id=methods.method_id
            INNER JOIN industrial.models models 
                ON anonymizations.model_id=models.model_id
            ORDER BY set_datetime DESC
            LIMIT 10
        """)
        last_params_columns = ['ID анонимизации', 'Время установки', 'Метод', 'Модель']
        if len(last_params):
            last_params = pd.DataFrame(last_params, columns=last_params_columns)
        else:
            last_params = pd.DataFrame(columns=last_params_columns)
        df_1.dataframe(last_params)
        div_1.divider()

        subh_2.subheader("Сеансы за последний 1 час", divider="orange")
        sessions = DatabaseWork.get_rows("""
            SELECT 
                last_sessions.session_id,
                users.login,
                users.gender,
                users.last_name,
                users.first_name,
                last_sessions.start_datetime start_session,
                last_sessions.finish_datetime finish_session,
                CASE
                    WHEN last_sessions.finish_datetime IS NULL THEN current_timestamp - last_sessions.start_datetime
                    ELSE last_sessions.finish_datetime - last_sessions.start_datetime
                    END AS duration_session,
                last_sessions.width_camera,
                last_sessions.height_camera
            FROM industrial.users users
            INNER JOIN (
                SELECT 
                    session_id, user_id, start_datetime, finish_datetime, width_camera, height_camera 
                FROM industrial.sessions sessions
                WHERE start_datetime > current_timestamp - interval '1 hour' 
                ORDER BY session_id DESC
            ) last_sessions
                ON users.user_id=last_sessions.user_id
            ORDER BY start_session DESC
        """)
        sessions_columns = [
            'ID сеанса', 'Логин', 'Пол', 'Фамилия', 'Имя', 
            'Начало', 'Завершение', 'Длительность', 'Ширина камеры', 'Высота камеры'
        ]
        if len(sessions):
            sessions = pd.DataFrame(sessions, columns=sessions_columns)
        else:
            sessions = pd.DataFrame(columns=sessions_columns)
        df_2.dataframe(sessions)
        div_2.divider()

        subh_3.subheader("Уникальные обнаружения за последнюю 1 минуту", divider="green")
        detections = DatabaseWork.get_rows("""
            SELECT 
                users.login,
                detections.detection_id,
                elements.name element, 
                categories.name category,
                types.name type,
          		avg(detections.probability) avg_probability,
          		max(detections.datetime) detection_last_datetime,
          		min(detections.datetime) detection_first_datetime,
                sessions.start_datetime start_session,
          		max(video_images.number) last_number,
          		min(video_images.number) first_number,
                sessions.session_id,
                models.name model,
                methods.name method
            FROM (
                SELECT 
                    detection_id, probability, session_id, element_id, datetime 
                FROM industrial.detections 
                WHERE datetime > current_timestamp - interval '1 minute'
            ) detections
            INNER JOIN industrial.video_images video_images 
                ON detections.session_id=video_images.session_id 
                    AND detections.datetime=video_images.datetime
            INNER JOIN industrial.sessions sessions 
                ON detections.session_id=sessions.session_id
            INNER JOIN industrial.users users 
                ON sessions.user_id=users.user_id
            INNER JOIN industrial.anonymizations anonymizations 
                ON video_images.anonymization_id=anonymizations.anonymization_id
            INNER JOIN industrial.models models 
                ON anonymizations.model_id=models.model_id            
            INNER JOIN industrial.methods methods 
                ON anonymizations.method_id=methods.method_id
            INNER JOIN industrial.elements elements 
                ON detections.element_id=elements.element_id
            LEFT JOIN industrial.categories categories 
                ON elements.category_id=categories.category_id
            LEFT JOIN industrial.types types 
                ON elements.type_id=types.type_id
            GROUP BY 
            	users.login, detections.detection_id, elements.name, categories.name, types.name, 
            	sessions.start_datetime, sessions.session_id, models.name, methods.name
            ORDER BY detection_last_datetime DESC
        """)
        detections_columns = [
            'Логин', 'ID обнаружения', 'Элемент', 'Категория', 'Тип', 
            'Средняя вероятность', 'Время последнего обнаружения', 'Время первого обнаружения', 
            'Начало сеанса', 'Последний номер', 'Первый номер', 'ID сеанса', 'Модель', 'Метод'
        ]
        if len(detections):
            detections = pd.DataFrame(detections, columns=detections_columns)
        else:
            detections = pd.DataFrame(columns=detections_columns)
        df_3.dataframe(detections)
        div_3.divider()

        subh_4.subheader("Количество обнаружений и количество уникальных обнаружений каждую 1 секунду за последнюю 1 минуту", divider="blue")
        last_count_detections = DatabaseWork.get_rows("""
            SELECT 
                users.login,
                elements.name element_name,
                count(detection_id) count_detections,
                count(distinct detection_id) unique_detections
            FROM (
                SELECT 
                    detection_id, datetime, session_id, element_id 
                FROM industrial.detections 
                WHERE datetime > current_timestamp - interval '1 minute'
            ) detections
            INNER JOIN industrial.sessions sessions 
                ON detections.session_id=sessions.session_id
            INNER JOIN industrial.users users 
                ON sessions.user_id=users.user_id
            INNER JOIN industrial.elements elements 
                ON detections.element_id=elements.element_id
            LEFT JOIN industrial.categories categories 
                ON elements.category_id=categories.category_id
            LEFT JOIN industrial.types types 
                ON elements.type_id=types.type_id
            GROUP BY 
                users.login, elements.name
            ORDER BY count_detections DESC
        """)
        last_count_detections_columns = [
            'Логин', 'Название элемента', 'Количество обнаружений', 'Количество уникальных обнаружений'
        ]
        if len(last_count_detections):
            last_count_detections = pd.DataFrame(last_count_detections, columns=last_count_detections_columns)
        else:
            last_count_detections = pd.DataFrame(columns=last_count_detections_columns)
        df_4.dataframe(last_count_detections)
        div_4.divider()

        subh_5.subheader("Количество обнаружений по категориям каждые 10 секунд за последние 5 минут", divider="red")
        last_count_detections_category = DatabaseWork.get_rows(f"""
            SELECT 
                users.login,
                categories.name category,
                detections.interval_datetime datetime,
                count(detection_id) count_detections
            FROM (
                SELECT 
                    detection_id, session_id, element_id, 
                    to_timestamp(floor(extract(epoch from datetime) / 10) * 10 - 10800) interval_datetime 
                FROM industrial.detections 
                WHERE datetime > current_timestamp - interval '5 minute'
            ) detections
            INNER JOIN industrial.sessions sessions 
                ON detections.session_id=sessions.session_id
            INNER JOIN industrial.users users 
                ON sessions.user_id=users.user_id
            INNER JOIN industrial.elements elements 
                ON detections.element_id=elements.element_id
            LEFT JOIN industrial.categories categories 
                ON elements.category_id=categories.category_id
            GROUP BY 
                detections.interval_datetime, users.login, categories.name
            ORDER BY datetime ASC
        """)
        df_cat = pd.DataFrame(
            last_count_detections_category, 
            columns=["Логин", "Категория", "Время", "Количество обнаружений"]
        )
        tick_cat = int((df_cat['Количество обнаружений'].max()-df_cat['Количество обнаружений'].min()-1)/10+1) if len(df_cat) else 1
        fig_cat = px.line(
            df_cat, 
            x='Время', 
            y='Количество обнаружений', 
            color="Логин", 
            line_dash="Категория", 
            markers=True
        )
        fig_cat.update_layout(
            xaxis=dict(tickformat='%H:%M:%S'),
            yaxis=dict(tickmode='linear', dtick=tick_cat),
            yaxis_range=[0, None],
        )
        ch_5.plotly_chart(fig_cat, use_container_width=True, key=10000000000000+i)
        div_5.divider()

        subh_6.subheader("Количество уникальных обнаружений по категориям каждые 10 секунд за последние 5 минут", divider="orange")
        last_count_unique_detections_category = DatabaseWork.get_rows(f"""
            SELECT 
                users.login,
                categories.name category,
                detections.interval_datetime datetime,
                count(distinct detection_id) count_unique_detections
            FROM (
                SELECT 
                    detection_id, session_id, element_id, 
                    to_timestamp(floor(extract(epoch from datetime) / 10) * 10 - 10800) interval_datetime 
                FROM industrial.detections 
                WHERE datetime > current_timestamp - interval '5 minute'
            ) detections
            INNER JOIN industrial.sessions sessions 
                ON detections.session_id=sessions.session_id
            INNER JOIN industrial.users users 
                ON sessions.user_id=users.user_id
            INNER JOIN industrial.elements elements 
                ON detections.element_id=elements.element_id
            LEFT JOIN industrial.categories categories 
                ON elements.category_id=categories.category_id
            GROUP BY 
                detections.interval_datetime, users.login, categories.name
            ORDER BY datetime ASC
        """)
        df_cat_unique = pd.DataFrame(
            last_count_unique_detections_category, 
            columns=["Логин", "Категория", "Время", "Количество уникальных обнаружений"]
        )
        tick_cat_unique = int((df_cat_unique['Количество уникальных обнаружений'].max()-df_cat_unique["Количество уникальных обнаружений"].min()-1)/10+1) if len(df_cat_unique) else 1
        fig_cat_unique = px.line(
            df_cat_unique, 
            x='Время', 
            y='Количество уникальных обнаружений', 
            color="Логин", 
            line_dash="Категория", 
            markers=True
        )
        fig_cat_unique.update_layout(
            xaxis=dict(tickformat='%H:%M:%S'),
            yaxis=dict(tickmode='linear', dtick=tick_cat_unique),
            yaxis_range=[0, None],
        )
        ch_6.plotly_chart(fig_cat_unique, use_container_width=True, key=20000000000000+i)
        div_6.divider()

        subh_7.subheader("Количество обнаружений по типам каждые 10 секунд за последние 5 минут", divider="green")
        last_count_detections_type = DatabaseWork.get_rows(f"""
            SELECT 
                users.login,
                types.name type,
                detections.interval_datetime datetime,
                count(detection_id) count_detections
            FROM (
                SELECT 
                    detection_id, session_id, element_id, 
                    to_timestamp(floor(extract(epoch from datetime) / 10) * 10 - 10800) interval_datetime 
                FROM industrial.detections 
                WHERE datetime > current_timestamp - interval '5 minute'
            ) detections
            INNER JOIN industrial.sessions sessions 
                ON detections.session_id=sessions.session_id
            INNER JOIN industrial.users users 
                ON sessions.user_id=users.user_id
            INNER JOIN industrial.elements elements 
                ON detections.element_id=elements.element_id
            LEFT JOIN industrial.types types 
                ON elements.type_id=types.type_id
            GROUP BY 
                detections.interval_datetime, users.login, types.name
            ORDER BY datetime ASC
        """)
        df_typ = pd.DataFrame(
            last_count_detections_type, 
            columns=["Логин", "Тип", "Время", "Количество обнаружений"]
        )
        tick_typ = int((df_typ['Количество обнаружений'].max()-df_typ['Количество обнаружений'].min()-1)/10+1) if len(df_typ) else 1
        fig_typ = px.line(
            df_typ, 
            x='Время', 
            y='Количество обнаружений', 
            color="Логин", 
            line_dash="Тип", 
            markers=True
        )
        fig_typ.update_layout(
            xaxis=dict(tickformat='%H:%M:%S'),
            yaxis=dict(tickmode='linear', dtick=tick_typ),
            yaxis_range=[0, None],
        )
        ch_7.plotly_chart(fig_typ, use_container_width=True, key=30000000000000+i)
        div_7.divider()

        subh_8.subheader("Количество обнаружений по типам каждые 10 секунд за последние 5 минут", divider="blue")
        last_count_unique_detections_type = DatabaseWork.get_rows(f"""
            SELECT 
                users.login,
                types.name type,
                detections.interval_datetime datetime,
                count(distinct detection_id) count_detections
            FROM (
                SELECT 
                    detection_id, session_id, element_id, 
                    to_timestamp(floor(extract(epoch from datetime) / 10) * 10 - 10800) interval_datetime 
                FROM industrial.detections 
                WHERE datetime > current_timestamp - interval '5 minute'
            ) detections
            INNER JOIN industrial.sessions sessions 
                ON detections.session_id=sessions.session_id
            INNER JOIN industrial.users users 
                ON sessions.user_id=users.user_id
            INNER JOIN industrial.elements elements 
                ON detections.element_id=elements.element_id
            LEFT JOIN industrial.types types 
                ON elements.type_id=types.type_id
            GROUP BY 
                detections.interval_datetime, users.login, types.name
            ORDER BY datetime ASC
        """)
        df_typ_unique = pd.DataFrame(
            last_count_unique_detections_type, 
            columns=["Логин", "Тип", "Время", "Количество уникальных обнаружений"]
        )
        tick_typ_unique = int((df_typ_unique['Количество уникальных обнаружений'].max()-df_typ_unique['Количество уникальных обнаружений'].min()-1)/10+1) if len(df_typ_unique) else 1
        fig_typ_unique = px.line(
            df_typ_unique, 
            x='Время', 
            y='Количество уникальных обнаружений', 
            color="Логин", 
            line_dash="Тип", 
            markers=True
        )
        fig_typ_unique.update_layout(
            xaxis=dict(tickformat='%H:%M:%S'),
            yaxis=dict(tickmode='linear', dtick=tick_typ_unique),
            yaxis_range=[0, None],
        )
        ch_8.plotly_chart(fig_typ_unique, use_container_width=True, key=40000000000000+i)
        div_8.divider()

        i += 1
        _ = await asyncio.sleep(0.5)


def main():
    settings()

    choose_monitoring = st.pills(
        label="Выбор мониторинга", options=["Информация", "Видеопоток"], default="Информация"
    )

    is_show = st.toggle("Демонстрация")
    
    st.header("Информация", divider="rainbow")
    tab_1, tab_2 = st.tabs(["Таблицы", "Графики"])
    with tab_1:
        t_subh_1 = st.empty()
        t_df_1 = st.empty()
        t_div_1 = st.empty()
        t_subh_2 = st.empty()
        t_df_2 = st.empty()
        t_div_2 = st.empty()
        t_subh_3 = st.empty()
        t_df_3 = st.empty()
        t_div_3 = st.empty()
        t_subh_4 = st.empty()
        t_df_4 = st.empty()
        t_div_4 = st.empty()

    with tab_2:
        t_subh_5 = st.empty()
        t_df_5 = st.empty()
        t_div_5 = st.empty()
        t_subh_6 = st.empty()
        t_df_6 = st.empty()
        t_div_6 = st.empty()
        t_subh_7 = st.empty()
        t_df_7 = st.empty()
        t_div_7 = st.empty()
        t_subh_8 = st.empty()
        t_df_8 = st.empty()
        t_div_8 = st.empty()

    st.header("Видеопоток", divider="rainbow")
    i_choice_id = st.empty()
    i_submit_id = st.empty()
    i_info_id = st.empty()
    i_time = st.empty()
    i_subh_images = st.empty()
    i_col11, i_col12, i_col13 = st.columns(3)
    with i_col11:
        i_info_original = st.empty()
        i_image_original = st.empty()
    with i_col12:
        i_info_parsed = st.empty()
        i_image_parsed = st.empty()
    with i_col13:
        i_info_finded = st.empty()
        i_image_finded = st.empty()
    
    i_subh_detections = st.empty()
    i_col21, i_col22, i_col23, i_col24, i_col25 = st.columns(5)
    with i_col21:
        table_21 = st.empty()
        mask_21 = st.empty()
    with i_col22:
        table_22 = st.empty()
        mask_22 = st.empty()
    with i_col23:
        table_23 = st.empty()
        mask_23 = st.empty()
    with i_col24:
        table_24 = st.empty()
        mask_24 = st.empty()
    with i_col25:
        table_25 = st.empty()
        mask_25 = st.empty()

    if is_show:
        if choose_monitoring == "Информация":
            try:
                asyncio.run(
                    monitoring_information(
                        t_subh_1, t_df_1, t_div_1, 
                        t_subh_2, t_df_2, t_div_2,
                        t_subh_3, t_df_3, t_div_3, 
                        t_subh_4, t_df_4, t_div_4, 
                        t_subh_5, t_df_5, t_div_5,
                        t_subh_6, t_df_6, t_div_6, 
                        t_subh_7, t_df_7, t_div_7,
                        t_subh_8, t_df_8, t_div_8, 
                    )
                )
            except Exception as e:
                print(f'error...{type(e)}')
                raise
        elif choose_monitoring == "Видеопоток":
            get_sessions = DatabaseWork.get_rows("""
                SELECT 
                    sessions.session_id, users.login
                FROM industrial.users users
                INNER JOIN industrial.sessions sessions
                    ON sessions.user_id=users.user_id
                WHERE finish_datetime IS NULL
            """)
            get_sessions_names = [f"{row[0]}: {row[1]}" for row in get_sessions]
            session_ = i_choice_id.selectbox("Сеанс", options=get_sessions_names)
            if i_submit_id.toggle("Демонстрация видеоизображений и обнаружений") \
                    and session_ is not None:
                i_info_id.info(session_)
                try:
                    asyncio.run(
                        monitoring_video_stream(
                            session_.split(':')[0],
                            i_time,
                            i_subh_images, 
                            i_info_original, i_image_original, 
                            i_info_parsed, i_image_parsed, 
                            i_info_finded, i_image_finded,
                            i_subh_detections,
                            table_21, mask_21,
                            table_22, mask_22,
                            table_23, mask_23,
                            table_24, mask_24,
                            table_25, mask_25,
                        )
                    )
                except Exception as e:
                    print(f'error...{type(e)}')
                    raise


if __name__ == "__main__":
    main()
