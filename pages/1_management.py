from datetime import datetime, timedelta
from os import scandir

import pandas as pd
import streamlit as st 

from help_functions.database_work import DatabaseWork
from help_functions.minio_work import MinioWork
from help_functions.dialogs import (
    Anonymization,
    Categories, 
    Classes, 
    Elements, 
    Frequencies,
    Methods, 
    Models, 
    Monetaries,
    Segments,
    Types, 
    Users,
)


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
    st.title("Управление")
    st.divider()


def init():
    st.session_state.num = 0


def get_df():

    models = DatabaseWork.get_rows("SELECT * FROM industrial.models")
    models_columns = ['ID модели', 'Статус', 'Название', 'Описание', 
        'Время создания', 'Время изменения', 'Версия'
    ] 
    if len(models):
        models = pd.DataFrame(models, columns=models_columns)
    else:
        models = pd.DataFrame(columns=models_columns)

    methods = DatabaseWork.get_rows("SELECT * FROM industrial.methods")
    methods_columns = ['ID метода', 'Статус', 'Название', 'Описание', 
        'Время создания', 'Время изменения', 'Версия'
    ]
    if len(methods):
        methods = pd.DataFrame(methods, columns=methods_columns)
    else:
        methods = pd.DataFrame(columns=models_columns)

    users = DatabaseWork.get_rows("SELECT * FROM industrial.users")
    users_columns = ['ID пользователя', 'Статус', 'Пол', 'Логин', 
        'Фамилия', 'Имя', 'Отчество', 'Дата рождения', 
        'Дата добавления', 'Время создания', 'Время изменения', 'Версия'
    ]
    if len(users):
        users = pd.DataFrame(users, columns=users_columns)
    else:
        users = pd.DataFrame(columns=users_columns)

    categories = DatabaseWork.get_rows("SELECT * FROM industrial.categories")
    categories_columns = ['ID категории', 'Статус', 'Название', 'Описание',
        'Время создания', 'Время изменения', 'Версия'
    ]
    if len(categories):
        categories = pd.DataFrame(categories, columns=categories_columns)
    else:
        categories = pd.DataFrame(columns=categories_columns)

    types = DatabaseWork.get_rows("SELECT * FROM industrial.types")
    types_columns = ['ID типа', 'Статус', 'Название', 'Описание',
        'Время создания', 'Время изменения', 'Версия'
    ]
    if len(types):
        types = pd.DataFrame(types, columns=types_columns)
    else:
        types = pd.DataFrame(columns=types_columns)

    elements = DatabaseWork.get_rows("SELECT * FROM industrial.elements")
    elements = pd.DataFrame(elements)
    elements_columns = ['ID элемента', 'Статус', 'Приоритет', 'Название', 'Описание', 
        'Категория', 'Тип', 'Время создания', 'Время изменения', 'Версия'
    ]
    if len(elements):
        mini_categories = categories[categories['Статус'] == True][['ID категории', 'Название']]
        mini_types = types[types['Статус'] == True][['ID типа', 'Название']]

        if elements[5].dtype != 'object' and elements[6].dtype != 'object':
            elements = elements.merge(mini_categories, how='left', left_on=5, right_on='ID категории')
            elements['Категория'] = elements[["ID категории", "Название"]].apply(
                lambda row: ': '.join(row.values.astype(str)).replace('.0', '').replace('nan: nan', ''), 
                axis=1
            )
            elements = elements.merge(mini_types, how='left', left_on=6, right_on='ID типа')
            elements['Тип'] = elements[["ID типа", "Название_y"]].apply(
                lambda row: ': '.join(row.values.astype(str)).replace('.0', '').replace('nan: nan', ''), 
                axis=1
            )
            elements.columns = ['ID элемента', 'Статус', 'Приоритет', 'Название', 'Описание', 
                'el_cat', 'el_typ', 'Время создания', 'Время изменения', 'Версия', 
                'ca_id', 'ca_nam', 'Категория', 'ty_id', 'ty_nam', 'Тип'
            ]
            elements = elements[elements_columns]
        
        elif elements[5].dtype != 'object':
            elements = elements.merge(mini_categories, how='left', left_on=5, right_on='ID категории')
            elements['Категория'] = elements[["ID категории", "Название"]].apply(
                lambda row: ': '.join(row.values.astype(str)).replace('.0', '').replace('nan: nan', ''), 
                axis=1
            )
            elements.columns = ['ID элемента', 'Статус', 'Приоритет', 'Название', 'Описание', 
                'el_cat', 'el_typ', 'Время создания', 'Время изменения', 'Версия', 'ca_id', 'ca_nam', 'Категория'
            ]
            elements['Тип'] = ''
            elements = elements[elements_columns]
        
        elif elements[6].dtype != 'object':
            elements = elements.merge(mini_types, how='left', left_on=6, right_on='ID типа')
            elements['Тип'] = elements[["ID типа", "Название"]].apply(
                lambda row: ': '.join(row.values.astype(str)).replace('.0', '').replace('nan: nan', ''), 
                axis=1
            )
            elements.columns = ['ID элемента', 'Статус', 'Приоритет', 'Название', 'Описание', 
                'el_cat', 'el_typ', 'Время создания', 'Время изменения', 'Версия', 'ty_id', 'ty_nam', 'Тип'
            ]
            elements['Категория'] = ''
            elements = elements[elements_columns]
        
        else:
            elements['Категория'] = ''
            elements['Тип'] = ''
            elements = elements[elements_columns]
    else:
        elements = pd.DataFrame(columns=elements_columns)


    classes = DatabaseWork.get_rows("SELECT * FROM industrial.classes")
    classes = pd.DataFrame(classes)
    classes_columns = ['Номер класса', 'Элемент', 'Статус', 
        'Время создания', 'Время изменения', 'Версия'
    ]
    if len(classes):
        mini_elements = elements[elements['Статус'] == True][['ID элемента', 'Название']]
        
        classes = classes.merge(mini_elements, how='left', left_on=1, right_on='ID элемента')
        classes['Элемент'] = classes[["ID элемента", "Название"]].apply(
            lambda row: ': '.join(row.values.astype(str)).replace('.0', '').replace('nan: nan', ''), 
            axis=1
        )
        classes.columns = ['Номер класса', 'el_id', 'Статус', 'Время создания', 
            'Время изменения', 'Версия', 'ID элемента', 'Название', 'Элемент'
        ]
        classes = classes[classes_columns]
    else:
        classes = pd.DataFrame(columns=classes_columns)

    frequencies = DatabaseWork.get_rows("SELECT * FROM industrial.frequencies")
    frequencies_columns = ['Номер', 'Статус', 'Максимальное значение', 
        'Время создания', 'Время изменения', 'Версия'
    ]
    if len(frequencies):
        frequencies = pd.DataFrame(frequencies, columns=frequencies_columns)
    else:
        frequencies = pd.DataFrame(columns=frequencies_columns)    

    monetaries = DatabaseWork.get_rows("SELECT * FROM industrial.monetaries")
    monetaries_columns = ['Номер', 'Статус', 'Максимальное значение', 
        'Время создания', 'Время изменения', 'Версия'
    ]
    if len(monetaries):
        monetaries = pd.DataFrame(monetaries, columns=monetaries_columns)
    else:
        monetaries = pd.DataFrame(columns=monetaries_columns)    

    segments = DatabaseWork.get_rows("SELECT * FROM industrial.segments")
    segments_columns = ['Код', 'Статус', 'Риск', 'Частота', 'Надежность',
        'Время создания', 'Время изменения', 'Версия'
    ]
    if len(segments):
        segments = pd.DataFrame(segments, columns=segments_columns)
    else:
        segments = pd.DataFrame(columns=segments_columns)   

    return models, methods, users, classes, categories, types, elements, frequencies, monetaries, segments


def convert_date(timestamp):
    #d = datetime.utcfromtimestamp(timestamp)
    #formated_date = d.strftime('%d %b %Y')
    #dt_with_tz = datetime.fromisoformat(timestamp)
    # return formated_date
    dt_with_plus_3 = timestamp + timedelta(hours=3)
    return dt_with_plus_3.replace(tzinfo=None)


def format_bytes(size):
    power = 2**10
    n = 0
    power_labels = {0 : 'bytes', 1: 'Kb', 2: 'Mb', 3: 'Gb', 4: 'Tb'}
    while size > power:
        size /= power
        n += 1
    return f'{round(size)} {power_labels[n]}'



def get_df_files(current_params):
    columns = ['Название', 'Активный метод', 'Последнее изменение', 'Размер']
    if current_params is None:
        return pd.DataFrame(columns=columns) 
    lst = []
    active_model, active_method = DatabaseWork.get_column(f"""
        SELECT name 
        FROM industrial.models 
        WHERE status = True 
            AND model_id = {current_params[0]}
        UNION ALL
        SELECT name 
        FROM industrial.methods 
        WHERE status = True 
            AND method_id = {current_params[1]}        
    """)
    objects = MinioWork.get_objects('models')
    for obj in objects:
        if not obj.is_dir:
            name = obj.object_name
            is_active = active_method if name == active_model else ""
            last_modified = convert_date(obj.last_modified)
            size = format_bytes(obj.size)
            lst.append((name, is_active, last_modified, size))
    df = pd.DataFrame(lst)
    df.columns = columns if len(df) else []
    actual_models = DatabaseWork.get_column("""
        SELECT name 
        FROM industrial.models 
        WHERE status = True
    """)
    df['Активность'] = df['Название'].isin(actual_models)
    df = df[df['Активность'] == True][columns] 
    df = df.sort_values(['Активный метод', 'Последнее изменение'], ascending=False).reset_index(drop=True)
    return df 


def main():
    settings()
    init()

    st.header("Анонимизация", divider="rainbow")

    current_params = DatabaseWork.get_row("""
        SELECT 
            model_id, method_id
        FROM industrial.anonymizations 
        ORDER BY set_datetime DESC
        LIMIT 1
    """)
    df = get_df_files(current_params)

    st.subheader("Загруженные доступные модели с активным методом")
    st.dataframe(df)
    anonymization_col1, _ = st.columns(2)
    with anonymization_col1:
        if st.button("Заменить анонимизацию"):
            is_method = DatabaseWork.get_check("""
                SELECT method_id 
                FROM industrial.methods 
                WHERE status = True
            """)
            is_model = DatabaseWork.get_check("""
                SELECT model_id 
                FROM industrial.models 
                WHERE status = True
            """)
            if is_method and is_model:
                Anonymization.vote_change(current_params)
            else:
                st.warning("Отсутствуют методы или модели")

    st.divider()
    st.header("Данные", divider="rainbow")

    models, methods, users, classes, categories, types, elements, frequencies, monetaries, segments = get_df()

    tab_1, tab_2, tab_3 = st.tabs([
        "Модели + Методы + Пользователи", 
        "Классы + Элементы + Категории + Типы", 
        "Параметры FM-анализа"
    ])

    with tab_1:

        st.divider()
        st.subheader("Модели", divider="red")
        st.dataframe(models.sort_values(['Статус', 'Время изменения'], ascending=False).reset_index(drop=True))
        models_col1, models_col2 = st.columns(2)
        with models_col1:
            if st.button("Изменить модель"):
                Models.vote_edit(save_folder='models/')
        with models_col2:
            if st.button("Добавить модель"):
                Models.vote_add(save_folder='models/')

        st.divider()
        st.subheader("Методы", divider="orange")
        st.dataframe(methods.sort_values(['Статус', 'Время изменения'], ascending=False).reset_index(drop=True))
        methods_col1, _ = st.columns(2)
        with methods_col1:
            if st.button("Изменить метод"):
                Methods.vote_edit()

        st.divider()
        st.subheader("Пользователи", divider="green")
        st.dataframe(users.sort_values(['Статус', 'Время изменения'], ascending=False).reset_index(drop=True))
        users_col1, users_col2 = st.columns(2)
        with users_col1:
            if st.button("Изменить пользователя"):
                Users.vote_edit()
        with users_col2:
            if st.button("Добавить пользователя"):
                Users.vote_add()

    with tab_2:

        st.divider()
        st.subheader("Классы", divider="blue")
        st.dataframe(classes.sort_values(['Статус', 'Время изменения'], ascending=False).reset_index(drop=True))
        classes_col1, classes_col2 = st.columns(2)
        with classes_col1:
            if st.button("Изменить ассоциацию класса"):
                Classes.vote_edit()
        with classes_col2:
            if st.button("Добавить ассоциацию класса"):
                Classes.vote_add()

        st.divider()
        st.subheader("Элементы", divider="violet")
        st.dataframe(elements.sort_values(['Статус', 'Время изменения'], ascending=False).reset_index(drop=True))
        elements_col1, elements_col2 = st.columns(2)
        with elements_col1:
            if st.button("Изменить элемент"):
                Elements.vote_edit()
        with elements_col2:
            if st.button("Добавить элемент"):
                Elements.vote_add()

        st.divider()
        st.subheader("Категории", divider="gray")
        st.dataframe(categories.sort_values(['Статус', 'Время изменения'], ascending=False).reset_index(drop=True))
        categories_col1, categories_col2 = st.columns(2)
        with categories_col1:
            if st.button("Изменить категорию"):
                Categories.vote_edit()
        with categories_col2:
            if st.button("Добавить категорию"):
                Categories.vote_add()

        st.divider()
        st.subheader("Типы", divider="red")
        st.dataframe(types.sort_values(['Статус', 'Время изменения'], ascending=False).reset_index(drop=True))
        types_col1, types_col2 = st.columns(2)
        with types_col1:
            if st.button("Изменить тип"):
                Types.vote_edit()
        with types_col2:
            if st.button("Добавить тип"):
                Types.vote_add()

    with tab_3:

        st.divider()
        st.subheader("Сегменты", divider="orange")
        st.dataframe(segments.sort_values(['Статус', 'Риск', 'Код'], ascending=False).reset_index(drop=True))
        segments_col1, segments_col2 = st.columns(2)
        with segments_col1:
            if st.button("Изменить сегмент"):
                Segments.vote_edit()
        with segments_col2:
            if st.button("Добавить сегмент"):
                Segments.vote_add()

        st.divider()
        st.subheader("Частоты", divider="green")
        st.dataframe(frequencies.sort_values(['Статус', 'Номер'], ascending=False).reset_index(drop=True))
        frequencies_col1, frequencies_col2 = st.columns(2)
        with frequencies_col1:
            if st.button("Изменить частоту"):
                Frequencies.vote_edit()
        with frequencies_col2:
            if st.button("Добавить частоту"):
                Frequencies.vote_add()

        st.divider()
        st.subheader("Надежности", divider="blue")
        st.dataframe(monetaries.sort_values(['Статус', 'Номер'], ascending=False).reset_index(drop=True))
        monetaries_col1, monetaries_col2 = st.columns(2)
        with monetaries_col1:
            if st.button("Изменить надежность"):
                Monetaries.vote_edit()
        with monetaries_col2:
            if st.button("Добавить надежность"):
                Monetaries.vote_add()


if __name__ == "__main__":
    main()
