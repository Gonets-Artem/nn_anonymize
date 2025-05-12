from datetime import date
from pathlib import Path

import streamlit as st 

from help_functions.database_work import DatabaseWork
from help_functions.minio_work import MinioWork


class Anonymization:

    @st.dialog("Замена анонимизации")
    def vote_change(cur_params):
        if cur_params is not None:
            cur_model = DatabaseWork.get_rows(f"""
                SELECT 
                    model_id, name 
                FROM industrial.models 
                WHERE status = True 
                    AND model_id = {cur_params[0]} 
                ORDER BY name
            """)
            new_models = DatabaseWork.get_rows(f"""
                SELECT 
                    model_id, name 
                FROM industrial.models 
                WHERE status = True 
                    AND model_id != {cur_params[0]} 
                ORDER BY name
            """)
            models = cur_model + new_models
        else:
            models = DatabaseWork.get_rows("""
                SELECT 
                    model_id, name 
                FROM industrial.models 
                WHERE status = True 
                ORDER BY name
        """)
        methods = DatabaseWork.get_rows("""
            SELECT 
                method_id, name 
            FROM industrial.methods 
            WHERE status = True 
            ORDER BY name
        """)
        model = st.selectbox("Модель", [el[1] for el in models])
        if cur_params is not None:
            default_method = [el[1] for el in methods if el[0] == cur_params[1]][0]
        else:
            default_method = methods[0][1]
        method = st.pills("Метод", options=[el[1] for el in methods], default=default_method)
        active_sessions = DatabaseWork.get_field("""
            SELECT count(*) 
            FROM industrial.sessions 
            WHERE finish_datetime IS NULL
        """)
        if active_sessions:
            st.info('Отсутствуют активные сеансы')
        if st.button('Подтвердить замену'):
            if model != "":
                method_id = [el[0] for el in methods if el[1] == method][0]
                model_id = [el[0] for el in models if el[1] == model][0]
                query = f"""
                    INSERT INTO industrial.anonymizations 
                    (set_datetime, method_id, model_id) VALUES 
                    (CURRENT_TIMESTAMP, {method_id}, {model_id})
                """
                DatabaseWork.row_add_edit(query)
                st.success('Параметры успешно сохранены!')
                st.rerun()
            else:
                st.warning('Нет моделей')


class Categories:

    @st.dialog("Добавление категории")
    def vote_add():
        status = st.segmented_control("Статус", options=["True", "False"], default="True")
        name = st.text_input("Название", max_chars=32)
        definition = st.text_input("Описание (необязательно)", max_chars=128)

        if st.button('Подтвердить добавление'):
            if name != "":
                query_categories = f"""
                    INSERT INTO industrial.categories 
                    (status, name, definition, created_on, modified_on, version) VALUES 
                    ({status}, '{name}', '{definition}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)
                """
                DatabaseWork.row_add_edit(query_categories)
                st.success("Новая категория добавлена")
                st.rerun()
            else:
                st.warning('Не заполнено "Название"')


    @st.dialog("Изменение категории")
    def vote_edit():
        choice = st.empty()
        confirm = st.empty()

        if st.session_state.num == 0:
            category_ids = DatabaseWork.get_rows("""
                SELECT 
                    category_id, status, name 
                FROM industrial.categories 
                ORDER BY status DESC, category_id DESC
            """)
            category_ids = [f'{row[1]}, {row[0]}: {row[2]}' for row in category_ids]
            category_ = choice.selectbox("Категория", options=category_ids)
            confirm_button = confirm.button('Подтвердить ID')
            if confirm_button:
                if category_ is not None:
                    category_id = category_.split(', ')[1].split(':')[0]
                    st.session_state.num = 1
                    st.session_state.row = DatabaseWork.get_row(f"""
                        SELECT * 
                        FROM industrial.categories 
                        WHERE category_id = {category_id}
                    """)
                    st.session_state.input_id = category_id
                else:
                    st.warning('Нет категорий')

        if st.session_state.num == 1:
            choice.info(f"ID категории = {st.session_state.input_id}")
            confirm.write('')

            status = st.segmented_control("Статус", options=["True", "False"], default=f"{st.session_state.row[1]}")
            name = st.text_input("Название", value=st.session_state.row[2], max_chars=32)
            definition = st.text_input("Описание (необязательно)", value=st.session_state.row[3], max_chars=128)
            
            if st.button(f'Подтвердить изменение'):
                if name != "":
                    query = f"""
                        UPDATE industrial.categories SET 
                            status = {status}, 
                            name = '{name}', 
                            definition = '{definition}', 
                            modified_on = CURRENT_TIMESTAMP, 
                            version = {st.session_state.row[6] + 1} 
                        WHERE category_id = {st.session_state.input_id}
                    """
                    DatabaseWork.row_add_edit(query)
                    st.success("Категория изменена")
                    st.rerun()
                else:
                    st.warning('Не заполнено "Название"')


class Classes:

    @st.dialog("Добавление ассоциации класса")
    def vote_add():
        get_elements = DatabaseWork.get_rows("""
            SELECT element_id, name FROM industrial.elements WHERE status = True
        """)
        get_elements_names = [f"{row[0]}: {row[1]}" for row in get_elements]
        class_number = st.number_input("Номер класса", min_value=0)
        element_ = st.selectbox("Элемент", options=get_elements_names)
        status = st.segmented_control("Статус", options=["True", "False"], default="True")

        if st.button('Подтвердить добавление'):
            
            if element_ is not None:
                query = f"""
                    INSERT INTO industrial.classes 
                    (class_number, element_id, status, created_on, modified_on, version) VALUES 
                    ({class_number}, '{element_.split(':')[0]}', '{status}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)
                """
                try:
                    DatabaseWork.row_add_edit(query)
                    st.success("Новая ассоциация класса добавлена")
                except:
                    st.warning('Данная комбинация "Номер класса" и "Элемент" уже существует')
                st.rerun()
            else:
                st.warning('Выберите "ID элемента", если существует')


    @st.dialog("Изменение ассоциации класса")
    def vote_edit():
        choice = st.empty()
        confirm = st.empty()

        if st.session_state.num == 0:
            rows = DatabaseWork.get_rows("""
                SELECT 
                    class_number, element_id, status 
                FROM industrial.classes 
                WHERE element_id IN (
                    SELECT element_id 
                    FROM industrial.elements 
                    WHERE status = True
                )
                ORDER BY status DESC, class_number DESC, element_id DESC
            """)
            rows_names = [f"{row[2]}, Номер класса = {row[0]}, ID элемента = {row[1]}" for row in rows]
            class_associations = choice.selectbox("Ассоциация класса", options=rows_names)
            confirm_button = confirm.button('Подтвердить ассоциацию')
            if confirm_button:
                if class_associations is not None:
                    st.session_state.num = 1
                    class_number, element_id, _ = rows[rows_names.index(class_associations)]
                    st.session_state.row = DatabaseWork.get_row(f"""
                        SELECT * 
                        FROM industrial.classes 
                        WHERE class_number = {class_number} 
                            AND element_id = {element_id}
                    """)
                    st.session_state.class_number = class_number
                    st.session_state.input_id = element_id
                else:
                    st.warning('Нет ассоциации класса')

        if st.session_state.num == 1:
            choice.info(f"Номер класса = {st.session_state.class_number}, ID элемента = {st.session_state.input_id}")
            confirm.write('')

            status = st.segmented_control("Статус", options=["True", "False"], default=f"{st.session_state.row[2]}")
            
            if st.button('Подтвердить изменение'):
                query = f"""
                    UPDATE industrial.classes 
                    SET 
                        status = {status},
                        modified_on = CURRENT_TIMESTAMP, 
                        version = {st.session_state.row[5] + 1} 
                    WHERE class_number = {st.session_state.class_number} 
                        AND element_id = {st.session_state.input_id}
                """
                DatabaseWork.row_add_edit(query)
                st.success("Ассоциация класса изменена")
                st.rerun()


class Elements:

    @st.dialog("Добавление элемента")
    def vote_add():
        get_categories = DatabaseWork.get_rows("""
            SELECT 
                category_id, name 
            FROM industrial.categories 
            WHERE status = True 
            ORDER BY category_id DESC
        """)
        get_categories_names = [""] + [f"{row[0]}: {row[1]}" for row in get_categories]
        get_types = DatabaseWork.get_rows("""
            SELECT 
                type_id, name 
            FROM industrial.types 
            WHERE status = True 
            ORDER BY type_id DESC
        """)
        get_types_names = [""] + [f"{row[0]}: {row[1]}" for row in get_types]

        status = st.segmented_control("Статус", options=["True", "False"], default="True")
        priority = st.slider("Приоритет", min_value=1, max_value=10, value=1)
        name = st.text_input("Название", max_chars=64)
        definition = st.text_input("Описание (необязательно)", max_chars=128)
        category_ = st.selectbox("Категория (необязательно)", options=get_categories_names)
        type_ = st.selectbox("Тип (необязательно)", options=get_types_names)

        if st.button('Подтвердить добавление'):
            if name != "":
                category_id_num = "null" if category_ == "" else category_.split(':')[0]
                type_id_num = "null" if type_ == "" else type_.split(':')[0]
                query = f"""
                    INSERT INTO industrial.elements 
                    (status, priority, name, definition, category_id, type_id, 
                        created_on, modified_on, version) VALUES 
                    ({status}, {priority}, '{name}', '{definition}', {category_id_num}, {type_id_num}, 
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)
                """
                DatabaseWork.row_add_edit(query)
                st.success("Новый элемент добавлен")
                st.rerun()
            else:
                st.warning('Не заполнено "Название"')


    @st.dialog("Изменение элемента")
    def vote_edit():
        choice = st.empty()
        confirm = st.empty()

        if st.session_state.num == 0:
            element_ids = DatabaseWork.get_rows("""
                SELECT 
                    element_id, status, name 
                FROM industrial.elements 
                ORDER BY status DESC, element_id DESC
            """)
            element_ids = [f'{row[1]}, {row[0]}: {row[2]}' for row in element_ids]
            element_ = choice.selectbox("Элемент", options=element_ids)
            confirm_button = confirm.button('Подтвердить ID')
            if confirm_button:
                if element_ is not None:
                    element_id = element_.split(', ')[1].split(':')[0]
                    st.session_state.num = 1
                    st.session_state.row = DatabaseWork.get_row(f"""
                        SELECT * 
                        FROM industrial.elements 
                        WHERE element_id = {element_id}
                    """)
                    st.session_state.input_id = element_id
                else:
                    st.warning('Нет элементов')

        if st.session_state.num == 1:
            choice.info(f"ID элемента = {st.session_state.input_id}")
            confirm = st.write('')

            cur_category_id = st.session_state.row[5] if st.session_state.row[5] is not None else ""
            cur_type_id = st.session_state.row[6] if st.session_state.row[6] is not None else ""
            if cur_category_id == "":
                all_categories = DatabaseWork.get_rows("""
                    SELECT 
                        category_id, name 
                    FROM industrial.categories 
                    WHERE status = True 
                    ORDER BY category_id DESC
                """)
                categories_names = [""] + [f"{row[0]}: {row[1]}" for row in all_categories]
            else:
                category_name = DatabaseWork.get_field(f"""
                    SELECT name 
                    FROM industrial.categories 
                    WHERE category_id = {cur_category_id} 
                    ORDER BY category_id DESC
                """)
                other_categories = DatabaseWork.get_rows(f"""
                    SELECT 
                        category_id, name 
                    FROM industrial.categories 
                    WHERE status = True 
                        AND category_id != {cur_category_id} 
                    ORDER BY category_id DESC
                """)
                categories_names = [f"{cur_category_id}: {category_name}", ""] + [f"{row[0]}: {row[1]}" for row in other_categories]
            if cur_type_id == "":
                all_types = DatabaseWork.get_rows("""
                    SELECT 
                        type_id, name 
                    FROM industrial.types 
                    WHERE status = True 
                    ORDER BY type_id DESC
                """)
                types_names = [""] + [f"{row[0]}: {row[1]}" for row in all_types]
            else:
                type_name = DatabaseWork.get_field(f"""
                    SELECT name 
                    FROM industrial.types 
                    WHERE type_id = {cur_type_id} 
                    ORDER BY type_id DESC
                """)
                other_types = DatabaseWork.get_rows(f"""
                    SELECT 
                        type_id, name 
                    FROM industrial.types 
                    WHERE status = True 
                        AND type_id != {cur_type_id} 
                    ORDER BY type_id DESC
                """)
                types_names = [f"{cur_type_id}: {type_name}", ""] + [f"{row[0]}: {row[1]}" for row in other_types]

            status = st.segmented_control("Статус", options=["True", "False"], default=f"{st.session_state.row[1]}")
            priority = st.slider("Приоритет", min_value=1, max_value=10, value=st.session_state.row[2])
            name = st.text_input("Название", value=st.session_state.row[3], max_chars=64)
            definition = st.text_input("Описание (необязательно)", value=st.session_state.row[4], max_chars=128)
            category_ = st.selectbox("Категория (необязательно)", options=categories_names)
            type_ = st.selectbox("Тип (необязательно)", options=types_names)
            
            if st.button(f'Подтвердить изменение'):
                if name != "":
                    category_id_num = "null" if category_ is None or category_ == "" else category_.split(':')[0]
                    type_id_num = "null" if type_ is None or type_ == "" else type_.split(':')[0]
                    query = f"""
                        UPDATE industrial.elements 
                        SET 
                            status = {status}, 
                            priority = {priority}, 
                            name = '{name}', 
                            definition = '{definition}', 
                            category_id = {category_id_num}, 
                            type_id = {type_id_num}, 
                            modified_on = CURRENT_TIMESTAMP, 
                            version = {st.session_state.row[9] + 1} 
                        WHERE element_id = {st.session_state.input_id}
                    """
                    DatabaseWork.row_add_edit(query)
                    st.session_state.num = 0
                    st.success("Элемент изменен")
                    st.rerun()
                else:
                    st.warning('Не заполнено "Название"')


class Frequencies:

    @st.dialog("Добавление частоты")
    def vote_add():
        number = st.number_input("Номер", min_value=1)
        status = st.segmented_control("Статус", options=["True", "False"], default="True")
        max_value = st.number_input("Максимальное значение", min_value=-1, max_value=2147483647)

        if st.button('Подтвердить добавление'):
            query = f"""
                INSERT INTO industrial.frequencies 
                (number, status, max_value, created_on, modified_on, version) VALUES 
                ({number}, {status}, {max_value}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)
            """
            try:
                DatabaseWork.row_add_edit(query)
                st.success("Новая частота добавлена")
            except:
                st.warning('Данный номер частоты уже существует')
            st.rerun()


    @st.dialog("Изменение частоты")
    def vote_edit():
        choice = st.empty()
        confirm = st.empty()

        if st.session_state.num == 0:
            numbers = DatabaseWork.get_rows("""
                SELECT 
                    number, status 
                FROM industrial.frequencies 
                ORDER BY status DESC, number DESC
            """)
            numbers = [f'{row[1]}, {row[0]}' for row in numbers]
            numbers_ = choice.selectbox("Частота", options=numbers)
            confirm_button = confirm.button('Подтвердить номер')
            if confirm_button:
                if numbers_ is not None:
                    num = numbers_.split(', ')[1]
                    st.session_state.num = 1
                    st.session_state.row = DatabaseWork.get_row(f"""
                        SELECT * 
                        FROM industrial.frequencies 
                        WHERE number = {num}
                    """)
                    st.session_state.input_id = num
                else:
                    st.warning('Нет частот')

        if st.session_state.num == 1:
            choice.info(f"Номер частоты = {st.session_state.input_id}")
            confirm.write('')
            status = st.segmented_control("Статус", options=["True", "False"], default=f"{st.session_state.row[1]}")
            max_value = st.number_input("Максимальное значение", min_value=0, max_value=2147483647, value=st.session_state.row[2])
            
            if st.button(f'Подтвердить изменение'):
                query = f"""
                    UPDATE industrial.frequencies SET 
                        status = {status}, 
                        max_value = {max_value},
                        modified_on = CURRENT_TIMESTAMP, 
                        version = {st.session_state.row[5] + 1} 
                    WHERE number = {st.session_state.input_id}
                """
                DatabaseWork.row_add_edit(query)
                st.success("Частота изменена")
                st.rerun()


class Methods:

    @st.dialog("Изменение метода")
    def vote_edit():
        choice = st.empty()
        confirm = st.empty()

        if st.session_state.num == 0:
            method_ids = DatabaseWork.get_rows("""
                SELECT 
                    method_id, status, name 
                FROM industrial.methods 
                ORDER BY status DESC, method_id DESC
            """)
            method_ids = [f'{row[1]}, {row[0]}: {row[2]}' for row in method_ids]
            method_ = choice.selectbox("Метод", options=method_ids)
            confirm_button = confirm.button('Подтвердить ID')
            if confirm_button:
                if method_ is not None:
                    method_id = method_.split(', ')[1].split(':')[0]
                    st.session_state.num = 1
                    st.session_state.row = DatabaseWork.get_row(f"""
                        SELECT * 
                        FROM industrial.methods 
                        WHERE method_id = {method_id}
                    """)
                    st.session_state.input_id = method_id
                else:
                    st.warning('Нет методов')

        if st.session_state.num == 1:
            choice.info(f"ID метода = {st.session_state.input_id}")
            confirm.write('')

            check_active_method = DatabaseWork.get_field(f"""
                SELECT count(*) 
                FROM industrial.methods
                WHERE method_id = {st.session_state.input_id} 
                    AND method_id IN (
                        SELECT method_id 
                        FROM industrial.anonymizations 
                        ORDER BY set_datetime DESC 
                        LIMIT 1
                    )
            """)
            if check_active_method:
                status = st.segmented_control("Статус", options=["True", "True"], default=f"{st.session_state.row[1]}")
            else:
                status = st.segmented_control("Статус", options=["True", "False"], default=f"{st.session_state.row[1]}")
            name = st.text_input("Название", value=st.session_state.row[2], max_chars=32)
            definition = st.text_input("Описание (необязательно)", value=st.session_state.row[3], max_chars=128)
            
            if st.button(f'Подтвердить изменение'):
                other_names = DatabaseWork.get_column(f"""
                    SELECT name 
                    FROM industrial.methods 
                    WHERE status = True 
                        AND method_id != {st.session_state.input_id}
                """)   
                if name != "" and name not in other_names:
                    query = f"""
                        UPDATE industrial.methods 
                        SET 
                            status = {status}, 
                            name = '{name}', 
                            definition = '{definition}', 
                            modified_on = CURRENT_TIMESTAMP, 
                            version = {st.session_state.row[6] + 1} 
                        WHERE method_id = {st.session_state.input_id}
                    """
                    DatabaseWork.row_add_edit(query)
                    st.success("Метод изменен")
                    st.rerun()
                elif name != "":
                    st.warning('Существует актуальный метод с таким же "Название"')
                else:
                    st.warning('Не заполнено "Название"')


class Models:

    @st.dialog("Добавление модели")
    def vote_add(save_folder):
        status = st.segmented_control("Статус", options=["True", "False"], default="True")
        file_model = st.file_uploader(label="Модель", type=["pt"])
        definition = st.text_input("Описание (необязательно)", max_chars=128)

        if st.button('Подтвердить добавление'):
            if file_model is not None:
                models_names = DatabaseWork.get_column("""
                    SELECT name 
                    FROM industrial.models
                """)
                if status == "True" and file_model.name in models_names: 
                    st.warning(f'Актуальная модель "{file_model.name}" была в базе данных, выберите другой файл')
                else:
                    # save_path = Path(save_folder, file_model.name)
                    # with open(save_path, mode='wb') as w:
                    #     w.write(file_model.getvalue())
                    MinioWork.save_io(
                        bytes=file_model.getvalue(),
                        bucket_name='models',
                        object_name=file_model.name
                    )
                    
                    if MinioWork.find_object_name('models', file_model.name):
                        st.success(f'Файл {file_model.name} успешно сохранен!')
                        query = f"""
                            INSERT INTO industrial.models 
                            (status, name, definition, created_on, modified_on, version) VALUES 
                            ({status}, '{file_model.name}', '{definition}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)
                        """
                        DatabaseWork.row_add_edit(query)
                        st.success(f'Новая модель добавлена')
                        st.rerun()
                    else:
                        st.error(f'Файл {file_model.name} не сохранился!')
            else:
                st.warning('Не заполнено "Модель"')

    @st.dialog("Изменение модели")
    def vote_edit(save_folder):
        choice = st.empty()
        confirm = st.empty()

        if st.session_state.num == 0:
            model_ids = DatabaseWork.get_rows("""
                SELECT 
                    model_id, status, name 
                FROM industrial.models 
                ORDER BY status DESC, model_id DESC
            """)
            model_ids = [f'{row[1]}, {row[0]}: {row[2]}' for row in model_ids]
            model_ = choice.selectbox("Модель", options=model_ids)
            confirm_button = confirm.button('Подтвердить ID')
            if confirm_button:
                if model_ is not None:
                    model_id = model_.split(', ')[1].split(':')[0]
                    st.session_state.num = 1
                    st.session_state.row = DatabaseWork.get_row(f"""
                        SELECT * 
                        FROM industrial.models 
                        WHERE model_id = {model_id}
                    """)
                    st.session_state.input_id = model_id
                else:
                    st.warning('Нет моделей')

        if st.session_state.num == 1:
            choice.info(f"ID модели = {st.session_state.input_id}")
            confirm.write('')

            check_active_model = DatabaseWork.get_field(f"""
                SELECT count(*) 
                FROM industrial.models 
                WHERE model_id = {st.session_state.input_id} 
                    AND model_id IN (
                        SELECT model_id 
                        FROM industrial.anonymizations 
                        ORDER BY set_datetime DESC 
                        LIMIT 1
                    )
            """)
            if check_active_model:
                status = st.segmented_control("Статус", options=["True", "False"], default=f"{st.session_state.row[1]}")
            else:
                status = st.segmented_control("Статус", options=["True", "False"], default=f"{st.session_state.row[1]}")
            name = st.text_input("Название", value=st.session_state.row[2][:-3], max_chars=32)
            definition = st.text_input("Описание (необязательно)", value=st.session_state.row[3], max_chars=128)
            
            if st.button(f'Подтвердить изменение'):
                other_names = DatabaseWork.get_column(f"""
                    SELECT name 
                    FROM industrial.models 
                    WHERE status = True 
                        AND model_id != {st.session_state.input_id}
                """)   
                if name != "" and name not in other_names:
                    # old_name = Path(save_folder, st.session_state.row[2])
                    # new_name = old_name.with_name(f'{name}.pt')
                    # old_name.rename(new_name)

                    old_name = st.session_state.row[2]
                    new_name = f'{name}.pt'
                    if old_name != new_name:
                        MinioWork.rename_object_name(
                            bucket_name='models', 
                            old_name=old_name,
                            new_name=new_name
                        )
                        if MinioWork.find_object_name('models', new_name):
                            st.success(f"Файл {st.session_state.row[2]} успешно изменен на {name}.pt")
                        else:
                            st.warning(f"Файл {st.session_state.row[2]} не изменен на {name}.pt")
                            st.rerun()
                                                
                    query = f"""
                        UPDATE industrial.models 
                        SET 
                            status = {status}, 
                            name = '{name}.pt', 
                            definition = '{definition}', 
                            modified_on = CURRENT_TIMESTAMP, 
                            version = {st.session_state.row[6] + 1} 
                        WHERE model_id = {st.session_state.input_id}
                    """
                    DatabaseWork.row_add_edit(query)
                    st.success('Модель изменена')
                    st.rerun()
                elif name != "":
                    st.warning('Существует актуальная модель с таким же "Название"')
                else:
                    st.warning('Не заполнено "Название"')


class Monetaries:

    @st.dialog("Добавление надежности")
    def vote_add():
        number = st.number_input("Номер", min_value=1)
        status = st.segmented_control("Статус", options=["True", "False"], default="True")
        max_value = st.number_input("Максимальное значение", min_value=-1, max_value=2147483647)

        if st.button('Подтвердить добавление'):
            query = f"""
                INSERT INTO industrial.monetaries 
                (number, status, max_value, created_on, modified_on, version) VALUES 
                ({number}, {status}, {max_value}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)
            """
            try:
                DatabaseWork.row_add_edit(query)
                st.success("Новая надежность добавлена")
            except:
                st.warning('Данный номер надежности уже существует')
            st.rerun()


    @st.dialog("Изменение надежности")
    def vote_edit():
        choice = st.empty()
        confirm = st.empty()

        if st.session_state.num == 0:
            numbers = DatabaseWork.get_rows("""
                SELECT 
                    number, status 
                FROM industrial.monetaries 
                ORDER BY status DESC, number DESC
            """)
            numbers = [f'{row[1]}, {row[0]}' for row in numbers]
            numbers_ = choice.selectbox("Надежность", options=numbers)
            confirm_button = confirm.button('Подтвердить номер')
            if confirm_button:
                if numbers_ is not None:
                    num = numbers_.split(', ')[1]
                    st.session_state.num = 1
                    st.session_state.row = DatabaseWork.get_row(f"""
                        SELECT * 
                        FROM industrial.monetaries 
                        WHERE number = {num}
                    """)
                    st.session_state.input_id = num
                else:
                    st.warning('Нет надежностей')

        if st.session_state.num == 1:
            choice.info(f"Номер надежности = {st.session_state.input_id}")
            confirm.write('')
            status = st.segmented_control("Статус", options=["True", "False"], default=f"{st.session_state.row[1]}")
            max_value = st.number_input("Максимальное значение", min_value=0, max_value=2147483647, value=st.session_state.row[2])
            
            if st.button(f'Подтвердить изменение'):
                query = f"""
                    UPDATE industrial.monetaries SET 
                        status = {status}, 
                        max_value = {max_value},
                        modified_on = CURRENT_TIMESTAMP, 
                        version = {st.session_state.row[5] + 1} 
                    WHERE number = {st.session_state.input_id}
                """
                DatabaseWork.row_add_edit(query)
                st.success("Надежность изменена")
                st.rerun()


class Segments:

    @st.dialog("Добавление сегмента")
    def vote_add():
        status = st.segmented_control("Статус", options=["True", "False"], default="True")
        risk = st.text_input("Риск (число + описание)", max_chars=16)
        frequency = st.number_input("Частота", min_value=1)
        monetary = st.number_input("Надежность", min_value=1)

        if st.button('Подтвердить добавление'):
            if risk != "":
                code = frequency * 10 + monetary
                query = f"""
                    INSERT INTO industrial.segments 
                    (code, status, risk, frequency, monetary, created_on, modified_on, version) VALUES 
                    ({code}, {status}, {risk}, {frequency}, {monetary}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)
                """
                st.write(code)
                try:
                    DatabaseWork.row_add_edit(query)
                    st.success("Новый сегмент добавлен")
                except Exception as e:
                    st.warning(f'{e} Данный код частоты уже существует или отсутствуют частота с надежностью')
                st.rerun()
            else:
                st.warning('Не заполнено "Риск"')


    @st.dialog("Изменение сегмента")
    def vote_edit():
        choice = st.empty()
        confirm = st.empty()

        if st.session_state.num == 0:
            codes = DatabaseWork.get_rows("""
                SELECT 
                    code, status 
                FROM industrial.segments 
                ORDER BY status DESC, code DESC
            """)
            codes = [f'{row[1]}, {row[0]}' for row in codes]
            codes_ = choice.selectbox("Сегмент", options=codes)
            confirm_button = confirm.button('Подтвердить код')
            if confirm_button:
                if codes_ is not None:
                    cod = codes_.split(', ')[1]
                    st.session_state.num = 1
                    st.session_state.row = DatabaseWork.get_row(f"""
                        SELECT * 
                        FROM industrial.segments 
                        WHERE code = {cod}
                    """)
                    st.session_state.input_id = cod
                else:
                    st.warning('Нет сегментов')

        if st.session_state.num == 1:
            choice.info(f"Код сегмента = {st.session_state.input_id}")
            confirm.write('')

            status = st.segmented_control("Статус", options=["True", "False"], default=f"{st.session_state.row[1]}")
            risk = st.text_input("Риск (число + описание)", max_chars=16, value=st.session_state.row[2])
            frequency = st.number_input("Частота", min_value=1, value=st.session_state.row[3])
            monetary = st.number_input("Надежность", min_value=1, value=st.session_state.row[4])
  
            if st.button(f'Подтвердить изменение'):
                if risk != "":
                    query = f"""
                        UPDATE industrial.segments SET 
                            status = {status}, 
                            risk = '{risk}',
                            frequency = {frequency},
                            monetary = {monetary},
                            modified_on = CURRENT_TIMESTAMP, 
                            version = {st.session_state.row[7] + 1} 
                        WHERE code = {st.session_state.input_id}
                    """
                    DatabaseWork.row_add_edit(query)
                    st.success("Сегмент изменен")
                    st.rerun()
                else:
                    st.warning('Не заполнено "Риск"')      


class Types:

    @st.dialog("Добавление типа")
    def vote_add():
        status = st.segmented_control("Статус", options=["True", "False"], default="True")
        name = st.text_input("Название", max_chars=32)
        definition = st.text_input("Описание (необязательно)", max_chars=128)

        if st.button('Подтвердить добавление'):
            if name != "":
                query = f"""
                    INSERT INTO industrial.types 
                    (status, name, definition, created_on, modified_on, version) VALUES 
                    ({status}, '{name}', '{definition}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)
                """
                DatabaseWork.row_add_edit(query)
                st.success("Новый тип добавлен")
                st.rerun()
            else:
                st.warning('Не заполнено "Название"')


    @st.dialog("Изменение типа")
    def vote_edit():
        choice = st.empty()
        confirm = st.empty()

        if st.session_state.num == 0:
            type_ids = DatabaseWork.get_rows("""
                SELECT 
                    type_id, status, name 
                FROM industrial.types 
                ORDER BY status DESC, type_id DESC
            """)
            type_ids = [f'{row[1]}, {row[0]}: {row[2]}' for row in type_ids]
            type_ = choice.selectbox("Тип", options=type_ids)
            confirm_button = confirm.button('Подтвердить ID')
            if confirm_button:
                if type_ is not None:
                    type_id = type_.split(', ')[1].split(':')[0]
                    st.session_state.num = 1
                    st.session_state.row = DatabaseWork.get_row(f"""
                        SELECT * 
                        FROM industrial.types 
                        WHERE type_id = {type_id}
                    """)
                    st.session_state.input_id = type_id
                else:
                    st.warning('Нет типов')

        if st.session_state.num == 1:
            choice.info(f"ID типа = {st.session_state.input_id}")
            confirm.write('')

            status = st.segmented_control("Статус", options=["True", "False"], default=f"{st.session_state.row[1]}")
            name = st.text_input("Название", value=st.session_state.row[2], max_chars=32)
            definition = st.text_input("Описание (необязательно)", value=st.session_state.row[3], max_chars=128)

            if st.button(f'Подтвердить изменение'):
                if name != "":
                    query = f"""
                        UPDATE industrial.types 
                        SET 
                            status = {status}, 
                            name = '{name}', 
                            definition = '{definition}', 
                            modified_on = CURRENT_TIMESTAMP, 
                            version = {st.session_state.row[6] + 1} 
                        WHERE type_id = {st.session_state.input_id}
                    """
                    DatabaseWork.row_add_edit(query)
                    st.success("Тип изменен")
                    st.rerun()
                else:
                    st.warning('Не заполнено "Название"')


class Users:

    @st.dialog("Добавление пользователя")
    def vote_add():
        status = st.segmented_control("Статус", options=["True", "False"], default="True")
        gender = st.segmented_control("Пол", options=["Мужской", "Женский"], default="Мужской")
        login = st.text_input("Логин", max_chars=32)
        last_name = st.text_input("Фамилия", max_chars=32)
        first_name = st.text_input("Имя", max_chars=32)
        second_name = st.text_input("Отчество (необязательно)", max_chars=32)
        birth_date = st.date_input("Дата рождения", value=date(2000, 1, 1), format="YYYY-MM-DD")
        adding_date = st.date_input("Дата добавления", value=date.today(), format="YYYY-MM-DD")

        if st.button('Подтвердить добавление'):
            if login != "" and last_name != "" and first_name != "":
                query = f"""
                    INSERT INTO industrial.users 
                    (status, gender, login, last_name, first_name, second_name, 
                        birth_date, adding_date, created_on, modified_on, version) VALUES 
                    ({status}, '{gender}', '{login}', '{last_name}', '{first_name}', '{second_name}', 
                        '{birth_date}', '{adding_date}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)
                """
                DatabaseWork.row_add_edit(query)
                st.success("Новый пользователь добавлен")
                st.rerun()
            else:
                check_out = 'Не заполнено:'
                if login == "":
                    check_out += ' "Логин"'
                if last_name == "":
                    check_out += ' "Фамилия"'
                if first_name == "":
                    check_out += ' "Имя"'
                st.warning(check_out)


    @st.dialog("Изменение пользователя")
    def vote_edit():
        choice = st.empty()
        confirm = st.empty()

        if st.session_state.num == 0:
            user_ids = DatabaseWork.get_rows("""
                SELECT 
                    user_id, status, login 
                FROM industrial.users 
                ORDER BY status DESC, user_id DESC
            """)
            user_ids = [f'{row[1]}, {row[0]}: {row[2]}' for row in user_ids]
            user_ = choice.selectbox("Пользователь", options=user_ids)
            confirm_button = confirm.button('Подтвердить ID')
            if confirm_button:
                if user_ is not None:
                    user_id = user_.split(', ')[1].split(':')[0]
                    st.session_state.num = 1
                    st.session_state.row = DatabaseWork.get_row(f"""
                        SELECT * 
                        FROM industrial.users 
                        WHERE user_id = {user_id}
                    """)
                    st.session_state.input_id = user_id
                else:
                    st.warning('Нет пользователей')

        if st.session_state.num == 1:
            choice.info(f"ID пользователя = {st.session_state.input_id}")
            confirm.write('')

            status = st.segmented_control("Статус", options=["True", "False"], default=f"{st.session_state.row[1]}")
            gender = st.segmented_control("Пол", options=["Мужской", "Женский"], default=f"{st.session_state.row[2]}")
            login = st.text_input("Логин", value=st.session_state.row[3], max_chars=32)
            last_name = st.text_input("Фамилия", value=st.session_state.row[4], max_chars=32)
            first_name = st.text_input("Имя", value=st.session_state.row[5], max_chars=32)
            second_name = st.text_input("Отчество (необязательно)", value=st.session_state.row[6], max_chars=32)
            birth_date = st.date_input("Дата рождения", value=st.session_state.row[7], format="YYYY-MM-DD")
            adding_date = st.date_input("Дата добавления", value=st.session_state.row[8], format="YYYY-MM-DD")

            if st.button(f'Подтвердить изменение'):
                if login != "" and last_name != "" and first_name != "":
                    query = f"""
                        UPDATE industrial.users 
                        SET 
                            status = {status}, 
                            gender = '{gender}', 
                            login = '{login}', 
                            last_name = '{last_name}', 
                            first_name = '{first_name}', 
                            second_name = '{second_name}', 
                            birth_date = '{birth_date}', 
                            adding_date = '{adding_date}', 
                            modified_on = CURRENT_TIMESTAMP, 
                            version = {st.session_state.row[11] + 1} 
                        WHERE user_id = {st.session_state.input_id}
                    """
                    DatabaseWork.row_add_edit(query)
                    st.session_state.num = 0
                    st.success("Пользователь изменен")
                    st.rerun()
                else:
                    check_out = 'Не заполнено:'
                    if login == "":
                        check_out += ' "Логин"'
                    if last_name == "":
                        check_out += ' "Фамилия"'
                    if first_name == "":
                        check_out += ' "Имя"'
                    st.warning(check_out)
