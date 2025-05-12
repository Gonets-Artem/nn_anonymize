import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st 

from help_functions.database_work import DatabaseWork


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
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.title("Аналитика")
    st.divider()


def make_filt_depth(filt_depth, key):
    return filt_depth.segmented_control("Глубина", options=["День", "Месяц", "Год"], default="День", key=key)


def make_filt_period(filt_period, label, value, key):
    return filt_period.date_input(label=label, value=value, format="YYYY-MM-DD", key=key)


def make_filt_user(filt_user, key):
    user_ids = DatabaseWork.get_rows("""
        SELECT 
            user_id, status, login 
        FROM industrial.users 
        ORDER BY status DESC, user_id DESC
    """)
    user_ids = [f'{row[1]}, {row[0]}: {row[2]}' for row in user_ids]
    user_ = filt_user.multiselect("Пользователи (исключение)", options=user_ids, key=key)
    if len(user_) == 1:
        return f"AND user_id != {int(user_[0].split(', ')[1].split(':')[0])} "
    elif len(user_) > 1:
        return f"AND user_id NOT IN {tuple(int(el.split(', ')[1].split(':')[0]) for el in user_)} " 
    return " "


def make_filt_method(filt_method, key):
    method_ids = DatabaseWork.get_rows("""
        SELECT 
            method_id, status, name 
        FROM industrial.methods 
        ORDER BY status DESC, method_id DESC
    """)
    method_ids = [f'{row[1]}, {row[0]}: {row[2]}' for row in method_ids]
    method_ = filt_method.multiselect("Методы (исключение)", options=method_ids, key=key)
    if len(method_) == 1:
        return f"AND method_id != {int(method_[0].split(', ')[1].split(':')[0])} "
    elif len(method_) > 1:
        return f"AND method_id NOT IN {tuple(int(el.split(', ')[1].split(':')[0]) for el in method_)} " 
    return " "


def make_filt_model(filt_model, key):
    model_ids = DatabaseWork.get_rows("""
        SELECT 
            model_id, status, name 
        FROM industrial.models 
        ORDER BY status DESC, model_id DESC
    """)
    model_ids = [f'{row[1]}, {row[0]}: {row[2]}' for row in model_ids]
    model_ = filt_model.multiselect("Модели (исключение)", options=model_ids, key=key)
    if len(model_) == 1:
        return f"AND model_id != {int(model_[0].split(', ')[1].split(':')[0])} "
    elif len(model_) > 1:
        return f"AND model_id NOT IN {tuple(int(el.split(', ')[1].split(':')[0]) for el in model_)} " 
    return " "


def make_filt_element(filt_element, key):
    elements_ids = DatabaseWork.get_rows("""
        SELECT 
            element_id, status, name 
        FROM industrial.elements 
        ORDER BY status DESC, element_id DESC
    """)
    elements_ids = [f'{row[1]}, {row[0]}: {row[2]}' for row in elements_ids]
    element_ = filt_element.multiselect("Элементы (исключение)", options=elements_ids, key=key)
    if len(element_) == 1:
        return f"AND element_id != {int(element_[0].split(', ')[1].split(':')[0])} "
    elif len(element_) > 1:
        return f"AND element_id NOT IN {tuple(int(el.split(', ')[1].split(':')[0]) for el in element_)} " 
    return " "


def make_filt_category(filt_category, key):
    category_ids = DatabaseWork.get_rows("""
        SELECT 
            category_id, status, name 
        FROM industrial.categories 
        ORDER BY status DESC, category_id DESC
    """)
    category_ids = [f'{row[1]}, {row[0]}: {row[2]}' for row in category_ids]
    category_ = filt_category.multiselect("Категории (исключение)", options=category_ids, key=key)
    if len(category_) == 1:
        return f"AND category_id != {int(category_[0].split(', ')[1].split(':')[0])} "
    elif len(category_) > 1:
        return f"AND category_id NOT IN {tuple(int(el.split(', ')[1].split(':')[0]) for el in category_)} " 
    return " "


def make_filt_type(filt_type, key):
    type_ids = DatabaseWork.get_rows("""
        SELECT 
            type_id, status, name 
        FROM industrial.types 
        ORDER BY status DESC, type_id DESC
    """)
    type_ids = [f'{row[1]}, {row[0]}: {row[2]}' for row in type_ids]
    type_ = filt_type.multiselect("Типы (исключение)", options=type_ids, key=key)
    if len(type_) == 1:
        return f"AND type_id != {int(type_[0].split(', ')[1].split(':')[0])} "
    elif len(type_) > 1:
        return f"AND type_id NOT IN {tuple(int(el.split(', ')[1].split(':')[0]) for el in type_)} " 
    return " "


def get_dates_user():
    min_date_user, max_date_user = DatabaseWork.get_row("""
        SELECT min(adding_date), max(adding_date) FROM industrial.matview_users;
    """)
    return min_date_user, max_date_user


def get_dates_session():
    min_date_session, max_date_session, max_datetime_session = DatabaseWork.get_row(f"""
        SELECT min(start_date), max(start_date), max(start_datetime) FROM industrial.matview_sessions;
    """)
    return min_date_session, max_date_session, max_datetime_session


def show_tab_11(
    filt_depth, filt_period_start, filt_period_end,
    min_date_user, max_date_user,
    div_111, subh_111, ch_111,
):
    depth = make_filt_depth(filt_depth, key=0)
    period_start = make_filt_period(filt_period_start, value=min_date_user, label="Период добавления пользователя - начало", key=1)
    period_end = make_filt_period(filt_period_end, value=max_date_user, label="Период добавления пользователя - конец", key=2)

    chart_111(div_111, subh_111, ch_111, depth, period_start, period_end)


def show_tab_12(
    filt_period_start, filt_period_end, filt_user, filt_method, filt_model,
    min_date_session, max_date_session,
    div_121, subh_121, ta_121,
    div_122, subh_122, ta_122,
    div_123, subh_123, ta_123,
):
    period_start = make_filt_period(filt_period_start, value=min_date_session, label="Период старта сеанса - начало", key=3)
    period_end = make_filt_period(filt_period_end, value=max_date_session, label="Период старта сеанса - конец", key=4)
    user = make_filt_user(filt_user, key=5)
    method = make_filt_method(filt_method, key=6)
    model = make_filt_model(filt_model, key=7)

    chart_121(div_121, subh_121, ta_121, period_start, period_end, user, method, model)
    chart_122(div_122, subh_122, ta_122, period_start, period_end, user, method, model)    
    chart_123(div_123, subh_123, ta_123, period_start, period_end, user, method, model)


def show_tab_21(
    filt_period_start, filt_period_end,
    min_date_session, max_date_session,
    div_211, subh_211, ba_211,
    div_212, subh_212, ba_212,
):
    period_start = make_filt_period(filt_period_start, value=min_date_session, label="Период старта сеанса - начало", key=8)
    period_end = make_filt_period(filt_period_end, value=max_date_session, label="Период старта сеанса - конец", key=9)

    chart_211(div_211, subh_211, ba_211, period_start, period_end)
    chart_212(div_212, subh_212, ba_212, period_start, period_end)


def show_tab_22(
    filt_depth, filt_period_start, filt_period_end, filt_user,
    min_date_session, max_date_session,
    div_221, subh_221, ch_221,
    div_222, subh_222, ch_222,
    div_223, subh_223, ch_223,
    div_224, subh_224, ch_224,
):
    depth = make_filt_depth(filt_depth, key=33)
    period_start = make_filt_period(filt_period_start, value=min_date_session, label="Период старта сеанса - начало", key=10)
    period_end = make_filt_period(filt_period_end, value=max_date_session, label="Период старта сеанса - конец", key=11)
    user = make_filt_user(filt_user, key=12)

    chart_221(div_221, subh_221, ch_221, depth, period_start, period_end, user)
    chart_222(div_222, subh_222, ch_222, depth, period_start, period_end, user)
    chart_223(div_223, subh_223, ch_223, depth, period_start, period_end, user)
    chart_224(div_224, subh_224, ch_224, depth, period_start, period_end, user)


def show_tab_23(
    filt_period_start, filt_period_end, filt_user,
    min_date_session, max_date_session,
    div_231, subh_231, ta_231,
):
    period_start = make_filt_period(filt_period_start, value=min_date_session, label="Период старта сеанса - начало", key=13)
    period_end = make_filt_period(filt_period_end, value=max_date_session, label="Период старта сеанса - конец", key=14)
    user = make_filt_user(filt_user, key=15)

    chart_231(div_231, subh_231, ta_231, period_start, period_end, user)


def show_tab_31(
    filt_period_start, filt_period_end, filt_user, filt_method, filt_model,
    min_date_session, max_date_session,
    div_311, subh_311, ba_311,
    div_312, subh_312, ba_312,
):
    period_start = make_filt_period(filt_period_start, value=min_date_session, label="Период старта сеанса - начало", key=16)
    period_end = make_filt_period(filt_period_end, value=max_date_session, label="Период старта сеанса - конец", key=17)
    user = make_filt_user(filt_user, key=18)
    method = make_filt_method(filt_method, key=19)
    model = make_filt_model(filt_model, key=20)

    chart_311(div_311, subh_311, ba_311, period_start, period_end, user, method, model)
    chart_312(div_312, subh_312, ba_312, period_start, period_end, user, method, model)


def show_tab_32(
    filt_depth, filt_period_start, filt_period_end, filt_user, filt_method, filt_model,
    min_date_session, max_date_session,
    div_321, subh_321, ch_321,
):
    depth = make_filt_depth(filt_depth, key=21)
    period_start = make_filt_period(filt_period_start, value=min_date_session, label="Период старта сеанса - начало", key=22)
    period_end = make_filt_period(filt_period_end, value=max_date_session, label="Период старта сеанса - конец", key=23)
    user = make_filt_user(filt_user, key=24)
    method = make_filt_method(filt_method, key=25)
    model = make_filt_model(filt_model, key=26)

    chart_321(div_321, subh_321, ch_321, depth, period_start, period_end, user, method, model)


def show_tab_4(
    filt_period_start, filt_period_end, filt_user, filt_element, filt_category, filt_type,
    min_date_session, max_date_session,
    div_411, subh_411, ta_411,
    div_412, subh_412, ta_412,
    div_413, subh_413, ta_413,
):
    period_start = make_filt_period(filt_period_start, value=min_date_session, label="Период старта сеанса - начало", key=27)
    period_end = make_filt_period(filt_period_end, value=max_date_session, label="Период старта сеанса - конец", key=28)
    user = make_filt_user(filt_user, key=29)
    element = make_filt_element(filt_element, key=30)
    category = make_filt_category(filt_category, key=31)
    type__ = make_filt_type(filt_type, key=32)

    chart_411(div_411, subh_411, ta_411, period_start, period_end, user, element, category, type__)
    chart_412(div_412, subh_412, ta_412, period_start, period_end, user, element, category, type__)
    chart_413(div_413, subh_413, ta_413, period_start, period_end, user, element, category, type__)


def show_tab_5(
    div_511, subh_511, wr_1, wr_2, ta_511,
):
    chart_511(div_511, subh_511, wr_1, wr_2, ta_511)


def chart_111(div, subh, ch, depth, period_start, period_end):
    div.divider()
    subh.subheader('Динамика (кумулятивная) добавленных пользователей', divider='red')

    if depth == "День":
        format_depth = 'YYYY-MM-DD'
        xaxis = dict(tickformat='%Y-%m-%d')
    elif depth == "Месяц":
        format_depth = 'YYYY-MM'
        xaxis = dict(tickformat='%Y-%m')
    elif depth == "Год":
        format_depth = 'YYYY'
        xaxis = dict(tickformat='%Y')
        
    data = DatabaseWork.get_rows(f"""
        SELECT 
            period, COALESCE(LAG(value) OVER (ORDER BY period), 0) + value count_users
        FROM (
            SELECT 
                TO_CHAR(adding_date, '{format_depth}') period, count(user_id) value 
            FROM industrial.matview_users 
            WHERE adding_date BETWEEN '{period_start}' AND '{period_end}' 
            GROUP BY TO_CHAR(adding_date, '{format_depth}')
            ORDER BY period
        ) t
    """)
    df = pd.DataFrame(data, columns=["Период", "Количество пользователей"])
    tick = int((df["Количество пользователей"].max()-df["Количество пользователей"].min()-1)/10+1) if len(df) else 1
    fig = px.line(df, x='Период', y='Количество пользователей', markers=True)
    fig.update_layout(
        xaxis=xaxis,
        yaxis=dict(tickmode='linear', dtick=tick),
        yaxis_range=[0, None],
    )
    fig.update_traces(
        marker=dict(size=6)
    )
    ch.plotly_chart(fig, use_container_width=True, key=100)


def chart_121(div, subh, ta, period_start, period_end, user, method, model):
    div.divider()
    subh.subheader('Количество сеансов, видеоизображений, замененных кадров по пользователям', divider='orange')

    data = DatabaseWork.get_rows(f"""
        SELECT
            user_login,
            count(DISTINCT session_session_id) count_sessions,
            count(DISTINCT (video_image_session_id, video_image_datetime)) count_video_images,
            count(DISTINCT (video_image_session_id, video_image_datetime)) FILTER(WHERE category_name = 'Запрещен' and detection_is_active = True) count_detection_video_images,
            (count(DISTINCT (video_image_session_id, video_image_datetime)) FILTER(WHERE category_name = 'Запрещен' and detection_is_active = True))::numeric / count(DISTINCT (video_image_session_id, video_image_datetime))::numeric * 100 part_detection_video_images
        FROM industrial.matview_detections 
        WHERE start_date BETWEEN '{period_start}' AND '{period_end}'  
            {user} {method} {model} 
        GROUP BY user_login
        ORDER BY user_login 
    """)
    df = pd.DataFrame(
        data, 
        columns=[
            "Логин", 
            "Количество сеансов", 
            "Количество кадров", 
            "Количество замененных кадров", 
            "Процент замененных кадров"
        ]
    )
    ta.dataframe(df)


def chart_122(div, subh, ta, period_start, period_end, user, method, model):
    div.divider()
    subh.subheader('Количество сеансов, видеоизображений, замененных кадров по пользователям и анонимизациям', divider='green')

    data = DatabaseWork.get_rows(f"""
        SELECT
            user_login,
            anonymization_id,
            method_name,
            model_name,
            count(DISTINCT session_session_id) count_sessions,
            count(DISTINCT (video_image_session_id, video_image_datetime)) count_video_images,
            count(DISTINCT (video_image_session_id, video_image_datetime)) FILTER(WHERE category_name = 'Запрещен' and detection_is_active = True) count_detection_video_images,
            (count(DISTINCT (video_image_session_id, video_image_datetime)) FILTER(WHERE category_name = 'Запрещен' and detection_is_active = True))::numeric / count(DISTINCT (video_image_session_id, video_image_datetime))::numeric * 100 part_detection_video_images
        FROM industrial.matview_anonymizations_full 
        WHERE start_date BETWEEN '{period_start}' AND '{period_end}' 
            {user} {method} {model} 
        GROUP BY 
            user_login,
            anonymization_id,
            method_name,
            model_name
        ORDER BY user_login
    """)
    df = pd.DataFrame(
        data, 
        columns=[
            "Логин", 
            "ID анонимизации",
            "Метод",
            "Модель",
            "Количество сеансов", 
            "Количество кадров", 
            "Количество замененных кадров", 
            "Процент замененных кадров"
        ]
    )
    ta.dataframe(df)


def chart_123(div, subh, ta, period_start, period_end, user, method, model):
    div.divider()
    subh.subheader('Время использования способов анонимизации', divider='blue')

    data = DatabaseWork.get_rows(f"""
        SELECT
            anonymization_id,
            set_datetime,
            method_name,
            model_name,
            sum(duration_session) sum_duration_sessions
        FROM industrial.matview_anonymizations_mini
        WHERE start_date BETWEEN '{period_start}' AND '{period_end}' 
            {user} {method} {model} 
        GROUP BY 
            anonymization_id,
            set_datetime,
            method_name,
            model_name
        ORDER BY sum_duration_sessions DESC
    """)
    df = pd.DataFrame(
        data, 
        columns=[
            "ID анонимизации",
            "Время установки метода",
            "Метод",
            "Модель",
            "Сумма времени сеансов"
        ]
    )
    ta.dataframe(df)


def chart_211(div, subh, ba, period_start, period_end):
    div.divider()
    subh.subheader('Количество сеансов по пользователям (Топ 10)', divider='red')

    data = DatabaseWork.get_rows(f"""
        SELECT
            user_login,
            count(DISTINCT session_id) count_sessions
        FROM industrial.matview_sessions
        WHERE start_date BETWEEN '{period_start}' AND '{period_end}' 
        GROUP BY 
            user_login
        ORDER BY count_sessions DESC
        LIMIT 10
    """)
    df = pd.DataFrame(
        data, 
        columns=[
            "Логин",
            "Количество сеансов"
        ]
    )
    fig = px.bar(df, x='Логин', y='Количество сеансов', text_auto=True)
    ba.plotly_chart(fig, key=101)


def chart_212(div, subh, ba, period_start, period_end):
    div.divider()
    subh.subheader('Количество сеансов по пользователям с анонимизацией (Топ 10)', divider='orange')

    data = DatabaseWork.get_rows(f"""
        SELECT
            user_login,
            anonymization_id,
            count(DISTINCT session_session_id) count_sessions
        FROM industrial.matview_anonymizations_mini
        WHERE start_date BETWEEN '{period_start}' AND '{period_end}' 
        GROUP BY 
            user_login,
            anonymization_id
        ORDER BY count_sessions DESC
        LIMIT 10
    """)
    df = pd.DataFrame(
        data, 
        columns=[
            "Логин",
            "ID анонимизации",
            "Количество сеансов"
        ]
    )
    fig = px.bar(df, x='Логин', y='Количество сеансов', color='ID анонимизации', text_auto=True)
    ba.plotly_chart(fig, key=102)


def chart_221(div, subh, ch, depth, period_start, period_end, user):
    div.divider()
    subh.subheader('Динамика количества сеансов', divider='green')

    if depth == "День":
        format_depth = 'YYYY-MM-DD'
        xaxis = dict(tickformat='%Y-%m-%d')
    elif depth == "Месяц":
        format_depth = 'YYYY-MM'
        xaxis = dict(tickformat='%Y-%m')
    elif depth == "Год":
        format_depth = 'YYYY'
        xaxis = dict(tickformat='%Y')
        
    data = DatabaseWork.get_rows(f"""
        SELECT 
            TO_CHAR(start_date, '{format_depth}') period, count(session_id) value 
        FROM industrial.matview_sessions
        WHERE start_date BETWEEN '{period_start}' AND '{period_end}'  
            {user}
        GROUP BY TO_CHAR(start_date, '{format_depth}')
        ORDER BY period
    """)
    df = pd.DataFrame(data, columns=["Период", "Количество сеансов"])
    tick = int((df["Количество сеансов"].max()-df["Количество сеансов"].min()-1)/10+1) if len(df) else 1
    fig = px.line(df, x='Период', y='Количество сеансов', markers=True)
    fig.update_layout(
        xaxis=xaxis,
        yaxis=dict(tickmode='linear', dtick=tick),
        yaxis_range=[0, None],
    )
    fig.update_traces(
        marker=dict(size=6) #, line_shape='spline'
    )
    ch.plotly_chart(fig, use_container_width=True, key=103)


def chart_222(div, subh, ch, depth, period_start, period_end, user):
    div.divider()
    subh.subheader('Динамика средней длительности сеансов', divider='blue')

    if depth == "День":
        format_depth = 'YYYY-MM-DD'
        xaxis = dict(tickformat='%Y-%m-%d')
    elif depth == "Месяц":
        format_depth = 'YYYY-MM'
        xaxis = dict(tickformat='%Y-%m')
    elif depth == "Год":
        format_depth = 'YYYY'
        xaxis = dict(tickformat='%Y')
        
    data = DatabaseWork.get_rows(f"""
        SELECT 
            TO_CHAR(start_date, '{format_depth}') period, avg(duration_seconds) value 
        FROM industrial.matview_sessions
        WHERE start_date BETWEEN '{period_start}' AND '{period_end}' 
            {user}
        GROUP BY TO_CHAR(start_date, '{format_depth}')
        ORDER BY period
    """)
    df = pd.DataFrame(data, columns=["Период", "Средняя длительность сеансов, c"])
    tick = int((df["Средняя длительность сеансов, c"].max()-df["Средняя длительность сеансов, c"].min()-1)/10+1) if len(df) else 1
    fig = px.line(df, x='Период', y='Средняя длительность сеансов, c', markers=True)
    fig.update_layout(
        xaxis=xaxis,
        yaxis=dict(tickmode='linear', dtick=tick),
        yaxis_range=[0, None],
    )
    fig.update_traces(
        marker=dict(size=6) #, line_shape='spline'
    )
    ch.plotly_chart(fig, use_container_width=True, key=104)


def chart_223(div, subh, ch, depth, period_start, period_end, user):
    div.divider()
    subh.subheader('Динамика суммарной длительности сеансов', divider='violet')

    if depth == "День":
        format_depth = 'YYYY-MM-DD'
        xaxis = dict(tickformat='%Y-%m-%d')
    elif depth == "Месяц":
        format_depth = 'YYYY-MM'
        xaxis = dict(tickformat='%Y-%m')
    elif depth == "Год":
        format_depth = 'YYYY'
        xaxis = dict(tickformat='%Y')
        
    data = DatabaseWork.get_rows(f"""
        SELECT 
            TO_CHAR(start_date, '{format_depth}') period, sum(duration_seconds) / 60 value 
        FROM industrial.matview_sessions
        WHERE start_date BETWEEN '{period_start}' AND '{period_end}'  
            {user}
        GROUP BY TO_CHAR(start_date, '{format_depth}')
        ORDER BY period
    """)
    df = pd.DataFrame(data, columns=["Период", "Суммарная длительность сеансов, мин"])
    tick = int((df["Суммарная длительность сеансов, мин"].max()-df["Суммарная длительность сеансов, мин"].min()-1)/10+1) if len(df) else 1
    fig = px.line(df, x='Период', y='Суммарная длительность сеансов, мин', markers=True)
    fig.update_layout(
        xaxis=xaxis,
        yaxis=dict(tickmode='linear', dtick=tick),
        yaxis_range=[0, None],
    )
    fig.update_traces(
        marker=dict(size=6) #, line_shape='spline'
    )
    ch.plotly_chart(fig, use_container_width=True, key=105)


def chart_224(div, subh, ch, depth, period_start, period_end, user):
    div.divider()
    subh.subheader('Динамика процента сеансов с анонимизацией', divider='gray')

    if depth == "День":
        format_depth = 'YYYY-MM-DD'
        xaxis = dict(tickformat='%Y-%m-%d')
    elif depth == "Месяц":
        format_depth = 'YYYY-MM'
        xaxis = dict(tickformat='%Y-%m')
    elif depth == "Год":
        format_depth = 'YYYY'
        xaxis = dict(tickformat='%Y')
        
    data = DatabaseWork.get_rows(f"""
        SELECT 
            TO_CHAR(start_date, 'YYYY-MM-DD') period, 
            (count(DISTINCT session_session_id) FILTER(WHERE category_name = 'Запрещен' and detection_is_active = True))::numeric / count(DISTINCT session_session_id)::numeric * 100 value 
        FROM industrial.matview_detections
        WHERE start_date BETWEEN '{period_start}' AND '{period_end}'  
            {user}
        GROUP BY TO_CHAR(start_date, 'YYYY-MM-DD')
        ORDER BY period
    """)
    df = pd.DataFrame(data, columns=["Период", "Процент сеансов с анонимизацией"])
    tick = int((df["Процент сеансов с анонимизацией"].max()-df["Процент сеансов с анонимизацией"].min()-1)/10+1) if len(df) else 1
    fig = px.line(df, x='Период', y='Процент сеансов с анонимизацией', markers=True)
    fig.update_layout(
        xaxis=xaxis,
        yaxis=dict(tickmode='linear', dtick=tick),
        yaxis_range=[0, None],
    )
    fig.update_traces(
        marker=dict(size=6) #, line_shape='spline'
    )
    ch.plotly_chart(fig, use_container_width=True, key=106)


def chart_231(div, subh, ta, period_start, period_end, user):
    div.divider()
    subh.subheader('Метрики длительности сеансов по пользователям', divider='red')

    data = DatabaseWork.get_rows(f"""
        SELECT
            user_login,
            min(duration_seconds) min_duration_seconds,
            avg(duration_seconds) avg_duration_seconds,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_seconds),
            max(duration_seconds) max_duration_seconds
        FROM industrial.matview_sessions
        WHERE start_date BETWEEN '{period_start}' AND '{period_end}' 
            {user}
        GROUP BY user_login
        ORDER BY user_login
    """)
    df = pd.DataFrame(
        data, 
        columns=[
            "Логин",
            "Минимальная, с",
            "Средняя, с",
            "Медианная, с",
            "Максимальная, с"
        ]
    )
    ta.dataframe(df)


def chart_311(div, subh, ba, period_start, period_end, user, method, model):
    div.divider()
    subh.subheader('Процент замененных кадров по пользователям (Топ 10)', divider='red')

    data = DatabaseWork.get_rows(f"""
        SELECT
            user_login,
            (count(DISTINCT (video_image_session_id, video_image_datetime)) FILTER(WHERE category_name = 'Запрещен' and detection_is_active = True))::numeric / count(DISTINCT (video_image_session_id, video_image_datetime))::numeric * 100 part_detection_video_images
        FROM industrial.matview_detections
        WHERE start_date BETWEEN '{period_start}' AND '{period_end}' 
            {user} {method} {model} 
        GROUP BY 
            user_login
        ORDER BY part_detection_video_images DESC
        LIMIT 10
    """)
    df = pd.DataFrame(
        data, 
        columns=[
            "Логин",
            "Процент замененных кадров"
        ]
    )
    fig = px.bar(df, x='Логин', y='Процент замененных кадров', text_auto=True)
    ba.plotly_chart(fig, key=107)


def chart_312(div, subh, ba, period_start, period_end, user, method, model):
    div.divider()
    subh.subheader('Процент замененных кадров по пользователям с анонимизацией (Топ 10)', divider='orange')

    data = DatabaseWork.get_rows(f"""
        SELECT
            user_login,
            anonymization_id,
            (count(DISTINCT (video_image_session_id, video_image_datetime)) FILTER(WHERE category_name = 'Запрещен' and detection_is_active = True))::numeric / count(DISTINCT (video_image_session_id, video_image_datetime))::numeric * 100 part_detection_video_images
        FROM industrial.matview_detections
        WHERE start_date BETWEEN '{period_start}' AND '{period_end}'  
            {user} {method} {model} 
        GROUP BY 
            user_login,
            anonymization_id
        ORDER BY part_detection_video_images DESC
        LIMIT 10
    """)
    df = pd.DataFrame(
        data, 
        columns=[
            "Логин",
            "ID анонимизации",
            "Процент замененных кадров"
        ]
    )
    fig = px.bar(df, x='Логин', y='Процент замененных кадров', color='ID анонимизации', barmode="group", text_auto=True)
    ba.plotly_chart(fig, key=108)


def chart_321(div, subh, ch, depth, period_start, period_end, user, method, model):
    div.divider()
    subh.subheader('Динамика процента видеоизображений с анонимизацией', divider='green')

    if depth == "День":
        format_depth = 'YYYY-MM-DD'
        xaxis = dict(tickformat='%Y-%m-%d')
    elif depth == "Месяц":
        format_depth = 'YYYY-MM'
        xaxis = dict(tickformat='%Y-%m')
    elif depth == "Год":
        format_depth = 'YYYY'
        xaxis = dict(tickformat='%Y')
        
    data = DatabaseWork.get_rows(f"""
        SELECT 
            TO_CHAR(start_date, '{format_depth}') period, 
            (count(DISTINCT (video_image_session_id, video_image_datetime)) FILTER(WHERE category_name = 'Запрещен' and detection_is_active = True))::numeric / count(DISTINCT (video_image_session_id, video_image_datetime))::numeric * 100 value 
        FROM industrial.matview_detections
        WHERE start_date BETWEEN '{period_start}' AND '{period_end}' 
            {user} {method} {model}
        GROUP BY TO_CHAR(start_date, '{format_depth}')
        ORDER BY period
    """)
    df = pd.DataFrame(data, columns=["Период", "Процент видеоизображений с анонимизацией"])
    tick = int((df["Процент видеоизображений с анонимизацией"].max()-df["Процент видеоизображений с анонимизацией"].min()-1)/10+1) if len(df) else 1
    fig = px.line(df, x='Период', y='Процент видеоизображений с анонимизацией', markers=True)
    fig.update_layout(
        xaxis=xaxis,
        yaxis=dict(tickmode='linear', dtick=tick),
        yaxis_range=[0, None],
    )
    fig.update_traces(
        marker=dict(size=6)
    )
    ch.plotly_chart(fig, use_container_width=True, key=109)


def chart_411(div, subh, ta, period_start, period_end, user, element, category, type__):
    div.divider()
    subh.subheader('Количество найденных различных элементов по пользователям', divider='red')

    data = DatabaseWork.get_rows(f"""
        SELECT
            user_login,
            element_name,
            category_name,
            type_name,
            count(DISTINCT (detection_id, detection_session_id, detection_datetime)) count_elements,
            avg(probability) avg_probability
        FROM industrial.matview_detections
        WHERE start_date BETWEEN '{period_start}' AND '{period_end}' 
            {user} {element} {category} {type__} 
        GROUP BY 
            user_login,
            element_name,
            category_name,
            type_name
        ORDER BY count_elements DESC
    """)
    df = pd.DataFrame(
        data, 
        columns=[
            "Логин",
            "Элемент",
            "Категория",
            "Тип",
            "Количество обнаружений",
            "Средняя вероятность обнаружений"
        ]
    )
    ta.dataframe(df)


def chart_412(div, subh, ta, period_start, period_end, user, element, category, type__):
    div.divider()
    subh.subheader('Количество найденных различных категорий по пользователям', divider='orange')

    data = DatabaseWork.get_rows(f"""
        SELECT
            user_login,
            category_name,
            count(DISTINCT (detection_id, detection_session_id, detection_datetime)) count_categories,
            avg(probability) avg_probability
        FROM industrial.matview_detections
        WHERE start_date BETWEEN '{period_start}' AND '{period_end}' 
            {user} {element} {category} {type__} 
        GROUP BY 
            user_login,
            category_name
        ORDER BY count_categories DESC
    """)
    df = pd.DataFrame(
        data, 
        columns=[
            "Логин",
            "Категория",
            "Количество обнаружений",
            "Средняя вероятность обнаружений"
        ]
    )
    ta.dataframe(df)


def chart_413(div, subh, ta, period_start, period_end, user, element, category, type__):
    div.divider()
    subh.subheader('Количество найденных различных типов по пользователям', divider='green')

    data = DatabaseWork.get_rows(f"""
        SELECT
            user_login,
            type_name,
            count(DISTINCT (detection_id, detection_session_id, detection_datetime)) count_types,
            avg(probability) avg_probability
        FROM industrial.matview_detections
        WHERE start_date BETWEEN '{period_start}' AND '{period_end}' 
            {user} {element} {category} {type__} 
        GROUP BY 
            user_login,
            type_name
        ORDER BY count_types DESC
    """)
    df = pd.DataFrame(
        data, 
        columns=[
            "Логин",
            "Тип",
            "Количество обнаружений",
            "Средняя вероятность обнаружений"
        ]
    )
    ta.dataframe(df)


def chart_511(div, subh, wr_1, wr_2, ta):
    div.divider()
    subh.subheader('Таблица FM-анализа по данным за последний месяц', divider='red')

    wr_1.write('Частота : Длительность сеансов, мин.')
    wr_2.write('Надежность: Сумма метрики по активным обнаружениям (Приоритет элемента * Количество кадров * (Средняя вероятность ^ 2))')

    data = DatabaseWork.get_rows(f"""
        SELECT * FROM industrial.matview_fm_analysis
    """)
    df = pd.DataFrame(
        data, 
        columns=[
            "Логин",
            "Код сегмента",
            "Уровень риска",
            "Частота",
            "Надежность"
        ]
    )
    ta.dataframe(df)


def main():
    settings()

    st.header("Обновление данных", divider="rainbow")

    min_date_user, max_date_user = get_dates_user()
    min_date_session, max_date_session, max_datetime_session = get_dates_session()

    st.info(f'Последнее время сеанса: {max_datetime_session}')

    if st.button("Обновить данные"):
        DatabaseWork.row_add_edit("REFRESH MATERIALIZED VIEW industrial.matview_anonymizations_full")
        DatabaseWork.row_add_edit("REFRESH MATERIALIZED VIEW industrial.matview_anonymizations_mini")
        DatabaseWork.row_add_edit("REFRESH MATERIALIZED VIEW industrial.matview_detections")
        DatabaseWork.row_add_edit("REFRESH MATERIALIZED VIEW industrial.matview_sessions")
        DatabaseWork.row_add_edit("REFRESH MATERIALIZED VIEW industrial.matview_users")
        DatabaseWork.row_add_edit("REFRESH MATERIALIZED VIEW industrial.matview_fm_analysis")
        st.rerun()

    st.divider()
    st.header("Визуализации", divider="rainbow")

    tab_1, tab_2, tab_3, tab_4, tab_5 = st.tabs(["Общие", "Сеансы", "Видеоизображения", "Обнаружения", "FM-анализ"])
    
    with tab_1:
        tab_11, tab_12 = st.tabs(["Графики", 'Таблицы'])

        with tab_11:
            filt_11_depth = st.empty()
            filt_11_period_start = st.empty()
            filt_11_period_end = st.empty()

            div_111 = st.empty()
            subh_111 = st.empty()
            ch_111 = st.empty()

        with tab_12:
            filt_12_period_start = st.empty()
            filt_12_period_end = st.empty()
            filt_12_user = st.empty() 
            filt_12_method = st.empty()
            filt_12_model = st.empty()

            div_121 = st.empty()
            subh_121 = st.empty()
            ta_121 = st.empty()

            div_122 = st.empty()
            subh_122 = st.empty()
            ta_122 = st.empty()

            div_123 = st.empty()
            subh_123 = st.empty()
            ta_123 = st.empty()

    with tab_2:
        tab_21, tab_22, tab_23 = st.tabs(["Диаграммы", "Графики", 'Таблицы'])
        with tab_21:
            filt_21_period_start = st.empty()
            filt_21_period_end = st.empty()

            div_211 = st.empty()
            subh_211 = st.empty()
            ba_211 = st.empty()

            div_212 = st.empty()
            subh_212 = st.empty()
            ba_212 = st.empty()

        with tab_22:
            filt_24_depth = st.empty()
            filt_22_period_start = st.empty()
            filt_22_period_end = st.empty()
            filt_22_user = st.empty()

            div_221 = st.empty()
            subh_221 = st.empty()
            ch_221 = st.empty()

            div_222 = st.empty()
            subh_222 = st.empty()
            ch_222 = st.empty()

            div_223 = st.empty()
            subh_223 = st.empty()
            ch_223 = st.empty()

            div_224 = st.empty()
            subh_224 = st.empty()
            ch_224 = st.empty()

        with tab_23:
            filt_23_period_start = st.empty()
            filt_23_period_end = st.empty()
            filt_23_user = st.empty()

            div_231 = st.empty()
            subh_231 = st.empty()
            ta_231 = st.empty()

    with tab_3:
        tab_31, tab_32 = st.tabs(["Диаграммы", 'Графики'])

        with tab_31:
            filt_31_period_start = st.empty()
            filt_31_period_end = st.empty()
            filt_31_user = st.empty()
            filt_31_method = st.empty()
            filt_31_model = st.empty()

            div_311 = st.empty()
            subh_311 = st.empty()
            ba_311 = st.empty()

            div_312 = st.empty()
            subh_312 = st.empty()
            ba_312 = st.empty()

        with tab_32:
            filt_32_depth = st.empty()
            filt_32_period_start = st.empty()
            filt_32_period_end = st.empty()
            filt_32_user = st.empty()
            filt_32_method = st.empty()
            filt_32_model = st.empty()

            div_321 = st.empty()
            subh_321 = st.empty()
            ch_321 = st.empty()

    with tab_4:
        filt_41_period_start = st.empty()
        filt_41_period_end = st.empty()
        filt_41_user = st.empty()
        filt_41_element = st.empty()
        filt_41_category = st.empty()
        filt_41_type = st.empty()

        div_411 = st.empty()
        subh_411 = st.empty()
        ta_411 = st.empty()

        div_412 = st.empty()
        subh_412 = st.empty()
        ta_412 = st.empty()

        div_413 = st.empty()
        subh_413 = st.empty()
        ta_413 = st.empty()

    with tab_5:
        div_511 = st.empty()
        subh_511 = st.empty()
        wr_1 = st.empty() 
        wr_2 = st.empty()
        ta_511 = st.empty()

    show_tab_11(
        filt_11_depth, filt_11_period_start, filt_11_period_end,
        min_date_user, max_date_user,
        div_111, subh_111, ch_111,
    )
    show_tab_12(
        filt_12_period_start, filt_12_period_end, filt_12_user, filt_12_method, filt_12_model,
        min_date_session, max_date_session,
        div_121, subh_121, ta_121,
        div_122, subh_122, ta_122,
        div_123, subh_123, ta_123,
    )
    show_tab_21(
        filt_21_period_start, filt_21_period_end,
        min_date_session, max_date_session,
        div_211, subh_211, ba_211,
        div_212, subh_212, ba_212,
    )
    show_tab_22(
        filt_24_depth, filt_22_period_start, filt_22_period_end, filt_22_user,
        min_date_session, max_date_session,
        div_221, subh_221, ch_221,
        div_222, subh_222, ch_222,
        div_223, subh_223, ch_223,
        div_224, subh_224, ch_224,
    )
    show_tab_23(
        filt_23_period_start, filt_23_period_end, filt_23_user,
        min_date_session, max_date_session,
        div_231, subh_231, ta_231,
    )
    show_tab_31(
        filt_31_period_start, filt_31_period_end, filt_31_user, filt_31_method, filt_31_model,
        min_date_session, max_date_session,
        div_311, subh_311, ba_311,
        div_312, subh_312, ba_312,
    )
    show_tab_32(
        filt_32_depth, filt_32_period_start, filt_32_period_end, filt_32_user, filt_32_method, filt_32_model,
        min_date_session, max_date_session,
        div_321, subh_321, ch_321,
    )
    show_tab_4(
        filt_41_period_start, filt_41_period_end, filt_41_user, filt_41_element, filt_41_category, filt_41_type,
        min_date_session, max_date_session,
        div_411, subh_411, ta_411,
        div_412, subh_412, ta_412,
        div_413, subh_413, ta_413,
    )
    show_tab_5(
        div_511, subh_511, wr_1, wr_2, ta_511,
    )

if __name__ == "__main__":
    main()
