import psycopg2


class DatabaseWork:
    @staticmethod
    def db_connection(name='postgres', user='postgres', host='localhost', port=5432, password='xxx'):
        try:
            return psycopg2.connect(dbname=name, user=user, host=host, port=port, password=password)
        except ConnectionError as e:
            print("connection:", e)
            return None

    @staticmethod
    def get_rows(query, args=None):
        connection = DatabaseWork.db_connection()
        cursor = connection.cursor()
        cursor.execute(query, args)
        rows = cursor.fetchall()
        connection.close()
        return rows

    @staticmethod
    def get_row(query, args=None):
        connection = DatabaseWork.db_connection()
        cursor = connection.cursor()
        cursor.execute(query, args)
        row = cursor.fetchone()
        connection.close()
        return row

    @staticmethod
    def get_column(query, args=None):
        connection = DatabaseWork.db_connection()
        cursor = connection.cursor()
        cursor.execute(query, args)
        column = [row[0] for row in cursor.fetchall()]
        connection.close()
        return column

    @staticmethod
    def get_field(query):
        connection = DatabaseWork.db_connection()
        cursor = connection.cursor()
        cursor.execute(query)
        field = cursor.fetchone()[0]
        connection.close()
        return field

    @staticmethod
    def get_check(query):
        connection = DatabaseWork.db_connection()
        cursor = connection.cursor()
        cursor.execute(query)
        length = len(cursor.fetchall())
        connection.close()
        return length

    @staticmethod
    def row_add_edit(query):
        connection = DatabaseWork.db_connection()
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        connection.close()
    
    @staticmethod
    def row_add_with_par(query_par):
        connection = DatabaseWork.db_connection()
        cursor = connection.cursor()
        cursor.execute(*query_par)
        connection.commit()
        connection.close()


class ScriptsSql:
    pass
