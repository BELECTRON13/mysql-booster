import mysql.connector


class SimpleMySQL:
    def __init__(self, host, user, password, database):
        self.conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

    def select(self, table, columns="*", where=None, limit=None):
        sql = f"SELECT {self._cols(columns)} FROM {table}"
        values = []

        if where:
            w, v = self._where(where)
            sql += " WHERE " + w
            values = v

        if limit:
            sql += f" LIMIT {limit}"

        cur = self.conn.cursor(dictionary=True)
        cur.execute(sql, values)
        return cur.fetchall()

    def insert(self, table, data: dict):
        keys = ", ".join(data.keys())
        placeholders = ", ".join(["%s"] * len(data))
        sql = f"INSERT INTO {table} ({keys}) VALUES ({placeholders})"

        cur = self.conn.cursor()
        cur.execute(sql, tuple(data.values()))
        self.conn.commit()
        return cur.lastrowid

    def update(self, table, data: dict, where: dict):
        set_sql = ", ".join([f"{k}=%s" for k in data])
        where_sql, where_values = self._where(where)

        sql = f"UPDATE {table} SET {set_sql} WHERE {where_sql}"
        values = list(data.values()) + where_values

        cur = self.conn.cursor()
        cur.execute(sql, values)
        self.conn.commit()
        return cur.rowcount

    def delete(self, table, where: dict):
        where_sql, values = self._where(where)
        sql = f"DELETE FROM {table} WHERE {where_sql}"

        cur = self.conn.cursor()
        cur.execute(sql, values)
        self.conn.commit()
        return cur.rowcount

    def _where(self, conditions: dict):
        parts = []
        values = []

        ops = {
            "gt": ">",
            "lt": "<",
            "gte": ">=",
            "lte": "<=",
            "ne": "!="
        }

        for key, value in conditions.items():
            if "__" in key:
                field, op = key.split("__")
                parts.append(f"{field} {ops[op]} %s")
            else:
                parts.append(f"{key} = %s")
            values.append(value)

        return " AND ".join(parts), values

    def _cols(self, columns):
        if columns == "*" or columns is None:
            return "*"
        if isinstance(columns, (list, tuple)):
            return ", ".join(columns)
        return columns
