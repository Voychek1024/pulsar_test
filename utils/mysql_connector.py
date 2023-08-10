import os
import sys
import mysql.connector
from mysql.connector import Error


class MySQLConnector:
    def __init__(self, database_, user_, host_, port_, password_):
        self.connection = None
        self.cursor = None
        self.tables = []
        self._max_allowed_packet = 0
        self.mysql_connector(database_, user_, host_, port_, password_)
        self.get_spec()

    def mysql_connector(self, database_, user_, host_, port_, password_):
        try:
            self.connection = mysql.connector.connect(host=host_,
                                                      database=database_,
                                                      user=user_,
                                                      port=port_,
                                                      password=password_)
        except Error as err:
            print(err)
            if self.connection is not None:
                if self.connection.is_connected():
                    self.connection.close()
                self.connection = None

    def health_check(self) -> bool:
        if self.connection is not None:
            if self.connection.is_connected():
                return True
        return False

    def get_spec(self):
        if self.health_check():
            self.cursor = self.connection.cursor()
            self.cursor.execute("show variables like 'max_allowed_packet';")
            result = self.cursor.fetchall()
            self._max_allowed_packet = int(result[0][-1])

    def get_tables(self):
        if self.health_check():
            self.tables.clear()
            self.cursor = self.connection.cursor()
            self.cursor.execute("show tables;")
            record = self.cursor.fetchall()
            for row in record:
                self.tables.append(row[0])
            return self.tables
        else:
            return None

    def shutdown(self):
        if self.cursor is not None:
            self.cursor.close()
        if self.health_check():
            self.connection.close()

    @property
    def max_allowed_packet(self):
        return self._max_allowed_packet


def normal_call():
    host_, port_, user_, password_, database_ = None, None, None, None, None
    try:
        if sys.argv[1] == "OS_ENV":
            host_ = os.getenv("WSL_HOST")
            port_, user_, password_, database_ = os.getenv("PULSAR_DB_VALUES").split(" ")
            print(f"Connecting MySQL database at %s using OS_ENV" % host_)
        connector = MySQLConnector(database_=database_ if database_ is not None else sys.argv[5],
                                   user_=user_ if user_ is not None else sys.argv[3],
                                   host_=host_ if host_ is not None else sys.argv[1],
                                   port_=port_ if port_ is not None else sys.argv[2],
                                   password_=password_ if password_ is not None else sys.argv[4])
        connector.get_tables()
        print('\n'.join(connector.tables))
        return connector
    except IndexError:
        print("Invalid argv!\nUsage: python mysql_connector.py <host> <port> <user> <password> <database>")
        return None


if __name__ == '__main__':
    normal_call()
