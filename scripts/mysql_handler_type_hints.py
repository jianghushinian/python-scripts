"""
MySQL handler for type hints
"""
import logging
from contextlib import contextmanager
from typing import Optional, List

import pymysql
from pymysql.constants import CLIENT

# Custom MySQL config
DB_HOST = '127.0.0.1'
DB_PORT = 3306
DB_USER = 'user'
DB_PASS = 'password'
DB_NAME = 'test'
DB_CHARSET = 'utf8mb4'


class FetchObject(object):
    """MySQL fetch object

    This class needs to use setattr() to dynamically set attributes.
    """


class MySQLHandler(object):
    """MySQL handler"""

    def __init__(self):
        self.conn = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            charset=DB_CHARSET,
            client_flag=CLIENT.MULTI_STATEMENTS,  # execute multi sql statements
        )
        self.cursor = self.conn.cursor()

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    @contextmanager
    def execute(self):
        try:
            yield self.cursor.execute
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logging.exception(e)

    @contextmanager
    def executemany(self):
        try:
            yield self.cursor.executemany
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logging.exception(e)

    def _tuple_to_object(self, data: List[tuple]) -> List[FetchObject]:
        obj_list = []
        attrs = [desc[0] for desc in self.cursor.description]
        for i in data:
            obj = FetchObject()
            for attr, value in zip(attrs, i):
                setattr(obj, attr, value)
            obj_list.append(obj)
        return obj_list

    def fetchone(self) -> Optional[FetchObject]:
        result = self.cursor.fetchone()
        return self._tuple_to_object([result])[0] if result else None

    def fetchmany(self, size: Optional[int] = None) -> Optional[List[FetchObject]]:
        result = self.cursor.fetchmany(size)
        return self._tuple_to_object(result) if result else None

    def fetchall(self) -> Optional[List[FetchObject]]:
        result = self.cursor.fetchall()
        return self._tuple_to_object(result) if result else None


def example():
    db = MySQLHandler()

    # create table
    sql = """
        DROP TABLE IF EXISTS `user`;
        CREATE TABLE `user` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `name` varchar(255) DEFAULT NULL,
          `age` int(11) DEFAULT NULL,
          PRIMARY KEY (`id`)
        );
    """
    with db.execute() as execute:
        execute(sql)

    # insert
    sql = """
        INSERT INTO user (name, age) VALUES (%(name)s, %(age)s);
    """
    with db.execute() as execute:
        execute(sql, {'name': 'tim', 'age': 18})

    # update
    sql = """
        UPDATE user SET age = %(age)s;
    """
    with db.execute() as execute:
        execute(sql, {'age': 20})

    # select
    sql = """
        SELECT id, name, age FROM user;
    """
    with db.execute() as execute:
        execute(sql)
    res = db.fetchone()
    print(res.id, res.name, res.age)

    # delete
    sql = """
        DELETE FROM user WHERE name = %(name)s;
    """
    with db.execute() as execute:
        execute(sql, {'name': 'tim'})


if __name__ == '__main__':
    example()
