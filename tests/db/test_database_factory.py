import unittest

#from db.database_mysql import MySqlDB
from db.database_postgres import PostgresDB
from db.database_factory import DatabaseFactory


class TestDatabaseFactory(unittest.TestCase):
    def test_returned_type_postgres_from_factory(self):
        url_dict={"user":"user", "password":"password", "url": "jdbc:postgresql://machine.syssede.systest.sanpaoloimi.com:5506/dbname"}
        res = DatabaseFactory.get_db(url_dict)
        self.assertIsInstance(res,PostgresDB)

    def test_url_host_component_postgres(self):
        url_dict={"user":"user", "password":"password", "url": "jdbc:postgresql://machine.syssede.systest.sanpaoloimi.com:5506/dbname"}
        res = DatabaseFactory.get_db(url_dict)
        match=res.match_url()
        self.assertEqual(match.group("host"), "machine.syssede.systest.sanpaoloimi.com")

    def test_url_port_component_postgres(self):
        url_dict={"user":"user", "password":"password", "url": "jdbc:postgresql://machine.syssede.systest.sanpaoloimi.com:5506/dbname"}
        res = DatabaseFactory.get_db(url_dict)
        match=res.match_url()
        self.assertEqual(match.group("port"), "5506")

    def test_url_db_component_postgres(self):
        url_dict={"user":"user", "password":"password", "url": "jdbc:postgresql://machine.syssede.systest.sanpaoloimi.com:5506/dbname"}
        res = DatabaseFactory.get_db(url_dict)
        match=res.match_url()
        self.assertEqual(match.group("db"), "dbname")




