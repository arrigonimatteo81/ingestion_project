import unittest

#from db.database_mysql import MySqlDB
from db.database_postgres import PostgresDB
from factories.database_factory import DatabaseFactory


class TestDatabaseFactory(unittest.TestCase):
    def test_returned_type_postgres_from_factory(self):
        url_dict={"user":"user", "password":"password", "url": "jdbc:postgresql://machine.syssede.systest.sanpaoloimi.com:5506/dbname"}
        dbf=DatabaseFactory(url_dict)
        res = dbf.get_db()
        self.assertIsInstance(res,PostgresDB)

    def test_url_host_component_postgres(self):
        url_dict={"user":"user", "password":"password", "url": "jdbc:postgresql://machine.syssede.systest.sanpaoloimi.com:5506/dbname"}
        dbf = DatabaseFactory(url_dict)
        res = dbf.get_db()
        match=res.match_url()
        self.assertEqual(match.group("host"), "machine.syssede.systest.sanpaoloimi.com")

    def test_url_port_component_postgres(self):
        url_dict={"user":"user", "password":"password", "url": "jdbc:postgresql://machine.syssede.systest.sanpaoloimi.com:5506/dbname"}
        dbf = DatabaseFactory(url_dict)
        res = dbf.get_db()
        match=res.match_url()
        self.assertEqual(match.group("port"), "5506")

    def test_url_db_component_postgres(self):
        url_dict={"user":"user", "password":"password", "url": "jdbc:postgresql://machine.syssede.systest.sanpaoloimi.com:5506/dbname"}
        dbf = DatabaseFactory(url_dict)
        res = dbf.get_db()
        match=res.match_url()
        self.assertEqual(match.group("db"), "dbname")


    def test_url_host_component_sqlserver(self):
        url_dict={"user":"user", "password":"password", "url": "jdbc:sqlserver://machine.syssede.systest.sanpaoloimi.com\\INSTANCE:1433;DatabaseName=RDBP0_MENS;encrypt=true;trustServerCertificate=true;integratedSecurity=true;authenticationScheme=NTLM;"}
        dbf = DatabaseFactory(url_dict)
        res = dbf.get_db()
        match=res.match_url()
        self.assertEqual(match.group("host"), "machine.syssede.systest.sanpaoloimi.com")


    def test_url_instance_component_sqlserver(self):
        url_dict={"user":"user", "password":"password", "url": "jdbc:sqlserver://machine.syssede.systest.sanpaoloimi.com\\INSTANCE:1433;DatabaseName=dbname;encrypt=true;trustServerCertificate=true;integratedSecurity=true;authenticationScheme=NTLM;"}
        dbf = DatabaseFactory(url_dict)
        res = dbf.get_db()
        match=res.match_url()
        self.assertEqual(match.group("instance"), "INSTANCE")

    def test_url_port_component_sqlserver(self):
        url_dict={"user":"user", "password":"password", "url": "jdbc:sqlserver://machine.syssede.systest.sanpaoloimi.com\\INSTANCE:1433;DatabaseName=dbname;encrypt=true;trustServerCertificate=true;integratedSecurity=true;authenticationScheme=NTLM;"}
        dbf = DatabaseFactory(url_dict)
        res = dbf.get_db()
        match=res.match_url()
        self.assertEqual(match.group("port"), "1433")

    def test_url_db_component_sqlserver(self):
        url_dict={"user":"user", "password":"password", "url": "jdbc:sqlserver://machine.syssede.systest.sanpaoloimi.com\\INSTANCE:1433;DatabaseName=dbname;encrypt=true;trustServerCertificate=true;integratedSecurity=true;authenticationScheme=NTLM;"}
        dbf = DatabaseFactory(url_dict)
        res = dbf.get_db()
        match=res.match_url()
        self.assertEqual(match.group("db"), "dbname")






