import pandas
import pyodbc
import sys

from os.path import join as pjoin
from resources import src


class DatabaseConnection:
    YEAR = src.db_info['year']
    HOUSEHOLD = \
        f"SELECT * FROM [household].[dbo].[getHousehold]({YEAR}, 2)"

    ELDER_ALLOWANCE = f"SELECT * " \
                      f"FROM [elder_allowance].[dbo].[getFarmerIncomeSurveyData]({YEAR})"

    FALLOW_DECLARE = f"SELECT * " \
                     f"FROM [fallow_declare].[dbo].[getFarmerIncomeSurveyData]({YEAR})"

    FALLOW_TRANSFER_SUBSIDY = f"SELECT * " \
                              f"FROM [fallow_transfer_subsidy].[dbo].[getFarmerIncomeSurveyData]({YEAR})"

    CROP_SBDY = \
        """
        SELECT [farmerId], [subName], [period]
        FROM [fallow_transfer_subsidy].[dbo].[108_labor_force_survey_view]
        WHERE [farmerId] in(
            SELECT pid 
            FROM [household].[dbo].[108_labor_force_survey_view]
        )
        """

    DISASTER_SUBSIDY = f"SELECT * "\
                       f"FROM [disaster_subsidy].[dbo].[getFarmerIncomeSurveyData]({YEAR})"

    LIVESTOCK = f"SELECT * "\
                f"FROM [livestock].[dbo].[getFarmerIncomeSurveyData]({YEAR}) " + \
                """
                ORDER BY [farmerId], [investigateYear], [fieldName],
                 CASE
                    WHEN [investigateSeason] = 'M5'
                        THEN 5
                    WHEN [investigateSeason] = 'M11'
                        THEN 11
                    WHEN [investigateSeason] = 'Q1'
                        THEN 1
                    WHEN [investigateSeason] = 'Q2'
                        THEN 2
                    WHEN [investigateSeason] = 'Q3'
                        THEN 3
                    WHEN [investigateSeason] = 'Q4'
                        THEN 4
                END
                """

    SMALL_LARGE_TENANT_TRANSFER = \
        f"SELECT * "\
        f"FROM [small_large_tenant_transfer].[dbo].[getFarmerIncomeSurveyData]({YEAR}) "\
        f"ORDER BY [pid], [period]"

    SMALL_LARGE_LANDLORD_RENT = \
        f"SELECT * " \
        f"FROM [small_large_landlord_rent].[dbo].[getFarmerIncomeSurveyData]({YEAR})" \
        f"ORDER BY [pid], [period]"

    SMALL_LARGE_LANDLORD_RETIRE = \
        f"SELECT * " \
        f"FROM [small_large_landlord_retire].[dbo].[getFarmerIncomeSurveyData]({YEAR})" \
        f"ORDER BY [pid], [period]"

    SMALL_LARGE_TENANT_INFORMATION_TENANT_ID = \
        f"SELECT * " \
        f"FROM [small_large_tenant_information].[dbo].[getFarmerIncomeSurveyTenantId]({YEAR})"

    SMALL_LARGE_TENANT_INFORMATION_LANDLORD_ID = \
        f"SELECT * " \
        f"FROM [small_large_tenant_information].[dbo].[getFarmerIncomeSurveyLandlordId]({YEAR})"

    CHILD_SCHOLARSHIP = \
        f"SELECT * " \
        f"FROM [child_scholarship].[dbo].[getFarmerIncomeSurveyData]({YEAR})"

    __pid = None
    driver = '{ODBC Driver 13 for SQL Server}'
    args = f"DRIVER={driver};" \
           f"SERVER={src.db_info['server']};" \
           f"DATABASE={src.db_info['database']};" \
           f"UID={src.db_info['username']};" \
           f"PWD={src.db_info['password']}"

    __instance = None

    def __init__(self, log=None, err_log=None, clean_cache=False, connect=True):
        """
        資料庫連線物件
        :param log: 紀錄 info
        :param err_log: 紀錄 warning level 以上
        :param clean_cache: 第一次連進資料庫時會將結果自動存成 .csv 檔, 若需要重新抓資料時將此變數設為 True
        :param connect: 其他電腦若沒有資料庫時要設為 False 否則會錯誤
        """
        self.conn = pyodbc.connect(DatabaseConnection.args) if connect else None
        self.cur = self.conn.cursor() if self.conn else None
        self.clean_cache = clean_cache
        self.db_cache = src.db_cache_dir
        self.log = log
        self.err_log = err_log
        self.__pid = None
        self.__db_name = None
        self._db_household = None
        self._db_elder_allowance = None
        self._db_fallow_declare = None
        self._db_fallow_transfer_subsidy = None
        self._db_disaster_subsidy = None
        self._db_livestock = None
        self._db_small_large_tenant_transfer = None
        self._db_small_large_landlord_rent = None
        self._db_small_large_landlord_retire = None
        self._db_small_large_tenant_information_tenant_id = None
        self._db_small_large_tenant_information_landlord_id = None
        self._db_child_scholarship = None
        print('Database has connected ...')

    def __read_db_handler(self, attr, index_col=None) -> pandas.DataFrame:
        db = None
        try:
            db = pandas.read_csv(pjoin(self.db_cache, f"{attr}.csv"), sep=',', index_col=index_col)
        except:
            info = sys.exc_info()
            print(f"{info[0]}, {info[1]}")
            return db
        else:
            return db

    def __write_db_handler(self, attr, data, index=False):
        data.to_csv(pjoin(self.db_cache, f"{attr}.csv"), index=index)

# ################################################### Propertys ###################################################
    @property
    def pid(self):
        return self.__pid

    @pid.setter
    def pid(self, _id):
        self.__pid = _id

    @property
    def db_name(self):
        return f"db_{self.__db_name}"

    @db_name.setter
    def db_name(self, name):
        self.__db_name = name

    @property
    def db_household(self):
        db = self._db_household
        if db is not None:
            return db

        name = self.db_name
        db = self.__read_db_handler(name)
        if db is None or self.clean_cache:
            try:
                print(f"execute sql statment: {DatabaseConnection.HOUSEHOLD}")
                db = pandas.read_sql(DatabaseConnection.HOUSEHOLD, self.conn)
            except:
                info = sys.exc_info()
                print(f"{info[0]}, {info[1]}")
                self.close_conn()
            else:
                self.__write_db_handler(name, db)
                self._db_household = db
        else:
            self._db_household = db
        return db

    @property
    def db_elder_allowance(self):
        db = self._db_elder_allowance
        if db is not None:
            return db

        name = self.db_name
        db = self.__read_db_handler(name, 'pid')
        if db is None or self.clean_cache:
            try:
                print(f"execute sql statment: {DatabaseConnection.ELDER_ALLOWANCE}")
                db = pandas.read_sql(DatabaseConnection.ELDER_ALLOWANCE, self.conn, index_col='pid')
            except:
                info = sys.exc_info()
                print(f"{info[0]}, {info[1]}")
                self.close_conn()
            else:
                self.__write_db_handler(name, db, True)
                self._db_elder_allowance = db
        else:
            self._db_elder_allowance = db
        return db

    @property
    def db_fallow_declare(self) -> pandas.DataFrame.groupby:
        db = self._db_fallow_declare
        if db is not None:
            return db

        name = self.db_name
        db = self.__read_db_handler(name)
        if db is None or self.clean_cache:
            try:
                print(f"execute sql statment: {DatabaseConnection.FALLOW_DECLARE}")
                db = pandas.read_sql(DatabaseConnection.FALLOW_DECLARE, self.conn)
            except:
                info = sys.exc_info()
                print(f"{info[0]}, {info[1]}")
                self.close_conn()
            else:
                self.__write_db_handler(name, db)
                self._db_fallow_declare = db = db.groupby('applicantId')
        else:
            self._db_fallow_declare = db = db.groupby('applicantId')
        return db

    @property
    def db_fallow_transfer_subsidy(self) -> pandas.DataFrame.groupby:
        db = self._db_fallow_transfer_subsidy
        if db is not None:
            return db

        name = self.db_name
        db = self.__read_db_handler(name)
        if db is None or self.clean_cache:
            try:
                print(f"execute sql statment: {DatabaseConnection.FALLOW_TRANSFER_SUBSIDY}")
                db = pandas.read_sql(DatabaseConnection.FALLOW_TRANSFER_SUBSIDY, self.conn)
            except:
                info = sys.exc_info()
                print(f"{info[0]}, {info[1]}")
                self.close_conn()
            else:
                self.__write_db_handler(name, db)
                self._db_fallow_transfer_subsidy = db = db.groupby('farmerId')
        else:
            self._db_fallow_transfer_subsidy = db = db.groupby('farmerId')
        return db

    @property
    def db_disaster_subsidy(self) -> pandas.DataFrame.groupby:
        db = self._db_disaster_subsidy
        if db is not None:
            return db

        name = self.db_name
        db = self.__read_db_handler(name)
        if db is None or self.clean_cache:
            try:
                print(f"execute sql statment: {DatabaseConnection.DISASTER_SUBSIDY}")
                db = pandas.read_sql(DatabaseConnection.DISASTER_SUBSIDY, self.conn)
            except:
                info = sys.exc_info()
                print(f"{info[0]}, {info[1]}")
                self.close_conn()
            else:
                self.__write_db_handler(name, db)
                self._db_disaster_subsidy = db = db.groupby('applicantId')
        else:
            self._db_disaster_subsidy = db = db.groupby('applicantId')
        return db

    @property
    def db_livestock(self) -> pandas.DataFrame.groupby:
        db = self._db_livestock
        if db is not None:
            return db

        name = self.db_name
        db = self.__read_db_handler(name)
        if db is None or self.clean_cache:
            try:
                print(f"execute sql statment: {DatabaseConnection.LIVESTOCK}")
                db = pandas.read_sql(DatabaseConnection.LIVESTOCK, self.conn)
            except:
                info = sys.exc_info()
                print(f"{info[0]}, {info[1]}")
                self.close_conn()
            else:
                self.__write_db_handler(name, db)
                self._db_livestock = db = db.groupby('farmerId')
        else:
            self._db_livestock = db = db.groupby('farmerId')
        return db

    @property
    def db_small_large_tenant_transfer(self) -> pandas.DataFrame.groupby:
        db = self._db_small_large_tenant_transfer
        if db is not None:
            return db

        name = self.db_name
        db = self.__read_db_handler(name)
        if db is None or self.clean_cache:
            try:
                print(f"execute sql statment: {DatabaseConnection.SMALL_LARGE_TENANT_TRANSFER}")
                db = pandas.read_sql(DatabaseConnection.SMALL_LARGE_TENANT_TRANSFER, self.conn)
            except:
                info = sys.exc_info()
                print(f"{info[0]}, {info[1]}")
                self.close_conn()
            else:
                self.__write_db_handler(name, db)
                self._db_small_large_tenant_transfer = db = db.groupby('pid')
        else:
            self._db_small_large_tenant_transfer = db = db.groupby('pid')
        return db

    @property
    def db_small_large_landlord_rent(self) -> pandas.DataFrame.groupby:
        db = self._db_small_large_landlord_rent
        if db is not None:
            return db

        name = self.db_name
        db = self.__read_db_handler(name)
        if db is None or self.clean_cache:
            try:
                print(f"execute sql statment: {DatabaseConnection.SMALL_LARGE_LANDLORD_RENT}")
                db = pandas.read_sql(DatabaseConnection.SMALL_LARGE_LANDLORD_RENT, self.conn)
            except:
                info = sys.exc_info()
                print(f"{info[0]}, {info[1]}")
                self.close_conn()
            else:
                self.__write_db_handler(name, db)
                self._db_small_large_landlord_rent = db = db.groupby('pid')
        else:
            self._db_small_large_landlord_rent = db = db.groupby('pid')
        return db

    @property
    def db_small_large_landlord_retire(self) -> pandas.DataFrame.groupby:
        db = self._db_small_large_landlord_retire
        if db is not None:
            return db

        name = self.db_name
        db = self.__read_db_handler(name)
        if db is None or self.clean_cache:
            try:
                print(f"execute sql statment: {DatabaseConnection.SMALL_LARGE_LANDLORD_RETIRE}")
                db = pandas.read_sql(DatabaseConnection.SMALL_LARGE_LANDLORD_RETIRE, self.conn)
            except:
                info = sys.exc_info()
                print(f"{info[0]}, {info[1]}")
                self.close_conn()
            else:
                self.__write_db_handler(name, db)
                self._db_small_large_landlord_retire = db = db.groupby('pid')
        else:
            self._db_small_large_landlord_retire = db = db.groupby('pid')
        return db

    @property
    def db_small_large_tenant_information_tenant_id(self) -> pandas.DataFrame:
        db = self._db_small_large_tenant_information_tenant_id
        if db is not None:
            return db

        name = self.db_name
        db = self.__read_db_handler(name)
        if db is None or self.clean_cache:
            try:
                print(f"execute sql statment: {DatabaseConnection.SMALL_LARGE_TENANT_INFORMATION_TENANT_ID}")
                db = pandas.read_sql(DatabaseConnection.SMALL_LARGE_TENANT_INFORMATION_TENANT_ID, self.conn)
            except:
                info = sys.exc_info()
                print(f"{info[0]}, {info[1]}")
                self.close_conn()
            else:
                self.__write_db_handler(name, db)
                self._db_small_large_tenant_information_tenant_id = db
        else:
            self._db_small_large_tenant_information_tenant_id = db
        return db

    @property
    def db_small_large_tenant_information_landlord_id(self) -> pandas.DataFrame:
        db = self._db_small_large_tenant_information_landlord_id
        if db is not None:
            return db

        name = self.db_name
        db = self.__read_db_handler(name)
        if db is None or self.clean_cache:
            try:
                print(f"execute sql statment: {DatabaseConnection.SMALL_LARGE_TENANT_INFORMATION_LANDLORD_ID}")
                db = pandas.read_sql(DatabaseConnection.SMALL_LARGE_TENANT_INFORMATION_LANDLORD_ID, self.conn)
            except:
                info = sys.exc_info()
                print(f"{info[0]}, {info[1]}")
                self.close_conn()
            else:
                self.__write_db_handler(name, db)
                self._db_small_large_tenant_information_landlord_id = db
        else:
            self._db_small_large_tenant_information_landlord_id = db
        return db

    @property
    def db_child_scholarship(self) -> pandas.DataFrame.groupby:
        db = self._db_child_scholarship
        if db is not None:
            return db

        name = self.db_name
        db = self.__read_db_handler(name)
        if db is None or self.clean_cache:
            try:
                print(f"execute sql statment: {DatabaseConnection.CHILD_SCHOLARSHIP}")
                db = pandas.read_sql(DatabaseConnection.CHILD_SCHOLARSHIP, self.conn)
            except:
                info = sys.exc_info()
                print(f"{info[0]}, {info[1]}")
                self.close_conn()
            else:
                self.__write_db_handler(name, db)
                self._db_child_scholarship = db = db.groupby('applicantId')
        else:
            self._db_child_scholarship = db = db.groupby('applicantId')
        return db

    # ################################################### Methods ###################################################
    def elder_allowance(self):
        db = self.db_elder_allowance
        try:
            amount = db.loc[self.pid]['amount']
        except KeyError:
            amount = ''
        return amount

    def fallow_declare(self) -> pandas.DataFrame:
        db = self.db_fallow_declare
        try:
            df = db.get_group(self.pid)
        except KeyError:
            return None
        else:
            return df

    def fallow_transfer_subsidy(self) -> pandas.DataFrame:
        db = self.db_fallow_transfer_subsidy
        try:
            df = db.get_group(self.pid)
        except KeyError:
            return None
        else:
            return df

    def disaster_subsidy(self) -> pandas.DataFrame:
        db = self.db_disaster_subsidy
        try:
            df = db.get_group(self.pid)
        except KeyError:
            return None
        else:
            return df

    def livestock(self) -> pandas.DataFrame:
        db = self.db_livestock
        try:
            df = db.get_group(self.pid)
        except KeyError:
            return None
        else:
            return df

    def small_large_tenant_transfer(self) -> pandas.DataFrame:
        db = self.db_small_large_tenant_transfer
        try:
            df = db.get_group(self.pid)
        except KeyError:
            return None
        else:
            return df

    def small_large_landlord_rent(self) -> pandas.DataFrame:
        db = self.db_small_large_landlord_rent
        try:
            df = db.get_group(self.pid)
        except KeyError:
            return None
        else:
            return df

    def small_large_landlord_retire(self) -> pandas.DataFrame:
        db = self.db_small_large_landlord_retire
        try:
            df = db.get_group(self.pid)
        except KeyError:
            return None
        else:
            return df

    def small_large_tenant_information_tenant_id(self) -> pandas.DataFrame:
        return self.db_small_large_tenant_information_tenant_id

    def small_large_tenant_information_landlord_id(self) -> pandas.DataFrame:
        return self.db_small_large_tenant_information_landlord_id

    def child_scholarship(self):
        db = self.db_child_scholarship
        try:
            df = db.get_group(self.pid)
        except KeyError:
            return None
        else:
            return df

    def close_conn(self) -> None:
        self.cur.close()
        self.conn.close()

    @staticmethod
    def get_db_instance(log=None, err_log=None, clean_cache=False, new=False, connect=True):
        if DatabaseConnection.__instance is None or new:
            DatabaseConnection.__instance = DatabaseConnection(log, err_log, clean_cache, connect)
        return DatabaseConnection.__instance


if __name__ == '__main__':
    db_conn = DatabaseConnection.get_db_instance(clean_cache=True)
    a = db_conn.small_large_tenant_transfer()
    print(a)
