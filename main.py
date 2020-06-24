import functools
import numpy
import re
import time
import pandas

from datetime import date, datetime
from db_conn import DatabaseConnection
from resources import src as default_src
from resources.utils import (
    SimpleLog,
    write_to_json_file
)
from collections import OrderedDict
from os.path import join as pjoin


def timer(func):
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()
        obj = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        m, s = divmod(run_time, 60)

        msg = f"\nexecute time: {int(m)} min, {int(s)} sec, " \
              f"empty id count: {obj.empty_id_count}, " \
              f"invalid id count: {obj.invalid_id_count}, " \
              f"dead count: {obj.death_id_count}"
        print(msg)
        obj.log.info(msg)

    return wrapper_timer


class GenerateOfficialDataOptions:

    def __init__(self, optinos=None):
        self.default_name_dict = getattr(optinos, 'default_name_dict', None)
        self.household_ordered_fields = getattr(optinos, 'household_ordered_fields', None)
        self.ordered_fields = getattr(optinos, 'ordered_fields', None)
        self.sample_info_fields = getattr(optinos, 'sample_info_fields', None)
        self.sample_fields_dict = getattr(optinos, 'sample_fields_dict', None)


class GenerateOfficialDataMetaclass(type):

    def __new__(mcs, name, bases, attrs):
        new_class = super().__new__(mcs, name, bases, attrs)

        if not hasattr(new_class, 'Meta'):
            return new_class

        opts = new_class._meta = GenerateOfficialDataOptions(getattr(new_class, 'Meta', None))
        new_class.sample_fields_dict = opts.sample_fields_dict
        if opts.household_ordered_fields is None:
            raise ValueError(f"'household_ordered_fields' is needed.")

        if opts.ordered_fields is None:
            raise ValueError(f"'ordered_fields' is needed.")

        if opts.default_name_dict:
            new_class.default_name_dict = opts.default_name_dict

        return new_class


class GenerateOfficialData(object, metaclass=GenerateOfficialDataMetaclass):

    def __init__(self, main_flag, src=None):
        self.src = src or default_src
        self.main_flag = main_flag
        self.log = SimpleLog(self.src.log_dir, 'informations')
        self.err_log = SimpleLog(self.src.log_dir, 'errors')
        self._sample_data_frame = None
        self._household_data_frame = None
        self._farmer_health_insurance = None
        self._farmer_occupational_insurance = None
        self._national_pension_insurance_benefits = None
        self._labor_insurance_benefits = None
        self._labor_pension_benefits = None
        self._farmer_health_insurance_benefits = None
        self._hospital_days = None
        self._clinic_times = None
        self._health_insurance_data = None
        self._labor_insurance_payment = None
        self._national_pension_insurance_payment = None
        self._small_large_tenant_id_list = None
        self._small_large_landlord_id_list = None
        self.__sample_id_hhn_mapping_dict = {}
        self.result_dict = {}
        self.crop_set = set()
        self.db_conn = DatabaseConnection.get_db_instance(self.log, self.err_log, connect=False)
        self.all_sample_count = 0
        self.count = 0
        self.empty_id_count = 0
        self.invalid_id_count = 0
        self.death_id_count = 0
        self.parse_int = lambda x: int(x) if type(x) == numpy.int64 else x

    def __generic_database_query(self, age_flag: int, household_members_df: pandas.DataFrame,
                                 target: str, ref_func_name: str) -> pandas.DataFrame:
        if household_members_df is None:
            return None

        # 轉換成民國年的 lambda
        parser = lambda x: int(x.rjust(7, '0')[:3]) if not x.startswith('-') else int(x[:2])
        result_df = None
        db = self.db_conn
        db.db_name = target
        roc = date.today().year
        func = getattr(db, target, None)

        if func is None:
            raise NotImplementedError(f"{self.db_conn.__class__.__name__} has not attr {target}")

        for i in range(len(household_members_df.index)):
            person_series = household_members_df.iloc[i]
            age = roc - parser(str(person_series['birth']))
            if age >= age_flag:
                pid = person_series['pid']
                db.pid = pid
                df = func()

                if df is not None:
                    if result_df is None:
                        result_df = df
                    else:
                        result_df = result_df.append(df)
                    self.log.info(f"'{ref_func_name}' -> id={pid}: {df.to_dict()}")

        return result_df

# ################################################### Propertys ###################################################
    @property
    def sample_data_frame(self) -> pandas.DataFrame:
        """
        讀取 sample, 檔名通常會叫做 '調查名冊.xlsx'
        會分成主備選, 並與戶籍檔 join, 目的是為了找出 sample 的戶號
        :return: pandas.core.frame.DataFrame
        """
        data = self._sample_data_frame
        if data is None:
            print('read sample file ...')
            df = pandas.read_excel(
                pjoin(self.src.input_excel_dir, '調查名冊.xlsx'), dtype={
                    '電話': 'str',
                    '農戶編號': 'str',
                }
            )
            df2 = self.household_data_frame \
                .rename(columns={'pid': 'ID'}) \
                .drop_duplicates(subset='ID')

            # 篩出主選或備選
            _filter = df['主備標記'] == '*' if self.main_flag else df['主備標記'] != '*'
            df = df[_filter]

            # 紀錄 ID 為空的農戶編號
            empty_id = df[df['ID'].isna()]
            if len(empty_id.index) > 0:
                l = len(empty_id.index)
                self.err_log.warning(f"These sample id are empty({l} records): {list(empty_id['農戶編號'])}")

            # 紀錄 ID 重複
            duplicate_id = df[(~df['ID'].isna()) & (df.duplicated(['ID']))]['ID']
            if len(duplicate_id.index) > 0:
                l = len(duplicate_id.index)
                self.err_log.warning(f"These sample id are duplicate({l} records): {list(duplicate_id)}")
            df = df.dropna(subset=['ID']).drop_duplicates(subset='ID').append(empty_id)

            result = df.merge(df2, how='left', on='ID')
            result['戶長姓名'] = result['戶長姓名'].str.replace('\u3000', '').replace(' ', '')
            result['ID'] = result['ID'].str.replace('\u3000', '').replace(' ', '')

            # 紀錄 sample id 在戶籍檔內找不到
            not_exist_id = set(df[~df['ID'].isna()]['ID']) - set(result[~result['householdNumber'].isna()]['ID'])
            if len(not_exist_id) > 0:
                l = len(not_exist_id)
                self.err_log.warning(f"These sample id are not in household({l} records): {not_exist_id}")

            self._sample_data_frame = data = result
        return data

    @property
    def household_data_frame(self) -> pandas.DataFrame:
        """
        讀取戶籍檔
        :return:
        """
        df = self._household_data_frame

        if df is None:
            db = self.db_conn
            db.db_name = 'household'
            df = db.db_household

            # 紀錄戶長已死亡的 ID
            temp_df = df[['pid', 'role', 'annotation']]
            temp_df = temp_df[(temp_df['role'] == '戶長') & (temp_df['annotation'] == '死亡')]['pid']
            if len(temp_df.index) > 0:
                self.err_log.warning(f"These '戶長' was dead in household file: {list(temp_df)}")

            self._household_data_frame = df
        return df

    @property
    def farmer_health_insurance(self) -> list:
        data = self._farmer_health_insurance
        if data is None:
            print('read farmer health insurance file ...')
            df = pandas.read_csv(pjoin(self.src.input_excel_dir, '農保.csv')).groupby('身份證字號')
            _list = list(df.groups.keys())
            self._farmer_health_insurance = data = _list
        return data

    @property
    def farmer_occupational_insurance(self) -> list:
        data = self._farmer_occupational_insurance
        if data is None:
            print('read farmer occupational insurance file ...')
            df = pandas.read_csv(pjoin(self.src.input_excel_dir, '農職.csv'))\
                .groupby('身份證字號')
            _list = list(df.groups.keys())
            self._farmer_occupational_insurance = data = _list
        return data

    @property
    def national_pension_insurance_benefits(self):
        data = self._national_pension_insurance_benefits
        if data is None:
            print(f"read '國保給付' file ...")
            parser = lambda x: datetime.strptime(x, '%Y%m')
            df = pandas.read_excel(
                pjoin(self.src.input_excel_dir, '國保給付1-8月.xlsx'),
                parse_dates=['核付年月'],
                date_parser=parser
            ).groupby('身分證號')

            self._national_pension_insurance_benefits = data = df
        return data

    @property
    def labor_insurance_benefits(self) -> pandas.DataFrame.groupby:
        data = self._labor_insurance_benefits
        if data is None:
            print(f"read '勞保給付' file ...")
            parser = lambda x: datetime.strptime(x, '%Y%m')
            df = pandas.read_excel(
                pjoin(self.src.input_excel_dir, '勞就保給付1-8.xlsx'),
                parse_dates=['核付年月'],
                date_parser=parser
            ).groupby('身分證號')
            self._labor_insurance_benefits = data = df
        return data

    @property
    def labor_pension_benefits(self) -> pandas.DataFrame.groupby:
        data = self._labor_pension_benefits
        if data is None:
            print(f"read '勞退退休金' file ...")
            df = pandas.read_excel(
                pjoin(self.src.input_excel_dir, '勞退1-8月退休金.xlsx')).groupby('身分證')
            self._labor_pension_benefits = data = df
        return data

    @property
    def farmer_health_insurance_benefits(self) -> pandas.DataFrame.groupby:
        data = self._farmer_health_insurance_benefits
        if data is None:
            print(f"read '農保給付' file ...")
            df = pandas.read_excel(
                pjoin(self.src.input_excel_dir, '農保給付1-8月.xlsx')).groupby('身分證字號')
            self._farmer_health_insurance_benefits = data = df
        return data

    @property
    def hospital_days(self) -> pandas.DataFrame:
        data = self._hospital_days
        if data is None:
            print(f"read '住院' file ...")
            df = pandas.read_excel(
                pjoin(self.src.input_excel_dir, '住院.xlsx'), index_col='ID')
            self._hospital_days = data = df
        return data

    @property
    def clinic_times(self) -> pandas.DataFrame:
        data = self._clinic_times
        if data is None:
            print(f"read '門診' file ...")
            df = pandas.read_excel(
                pjoin(self.src.input_excel_dir, '門診.xlsx'), index_col='ID')
            self._clinic_times = data = df
        return data

    @property
    def health_insurance_data(self) -> pandas.DataFrame:
        data = self._health_insurance_data
        if data is None:
            print(f"read '健保應收保險費' file ...")
            df = pandas.read_csv(
                pjoin(self.src.input_excel_dir, '健保應收保險費.csv'))\
                .drop_duplicates(subset='受訪者身分證字號').set_index('受訪者身分證字號')\
                .fillna('')
            self._health_insurance_data = data = df
        return data

    @property
    def labor_insurance_payment(self) -> pandas.DataFrame.groupby:
        data = self._labor_insurance_payment
        if data is None:
            print(f"read '勞就保應計保險費' file ...")
            df = pandas.read_csv(
                pjoin(self.src.input_excel_dir, '勞就保1-8月應計保險費.csv'))\
                .fillna(0).groupby('身分證字號')
            self._labor_insurance_payment = data = df
        return data

    @property
    def national_pension_insurance_payment(self) -> pandas.DataFrame.groupby:
        data = self._national_pension_insurance_payment
        if data is None:
            print(f"read '國保實收保險費' file ...")
            df = pandas.read_csv(
                pjoin(self.src.input_excel_dir, '國保1-8月實收保險費.csv'))\
                .fillna(0).groupby('被保險人IDN')
            self._national_pension_insurance_payment = data = df
        return data

    @property
    def small_large_tenant_id_list(self) -> list:
        data = self._small_large_tenant_id_list
        if data is None:
            conn = self.db_conn
            conn.db_name = 'small_large_tenant_information_tenant_id'
            print(f"\nread '小大承租人ID清單' ...")
            df = list(conn.small_large_tenant_information_tenant_id()['tenantId'])

            self._small_large_tenant_id_list = data = df
        return data

    @property
    def small_large_landlord_id_list(self) -> list:
        data = self._small_large_landlord_id_list
        if data is None:
            conn = self.db_conn
            conn.db_name = 'small_large_tenant_information_landlord_id'
            print(f"read '小大所有權人(地主)ID清單' ...")
            df = list(conn.small_large_tenant_information_landlord_id()['ownerId'])

            self._small_large_landlord_id_list = data = df
        return data

# ################################################### Methods ###################################################
    def create_farmer_num(self, sample_series, *args, **kwargs):
        farmer_num = sample_series['農戶編號']
        return farmer_num

    def create_id(self, sample_series, *args, **kwargs):
        _id = sample_series['ID']
        return _id

    def create_name(self, sample_series, *args, **kwargs):
        name = sample_series['戶長姓名']
        return name

    def create_tel(self, sample_series, *args, **kwargs):
        sample_series = sample_series.fillna(0)
        tel = '0' + sample_series['電話'] \
            if sample_series['電話'] and not sample_series['電話'].startswith('0') \
            else sample_series['電話']

        if sample_series['地址'] == sample_series['addr'] \
                or sample_series['戶長姓名'] == sample_series['name']:
            if sample_series['tel']:
                tel = sample_series['tel']
            if sample_series['phone']:
                tel += '/\n' + sample_series['phone']

        return tel

    def create_addr(self, sample_series, *args, **kwargs):
        addr = sample_series['地址']
        if sample_series['戶長姓名'] == sample_series['name']:
            addr = sample_series['addr']
        return addr

    def create_birth(self, sample_series, *args, **kwargs):
        sample_series = sample_series.fillna(0)
        birth = sample_series['birth']
        if not birth:
            return ''

        birth = str(int(birth))
        if not birth.startswith('-'):
            birth = int(birth.rjust(7, '0')[:3])
        else:
            birth = int(birth[:2])
        return birth

    def create_layer(self, sample_series, *args, **kwargs):
        layer = sample_series['層別']
        return self.parse_int(layer)

    def create_link_num(self, sample_series, *args, **kwargs):
        link_num = str(sample_series['連結編號']).rjust(5, '0')
        return link_num

    def create_household(self, sample_series, household_members_df, fields, *args, **kwargs) -> list:
        if household_members_df is None:
            return []

        def create_birth(series, *args, **kwargs):
            birth = str(series['birth'])
            if not birth.startswith('-'):
                birth = int(birth.rjust(7, '0')[:3])
            else:
                birth = int(birth[:2])
            return birth

        def create_role(series, *args, **kwargs):
            role = series['role']
            return role

            annotation = '' if series['annotation'] == '現住人口' else series['annotation']
            return annotation

        def create_farmer_health_and_occupational_insurance(series, age, *args, **kwargs):
            # 農保至少要 15 歲
            if age < 15:
                return ''

            _id = series['pid']
            # 農保
            l1 = self.farmer_health_insurance
            # 農職
            l2 = self.farmer_occupational_insurance

            _str = 'Y' if _id in l1 else 'N'
            _str += '/Y' if _id in l2 else '/N'
            return _str

        def create_elder_allowance(series, age, *args, **kwargs):
            # 老農津貼至少 65 歲
            if age < 65:
                return ''

            field = kwargs.get('field')
            conn = self.db_conn
            conn.db_name = field
            func = getattr(conn, field, None)

            if func is None:
                raise NotImplementedError(f"{self.db_conn.__class__.__name__} has not attr '{func}'")
            conn.pid = series['pid']
            value = func()

            if value is None:
                raise ValueError(f"'{func.__name__}' must return value.")

            return self.parse_int(value)

        def create_national_pension_insurance_benefits(series, *args, **kwargs):
            df = self.national_pension_insurance_benefits
            _id = series['pid']
            amount = 0
            try:
                # 可能領多種給付, 因此再以給付種類為 group
                category_group = df.get_group(_id).groupby('給付種類')
            except KeyError:
                return ''
            else:
                for key in category_group.groups:
                    group = category_group.get_group(key).sort_values('核付年月')
                    # 60:生育給付, 66:喪葬給付, 此兩種給付為一次性
                    if key == 60 or key == 66:
                        amount += group['金額'].sum()
                    # 其餘都為領一年(12個月)
                    else:
                        # 因月份不一定是從1月開始, 因此抓最後一個月再乘以剩下月份(因為不滿12個月)
                        # e.g. 金額(1-8月加總) + (12 - 8)(剩四個月份) * 金額
                        amount += group['金額'].sum() +\
                                  (12 - group['核付年月'].iloc[-1].month) *\
                                  group['金額'].iloc[-1]
                return self.parse_int(amount)

        def create_labor_insurance_benefits(series, *args, **kwargs):
            df = self.labor_insurance_benefits
            _id = series['pid']
            amount = 0
            try:
                category_group = df.get_group(_id).groupby('給付種類')
            except KeyError:
                return ''
            else:
                for key in category_group.groups:
                    group = category_group.get_group(key).sort_values('核付年月')
                    # 這些類別以外的為一次性給付
                    if key not in [45, 48, 35, 36, 37, 38, 55, 56, 57, 59]:
                        amount += group['金額（元）'].sum()
                    # 領一年(12個月)
                    else:
                        amount += group['金額（元）'].sum() + \
                                  (12 - group['核付年月'].iloc[-1].month) * \
                                  group['金額（元）'].iloc[-1]
                return self.parse_int(amount)

        def create_labor_pension_benefits(series, *args, **kwargs):
            df = self.labor_pension_benefits
            _id = series['pid']
            try:
                group = df.get_group(_id)
            except KeyError:
                return ''
            else:
                amount = group['金額(元)'].sum()
                return self.parse_int(amount)

        def create_farmer_health_insurance_benefits(series, *args, **kwargs):
            df = self.farmer_health_insurance_benefits
            _id = series['pid']
            try:
                group = df.get_group(_id)
            except KeyError:
                return ''
            else:
                amount = group['核付總金額(元)'].sum()
                return self.parse_int(amount)

        def create_hospital_days(series, *args, **kwargs):
            df = self.hospital_days
            _id = series['pid']
            try:
                data = df.loc[_id]
            except KeyError:
                return 0
            else:
                return self.parse_int(data['住院日數'])

        def create_clinic_times(series, *args, **kwargs):
            df = self.clinic_times
            _id = series['pid']
            try:
                data = df.loc[_id]
            except KeyError:
                return 0
            else:
                return self.parse_int(data['1-8月門診件數'])

        def create_health_insurance_type(series, *args, **kwargs):
            df = self.health_insurance_data
            _id = series['pid']
            try:
                data = df.loc[_id]
            except KeyError:
                return ''
            else:
                return self.parse_int(data['本會調查表健保身分別'])

        def create_health_insurance_annotation(series, *args, **kwargs):
            df = self.health_insurance_data
            _id = series['pid']
            try:
                data = df.loc[_id]
            except KeyError:
                return ''
            else:
                return data['被保險人註記']

        def create_dependent_count(series, *args, **kwargs):
            df = self.health_insurance_data
            _id = series['pid']
            try:
                data = df.loc[_id]
            except KeyError:
                return ''
            else:
                return self.parse_int(data['應繳眷口數(人)'])

        def create_health_insurance_payment(series, *args, **kwargs):
            df = self.health_insurance_data
            _id = series['pid']
            try:
                data = df.loc[_id]
            except KeyError:
                return ''
            else:
                return self.parse_int(data['1-8月自付金額'])

        def create_labor_insurance_payment(series, *args, **kwargs):
            df = self.labor_insurance_payment
            _id = series['pid']
            try:
                group = df.get_group(_id).sort_values('保費年月').iloc[-1]
            except KeyError:
                return ''
            else:
                payment = group['被保險人負擔勞保費'] + group['被保險人負擔就保費']
                return int(payment)

        def create_national_pension_insurance_payment(series, *args, **kwargs):
            df = self.national_pension_insurance_payment
            _id = series['pid']
            try:
                group = df.get_group(_id).sort_values('繳費年月').iloc[-1]
            except KeyError:
                return ''
            else:
                payment = group['實收保險費(元)']
                return int(payment)

        def create_id(series, *args, **kwargs):
            return series['pid']

        # 記錄至 log 用
        farmer_num = self.create_farmer_num(sample_series)
        # 回傳此戶內人口的大列表, 每個元素都是戶內的一個人
        members_list = []

        # 迭代戶內人口的 data frame
        for i in range(len(household_members_df.index)):
            person_data_list = []
            person_series = household_members_df.iloc[i]
            # 民國年
            roc = date.today().year - 1911
            age = roc - create_birth(person_series)

            for field in fields:
                func = locals().get(f"create_{field}", None)

                if func is None:
                    raise NotImplementedError(f"'{field}' not implemented 'create_{field}'.")

                value = func(person_series, age, field=field)
                if value is None:
                    raise ValueError(f"'{func.__name__}' must return value.")

                # 個人的資料 list, 最後會被加到大列表
                person_data_list.append(value)

            members_list.append(person_data_list)

        msg = f"'{self.create_household.__name__}' -> {farmer_num}: {members_list}"
        self.log.info(msg)
        return members_list

    def create_fallow_declare(self, sample_series, household_members_df, *args, **kwargs) -> str:
        if household_members_df is None:
            return ''

        attr = kwargs.get('field')

        # 申報年齡至少20歲(通常是戶長申請, 戶長年齡至少要成年)
        df = self.__generic_database_query(
            20, household_members_df, attr, self.create_fallow_declare.__name__
        )
        if df is None:
            return ''
        else:
            crops = set()
            # 前三個稻米判斷面積大於 0 就新增對應的名稱
            # 後三項轉作作物, 任一面積大於 0 就新增
            for i in range(len(df.index)):
                series = df.iloc[i]
                if series['japonicaApproveArea'] > 0:
                    crops.add('梗稻')
                    self.crop_set.add('梗稻')
                if series['indicaApproveArea'] > 0:
                    crops.add('秈稻')
                    self.crop_set.add('秈稻')
                if series['glutinousApproveArea'] > 0:
                    crops.add('糯稻')
                    self.crop_set.add('糯稻')
                if series['approveTransferArea1'] > 0:
                    crops.add(series['approveTransferCrop1'])
                    self.crop_set.add(series['approveTransferCrop1'])
                if series['approveTransferArea2'] > 0:
                    crops.add(series['approveTransferCrop2'])
                    self.crop_set.add(series['approveTransferCrop2'])
                if series['approveTransferArea3'] > 0:
                    crops.add(series['approveTransferCrop3'])
                    self.crop_set.add(series['approveTransferCrop3'])

            return ', '.join(crops)

    def create_fallow_transfer_subsidy(self, sample_series, household_members_df, *args, **kwargs) -> list:
        if household_members_df is None:
            return []

        attr = kwargs.get('field')
        df = self.__generic_database_query(
            20, household_members_df, attr, self.create_fallow_transfer_subsidy.__name__
        )
        if df is None:
            return []
        else:
            _list = []
            # 以期別與作物為分群條件
            df = df.groupby(['period', 'subName'])
            for key in df.groups:
                group = df.get_group(key)
                _list.append({
                    'crop': group['subName'].iloc[-1],
                    # 金額加總
                    'amount': self.parse_int(group['subsidy'].sum()),
                    'period': group['period'].iloc[-1]
                })
                self.crop_set.add(group['subName'].iloc[-1])
            return _list

    def create_disaster_subsidy(self, sample_series, household_members_df, *args, **kwargs) -> list:
        if household_members_df is None:
            return []

        attr = kwargs.get('field')
        # 申請年齡不確定, 因此傳 -1
        df = self.__generic_database_query(
            -1, household_members_df, attr, self.create_disaster_subsidy.__name__
        )

        if df is None:
            return []
        else:
            _list = []
            # 以災害名稱與作物分群
            df = df.groupby(['eventName', 'approveCrop'])
            for key in df.groups:
                group = df.get_group(key)
                _list.append({
                    'event_name': group['eventName'].iloc[-1],
                    'crop': group['approveCrop'].iloc[-1],
                    # 面積加總
                    'area': group['approveArea'].sum(),
                    # 金額加總
                    'amount': self.parse_int(group['subsidyAmount'].sum()),

                })
                self.crop_set.add(group['approveCrop'].iloc[-1])
            return _list

    def create_livestock(self, sample_series, household_members_df, *args, **kwargs) -> dict:
        if household_members_df is None:
            return {}

        _ = self.parse_int
        attr = kwargs.get('field')
        df = self.__generic_database_query(
            18, household_members_df, attr, self.create_livestock.__name__
        )
        if df is None:
            return {}
        else:
            _dict = {}
            groups = df.groupby('farmerId')
            for _id in groups.groups:
                group = groups.get_group(_id)
                for i in range(len(group.index)):
                    series = group.iloc[i]
                    livestock = [None] * 7
                    field_name = series['fieldName']
                    livestock[0] = str(series['investigateYear'])
                    livestock[1] = series['investigateSeason'].strip()
                    livestock[2] = series['animalName']
                    livestock[3] = _(series['raiseCount'])
                    livestock[4] = _(series['slaughterCount'])
                    livestock[5] = None
                    livestock[6] = 0

                    if re.match('^蛋*(雞|鴨|鵝|鵪鶉|鴿)', livestock[2].strip()):
                        # 在養量為零, 且屠宰量不為零 則出清
                        if livestock[3] == 0 and livestock[4] != 0:
                            livestock[3] = '出清'
                        # 底下兩行不確定用意, 有需要再解開註解
                            if livestock[2].strip() != '蛋雞':
                                livestock[4] = ''

                    if series['milkCount'] != 0:
                        livestock[5] = '產乳量\n(公斤)'
                        livestock[6] = _(series['milkCount'])

                    if series['antlerCount'] != 0:
                        livestock[5] = '鹿茸'
                        livestock[6] = _(series['antlerCount'])

                    if series['eggCount'] != 0:
                        livestock[5] = '產蛋量\n(千個)'
                        livestock[6] = series['eggCount']

                    if field_name in _dict:
                        _dict.get(field_name).append(livestock)
                    else:
                        livestock_data = [livestock]
                        _dict[field_name] = livestock_data
            return _dict

    def create_small_large_data(self, sample_series, household_members_df, *args, **kwargs) -> list:
        if household_members_df is None:
            return []

        _list = []

        # 依照此列表裡的順序執行 func, 沒實作會拋出 Exception
        fields = ['small_large_tenant_transfer',
                  'small_large_landlord_rent',
                  'small_large_landlord_retire', ]

        def get_data_frame(series, attr) -> pandas.DataFrame:
            conn = self.db_conn
            conn.db_name = attr
            conn.pid = series['pid']
            func = getattr(conn, attr, None)
            if func is None:
                raise NotImplementedError(f"{self.db_conn.__class__.__name__} has not attr {attr}")
            return func()

        def create_small_large_tenant_transfer(series, attr) -> pandas.DataFrame:
            df = get_data_frame(series, attr)
            return df

        def create_small_large_landlord_rent(series, attr) -> pandas.DataFrame:
            df = get_data_frame(series, attr)
            return df

        def create_small_large_landlord_retire(series, attr) -> pandas.DataFrame:
            df = get_data_frame(series, attr)
            return df

        def landlord_or_tenant(_id) -> str:
            _str = ''
            tenant_list = self.small_large_tenant_id_list
            landlord_list = self.small_large_landlord_id_list
            _str += '小地主' if _id in landlord_list else ''
            if _id in tenant_list:
                _str += '/大專業農' if len(_str) > 0 else '大專業農'
            return _str

        # 因為將一期與二期資料分成兩個 dict 放, 因此要判斷，回傳判斷後的 dict
        judge_by_period = lambda x, y, z: y if x.find('2期') != -1 else z
        # 只有戶長會有姓名，其餘的人則顯示 '與戶長關係' e.g. 妻、父、母
        judge_by_id = lambda x, y: x['戶長姓名'] if x['ID'] == y['pid'] else y['role']

        for i in range(len(household_members_df.index)):
            d1 = OrderedDict({
                'name_or_role': '',
                'small_large_tenant_transfer': '',
                'small_large_landlord_rent': '',
                'small_large_landlord_retire': '',
                'period': '',
                'landlord_or_tenant': '',
            })
            d2 = d1.copy()
            person_series = household_members_df.iloc[i]

            for field in fields:
                func = locals().get(f"create_{field}", None)

                if func is None:
                    raise NotImplementedError(f"'{field}' not implemented 'create_{field}'.")

                df = func(person_series, field)
                if df is not None:
                    for j in range(len(df.index)):
                        series = df.iloc[j]
                        self.log.info(f"'{field}' -> id={series['pid']}: {df.to_dict()}")
                        _dict = judge_by_period(series['period'], d1, d2)
                        _dict['name_or_role'] = judge_by_id(sample_series, person_series)
                        _dict[field] = self.parse_int(series['subsidy'])
                        _dict['period'] = series['period']
                        _dict['landlord_or_tenant'] = landlord_or_tenant(series['pid'])

            _list.append(d1) if d1['period'] else ...
            _list.append(d2) if d2['period'] else ...

        return _list

    def create_crop_name(self, *args, **kwargs) -> str:
        result = ', '.join(self.crop_set)
        self.crop_set.clear()
        return result

    def create_child_scholarship(self, sample_series, household_members_df, *args, **kwargs) -> str:
        if household_members_df is None:
            return ''

        attr = kwargs.get('field')
        # 父母應該至少要有30歲(因為小孩15歲以上才能申請獎學金(高中))
        df = self.__generic_database_query(
            30, household_members_df, attr, self.create_child_scholarship.__name__
        )
        if df is None:
            return ''
        else:
            _list = []
            groups = df.groupby('studentName')
            for name in groups.groups:
                group = groups.get_group(name)
                # format is: 陳XX-6500
                _list.append(f"{group.iloc[-1]['studentName']}-{str(group['amount'].sum())}")

        return ', '.join(_list)

    @timer
    def generate_official_data(self):
        """
        主要產生資料的進入點 func
        :return:
        """
        opts = self._meta
        update_df = ConcreteGenerateOfficialData.read_update_file()
        sample_data_frame = self.sample_data_frame.merge(update_df, how='left', on='ID')

        household_data_frame = self.household_data_frame.groupby('householdNumber')
        self.all_sample_count = len(sample_data_frame.index)

        for i in range(len(sample_data_frame.index)):
            sample_series = sample_data_frame.iloc[i]
            temp_data_dict = {}

            # 排除家庭收支調查的農戶編號
            if sample_series['農戶編號'] in ConcreteGenerateOfficialData.EXCEPT_LIST:
                continue

            # 取得整戶的人
            try:
                household_members_df = household_data_frame.get_group(sample_series['householdNumber'])
            except KeyError:
                household_members_df = None

            for field in opts.ordered_fields:
                func = getattr(self, f"create_{field}", None)
                if func is None:
                    raise NotImplementedError(f"{field} not implemented 'create_{field}'.")

                value = func(
                    sample_series, household_members_df, opts.household_ordered_fields, field=field
                )
                if value is None:
                    raise ValueError(f"'{func.__name__}' must return value.")

                temp_data_dict[field] = value

            self.add_to_result_dict(sample_series, temp_data_dict)
            temp_data_dict.clear()

            self.count += 1
            print(f"\r{self.count} / {self.all_sample_count} ...", end='', flush=True)

        return self

    def add_to_result_dict(self, sample_series, data_dict):
        farmer_num = self.create_farmer_num(sample_series)

        ordered_dict = OrderedDict()
        ordered_dict['sample_info'] = OrderedDict()

        for field in self._meta.ordered_fields:
            if field in self._meta.sample_info_fields:
                ordered_dict['sample_info'][field] = data_dict[field]
            else:
                ordered_dict[field] = data_dict[field]

        if farmer_num not in self.result_dict:
            self.result_dict[farmer_num] = ordered_dict
        else:
            msg = f"{farmer_num} is duplicate in official data."
            print(msg)
            self.err_log.warning(msg)

    @staticmethod
    def household_to_excel(file_name, year):
        _dir = default_src.output_excel_dir

        path1 = pjoin(_dir, file_name)
        conn = DatabaseConnection.get_db_instance().conn
        sql = f"SELECT * " \
              f"FROM [household].[dbo].[getCompletedHousehold]({year}, 2)"
        df = pandas.read_sql(sql, conn)
        writer = pandas.ExcelWriter(path1)
        df.to_excel(writer, encoding='utf8', index=False)
        writer.save()

        path2 = pjoin(_dir, '身分證號給勞動部與衛福部.xlsx')
        df_pid = df['身分證號']
        writer = pandas.ExcelWriter(path2)
        df_pid.to_excel(writer, encoding='utf8', index=False)
        writer.save()
        conn.close()

    @staticmethod
    def generate_difference_file():
        _dir = default_src.input_excel_dir
        file = '108對照用調查名冊.xlsx'
        sheet = '108年主力農家主備選樣本名冊'

        left_df = pandas \
            .read_excel(pjoin(_dir, file), sheet_name=sheet) \
            .dropna(subset=['ID']) \
            .drop_duplicates(subset='ID')['ID']

        file = '104-107.xlsx'
        right_df = pandas \
            .read_excel(pjoin(_dir, file)) \
            .rename(columns={'戶長ID': 'ID'}) \
            .dropna(subset=['ID']) \
            .drop_duplicates(subset='ID')[['ID', '戶長姓名', '連絡電話', '手機號碼', '地址']]
        right_df['戶長姓名'] = right_df['戶長姓名'].str.replace('\u3000', '').replace(' ', '')

        file = 'update.xlsx'
        result = pandas.merge(left_df, right_df, on='ID', validate='1:1')
        writer = pandas.ExcelWriter(pjoin(_dir, file))
        result.to_excel(writer, encoding='utf8', index=False)
        writer.save()

    @staticmethod
    def read_update_file() -> pandas.DataFrame:
        _dir = default_src.input_excel_dir
        file = 'update.xlsx'
        df = pandas.read_excel(pjoin(_dir, file), dtype={
            '連絡電話': 'str',
            '手機號碼': 'str',
        }).rename(columns={'戶長姓名': 'name', '連絡電話': 'tel', '手機號碼': 'phone', '地址': 'addr', })

        return df

    @staticmethod
    def id_number_valid(_id) -> bool:
        """
        檢查此ID是否是有效的
        :param _id: To check id
        :return: bool
        """
        mapping = {
            'A': 10, 'B': 11, 'C': 12, 'D': 13, 'E': 14, 'F': 15, 'G': 16, 'H': 17,
            'I': 34, 'J': 18, 'K': 19, 'L': 20, 'M': 21, 'N': 22, 'O': 35, 'P': 23,
            'Q': 24, 'R': 25, 'S': 26, 'T': 27, 'U': 28, 'V': 29, 'W': 32, 'X': 30,
            'Y': 31, 'Z': 33
        }
        l = []

        for index, i in enumerate(_id):
            if index == 0:
                alphabet = mapping[i]
                l.append(int(alphabet / 10))
                l.append(alphabet % 10)
            else:
                l.append(int(i))

        multiply_factor = [1, 9, 8, 7, 6, 5, 4, 3, 2, 1, 1]
        result = sum([i * j for i, j in zip(l, multiply_factor)]) % 10
        return result % 10 == 0


# ################################################### Concrete Class ###################################################
class ConcreteGenerateOfficialData(GenerateOfficialData):
    EXCEPT_LIST = ['100100511360', '100091101891', '100072412291', '100130513421', '640230005712',
                   '100091101723', '100101803573', '100080408533', '660190009173', '100080210033',
                   '660270007154', '640310001685', '100140902855', '100151007255', '100090901786',
                   '100080205626']

    class Meta:
        household_ordered_fields = [
            'birth', 'role', 'annotation', 'farmer_health_and_occupational_insurance',
            'elder_allowance', 'national_pension_insurance_benefits', 'labor_insurance_benefits',
            'labor_pension_benefits', 'farmer_health_insurance_benefits', 'hospital_days', 'clinic_times',
            'health_insurance_type', 'health_insurance_annotation', 'dependent_count',
            'health_insurance_payment', 'labor_insurance_payment', 'national_pension_insurance_payment',
            'id'
        ]

        ordered_fields = [
            'farmer_num', 'id', 'name', 'birth', 'layer', 'link_num', 'tel', 'addr', 'household',
            'fallow_declare', 'fallow_transfer_subsidy', 'disaster_subsidy', 'livestock',
            'small_large_data', 'crop_name', 'child_scholarship',
        ]

        sample_info_fields = ['farmer_num', 'id', 'name', 'birth', 'layer', 'link_num', 'tel', 'addr', ]


if __name__ == '__main__':
    # 前置作業
    # ConcreteGenerateOfficialData.household_to_excel('戶籍檔.xlsx', default_src.dir_year)
    # ConcreteGenerateOfficialData.generate_difference_file()

    _input = input('選擇產生公務資料\n主選 -> 0\n備選 -> 1:')
    generator = ConcreteGenerateOfficialData(not bool(int(_input)))
    generator.generate_official_data()
    prefix = f"主選" if generator.main_flag else f"備選"
    write_to_json_file(
        pjoin(default_src.output_json_dir, f"{prefix}_公務資料.json"), generator.result_dict)
