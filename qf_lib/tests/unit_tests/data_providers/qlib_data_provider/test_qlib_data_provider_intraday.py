#     Copyright 2016-present CERN – European Organization for Nuclear Research
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
from datetime import datetime
from unittest import TestCase
import pandas as pd
from qf_lib.common.enums.frequency import Frequency
from qf_lib.common.enums.price_field import PriceField
from qf_lib.common.enums.security_type import SecurityType
from qf_lib.common.utils.dateutils.relative_delta import RelativeDelta
from qf_lib.containers.dataframe.prices_dataframe import PricesDataFrame
from qf_lib.containers.dataframe.qf_dataframe import QFDataFrame
from qf_lib.containers.qf_data_array import QFDataArray
from qf_lib.containers.series.prices_series import PricesSeries
from qf_lib.containers.series.qf_series import QFSeries
from qf_lib.data_providers.qlib.qlib_data_provider import QLibDataProvider
from qf_lib.tests.unit_tests.backtesting.portfolio.dummy_ticker import DummyTicker


class TestQLibDataProviderIntraday(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.start_date = datetime(2020, 12, 28)
        cls.end_date = datetime(2020, 12, 31)

        cls.fields = ['open', 'high', 'low', 'close', 'volume']
        cls.field_to_price_field_dict = {
            'open': PriceField.Open,
            'high': PriceField.High,
            'low': PriceField.Low,
            'close': PriceField.Close,
            'volume': PriceField.Volume,
        }
        cls.price_fields = PriceField.ohlcv()

        cls.ticker = DummyTicker('SH000300', SecurityType.STOCK)
        cls.tickers = [cls.ticker, DummyTicker('SH601228', SecurityType.STOCK), DummyTicker('SH600021', SecurityType.STOCK)]

        cls.data_provider = QLibDataProvider(tickers=cls.tickers, field_to_price_field_dict = cls.field_to_price_field_dict,
                                            fields=cls.fields, start_date=cls.start_date, end_date=cls.end_date,frequency=Frequency.DAILY)

    def test_get_price_single_ticker_many_fields(self):
        prices = self.data_provider.get_price(self.ticker, self.price_fields, self.start_date, self.end_date, Frequency.DAILY)

        self.assertEqual(type(prices), PricesDataFrame)
        self.assertEqual(prices.shape[1], len(self.price_fields))
        self.assertEqual(Frequency.infer_freq(prices.index), Frequency.DAILY)

    def test_agg_get_price_single_ticker_many_fields(self):
        prices = self.data_provider.get_price(self.ticker, self.price_fields, self.start_date, self.end_date, Frequency.DAILY)

        self.assertEqual(type(prices), PricesDataFrame)
        self.assertEqual(prices.shape[1], len(self.price_fields))
        self.assertEqual(Frequency.infer_freq(prices.index), Frequency.DAILY)

    def test_get_history_single_ticker_many_fields(self):
        df = self.data_provider.get_history(self.ticker, self.fields, self.start_date, self.end_date, Frequency.DAILY)

        self.assertEqual(type(df), QFDataFrame)
        self.assertEqual(df.shape[1], len(self.price_fields))
        self.assertEqual(Frequency.infer_freq(df.index), Frequency.DAILY)

    def test_get_price_single_ticker_many_fields_single_date(self):
        date = datetime(2020, 12, 30, 1, 5)

        prices = self.data_provider.get_price(self.ticker, self.price_fields, date, date, Frequency.DAILY)

        self.assertEqual(type(prices), PricesSeries)
        self.assertEqual(prices.shape, (len(self.price_fields),))

    def test_agg_get_price_single_ticker_many_fields_single_date(self):
        date = datetime(2020, 12, 30, 1, 15)

        prices = self.data_provider.get_price(self.ticker, self.price_fields, date, date + RelativeDelta(minutes=1),
                                              Frequency.DAILY)

        self.assertEqual(type(prices), PricesSeries)
        self.assertEqual(prices.shape, (len(self.price_fields),))

    def test_get_history_single_ticker_many_fields_single_date(self):
        date = datetime(2020, 12, 30, 1, 5)

        df = self.data_provider.get_history(self.ticker, self.fields, date, date, Frequency.DAILY)

        self.assertEqual(type(df), QFSeries)
        self.assertEqual(df.shape, (len(self.fields),))

    def test_get_price_single_ticker_single_field_many_dates(self):
        prices = self.data_provider.get_price(self.ticker, PriceField.Close, self.start_date, self.end_date,
                                              Frequency.DAILY)

        self.assertEqual(type(prices), PricesSeries)

    def test_agg_get_price_single_ticker_single_field_many_dates(self):
        prices = self.data_provider.get_price(self.ticker, PriceField.Close, self.start_date, self.end_date,
                                              Frequency.DAILY)

        self.assertEqual(type(prices), PricesSeries)

    def test_get_history_single_ticker_single_field_many_dates(self):
        df = self.data_provider.get_history(self.ticker, 'close', self.start_date, self.end_date,
                                            Frequency.DAILY)

        self.assertEqual(type(df), QFSeries)

    def test_get_price_single_ticker_single_field_single_date(self):
        date = datetime(2020, 12, 30, 1, 5)

        prices = self.data_provider.get_price(self.ticker, PriceField.Close, date, date, Frequency.DAILY)

        self.assertEqual(type(prices), float)
        self.assertEqual(prices, 5113.71044921875)

    def test_agg_get_price_single_ticker_single_field_single_date(self):
        date = datetime(2020, 12, 30, 1, 5)

        prices = self.data_provider.get_price(self.ticker, PriceField.Close, date, date, Frequency.DAILY)

        self.assertEqual(type(prices), float)
        self.assertEqual(prices, 5113.71044921875)

    def test_get_history_single_ticker_single_field_single_date(self):
        date = datetime(2020, 12, 30, 1, 5)

        df = self.data_provider.get_history(self.ticker, 'close', date, date, Frequency.DAILY)

        self.assertEqual(type(df), float)
        self.assertEqual(df, 5113.71044921875)

    def test_get_price_many_tickers_many_fields_many_dates(self):
        prices = self.data_provider.get_price(self.tickers, self.price_fields, self.start_date, self.end_date, Frequency.DAILY)

        self.assertEqual(type(prices), QFDataArray)
        self.assertEqual(Frequency.infer_freq(pd.to_datetime(prices.dates.values)), Frequency.DAILY)

    def test_agg_get_price_many_tickers_many_fields_many_dates(self):
        prices = self.data_provider.get_price(self.tickers, self.price_fields, self.start_date, self.end_date, Frequency.DAILY)

        self.assertEqual(type(prices), QFDataArray)
        self.assertEqual(Frequency.infer_freq(pd.to_datetime(prices.dates.values)), Frequency.DAILY)

    def test_get_history_many_tickers_many_fields_many_dates(self):
        df = self.data_provider.get_history(self.tickers, self.fields, self.start_date, self.end_date, Frequency.DAILY)

        self.assertEqual(type(df), QFDataArray)
        self.assertEqual(Frequency.infer_freq(pd.to_datetime(df.dates.values)), Frequency.DAILY)

    def test_get_price_many_tickers_single_field_many_dates(self):
        prices = self.data_provider.get_price(self.tickers, PriceField.Close, self.start_date, self.end_date, Frequency.DAILY)

        self.assertEqual(type(prices), PricesDataFrame)
        self.assertEqual(Frequency.infer_freq(pd.to_datetime(prices.index)), Frequency.DAILY)

    def test_agg_get_price_many_tickers_single_field_many_dates(self):
        prices = self.data_provider.get_price(self.tickers, PriceField.Close, self.start_date, self.end_date, Frequency.DAILY)

        self.assertEqual(type(prices), PricesDataFrame)
        self.assertEqual(Frequency.infer_freq(pd.to_datetime(prices.index)), Frequency.DAILY)

    def test_get_history_many_tickers_single_field_many_dates(self):
        df = self.data_provider.get_history(self.tickers, 'close', self.start_date, self.end_date, Frequency.DAILY)

        self.assertEqual(type(df), QFDataFrame)
        self.assertEqual(Frequency.infer_freq(pd.to_datetime(df.index)), Frequency.DAILY)

    def test_get_price_many_tickers_many_fields_single_date(self):
        date = datetime(2020, 12, 30, 1, 5)
        prices = self.data_provider.get_price(self.tickers, self.price_fields, date, date, Frequency.DAILY)

        self.assertEqual(type(prices), PricesDataFrame)
        self.assertEqual(prices.shape, (len(self.tickers), len(self.price_fields)))

    def test_agg_get_price_many_tickers_many_fields_single_date(self):
        date = datetime(2020, 12, 30, 1, 15)
        prices = self.data_provider.get_price(self.tickers, self.price_fields, date, date + RelativeDelta(minutes=1),
                                              Frequency.DAILY)

        self.assertEqual(type(prices), PricesDataFrame)
        self.assertEqual(prices.shape, (len(self.tickers), len(self.price_fields)))

    def test_get_history_many_tickers_many_fields_single_date(self):
        date = datetime(2020, 12, 30, 1, 5)
        df = self.data_provider.get_history(self.tickers, self.fields, date, date, Frequency.DAILY)

        self.assertEqual(type(df), QFDataFrame)
        self.assertEqual(df.shape, (len(self.tickers), len(self.fields)))

    def test_get_price_many_tickers_single_field_single_date(self):
        date = datetime(2020, 12, 30, 1, 5)
        prices = self.data_provider.get_price(self.tickers, PriceField.Close, date, date, Frequency.DAILY)

        self.assertEqual(type(prices), PricesSeries)
        self.assertEqual(prices.shape, (len(self.tickers),))

    def test_agg_get_price_many_tickers_single_field_single_date(self):

        date = datetime(2020, 12, 30, 1, 15)
        prices = self.data_provider.get_price(self.tickers, PriceField.Close, date, date + RelativeDelta(minutes=1),
                                              Frequency.DAILY)

        self.assertEqual(type(prices), PricesSeries)
        self.assertEqual(prices.shape, (len(self.tickers),))

    def test_get_history_many_tickers_single_field_single_date(self):
        date = datetime(2020, 12, 30, 1, 5)
        df = self.data_provider.get_history(self.tickers, 'close', date, date, Frequency.DAILY)

        self.assertEqual(type(df), QFSeries)
        self.assertEqual(df.shape, (len(self.tickers),))
