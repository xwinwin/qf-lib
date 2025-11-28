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
from cmath import isnan
from numpy import nan
from numpy.testing import assert_almost_equal
from pandas import date_range
from pandas import DatetimeIndex
from qf_lib.common.enums.frequency import Frequency
from qf_lib.common.enums.price_field import PriceField
from qf_lib.common.enums.security_type import SecurityType
from qf_lib.common.tickers.tickers import QLibTicker
from qf_lib.common.utils.dateutils.timer import SettableTimer
from qf_lib.containers.series.prices_series import PricesSeries
from qf_lib.data_providers.qlib.qlib_data_provider import QLibDataProvider
from qf_lib.common.utils.dateutils.string_to_date import str_to_date
from qf_lib.tests.unit_tests.backtesting.portfolio.dummy_ticker import DummyTicker
from qf_lib.tests.unit_tests.data_providers.qlib_data_provider.test_qlib_data_provider_daily import TestQLibDataProviderDaily
from qf_lib.backtesting.events.time_event.regular_time_event.market_close_event import MarketCloseEvent
from qf_lib.backtesting.events.time_event.regular_time_event.market_open_event import MarketOpenEvent
from qf_lib.containers.dataframe.qf_dataframe import QFDataFrame
from qf_lib.containers.qf_data_array import QFDataArray
from qf_lib.containers.series.qf_series import QFSeries
from qf_lib.tests.helpers.testing_tools.containers_comparison import assert_series_equal, assert_dataframes_equal, \
    assert_dataarrays_equal


"""
The purpose of this script is to provide an example of how to use the QLibDataProvider.
"""


def main():

    # qlib.init(provider_uri="~/.qlib/qlib_data/cn_data", region=REG_CN)
    # instruments = D.instruments(market="csi300")
    # instrument_list = D.list_instruments(instruments=instruments, as_list=True, freq="day")
    # df = D.features(instruments=instrument_list, fields=["$open", "$close", "$high", "$low", "$volume"], freq="day")
    # print(df.head())

    # df.reset_index(inplace=True)
    # print(df.head())

    # instruments = ['SH600000'] # 
    # df = D.features(instruments=instruments, fields=["$open", "$close", "$high", "$low", "$volume"], freq="day")
    # print(df.head())

    # field_to_price_field_dict = {
    #         'open': PriceField.Open,
    #         'high': PriceField.High,
    #         'low': PriceField.Low,
    #         'close': PriceField.Close,
    #         'volume': PriceField.Volume,
    #     }

    # fields = ['open', 'high', 'low', 'close', 'volume']

    # data_provider = QLibDataProvider(path="~/.qlib/qlib_data/cn_data", region=REG_CN,
    #                                 # tickers=[QLibTicker("SH600000", SecurityType.STOCK), QLibTicker("SH600004", SecurityType.STOCK)],
    #                                 tickers=[QLibTicker("csi300", SecurityType.INDEX)],
    #                                 field_to_price_field_dict=field_to_price_field_dict,
    #                                 fields=fields)

    end_date = datetime.now()
    start_date = end_date.replace(day=end_date.day - 10)

    fields = ['open', 'high', 'low', 'close', 'volume']
    field_to_price_field_dict = {
        'open': PriceField.Open,
        'high': PriceField.High,
        'low': PriceField.Low,
        'close': PriceField.Close,
        'volume': PriceField.Volume,
    }
    price_fields = PriceField.ohlcv()

    ticker = DummyTicker('SH000300', SecurityType.STOCK)
    # ticker = DummyTicker('HAHA', SecurityType.STOCK)
    tickers = [ticker, DummyTicker('csi300', SecurityType.INDEX), DummyTicker('SH601228', SecurityType.STOCK), DummyTicker('SH600021', SecurityType.STOCK)]

    data_provider = QLibDataProvider(tickers=tickers, field_to_price_field_dict = field_to_price_field_dict,
                                        fields=fields, start_date=start_date, end_date=end_date,frequency=Frequency.DAILY)


    # # Return the latest 5 close prices of the SH600001 stock
    prices = data_provider.historical_price(QLibTicker("SH600001", SecurityType.STOCK), PriceField.Close,
                                           nr_of_bars=5)
    print(prices)

    # # Return the close price of the the CSI 300 index between 2025-01-01 and 2025-01-07
    price = data_provider.get_price(DummyTicker("SH000300", SecurityType.STOCK), PriceField.Close, start_date=start_date, end_date=end_date)
    print(price)


if __name__ == '__main__':
    main()
