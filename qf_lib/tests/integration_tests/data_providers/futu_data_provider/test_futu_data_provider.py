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
from cmath import isnan
from datetime import datetime

import pytest
from numpy.testing import assert_almost_equal
from pandas import date_range, DatetimeIndex

from qf_lib.backtesting.events.time_event.regular_time_event.market_close_event import MarketCloseEvent
from qf_lib.backtesting.events.time_event.regular_time_event.market_open_event import MarketOpenEvent
from qf_lib.common.enums.frequency import Frequency
from qf_lib.common.utils.dateutils.string_to_date import str_to_date
from qf_lib.common.utils.dateutils.timer import SettableTimer
from qf_lib.containers.dataframe.qf_dataframe import QFDataFrame
from qf_lib.containers.qf_data_array import QFDataArray
from qf_lib.containers.series.qf_series import QFSeries
from qf_lib.data_providers.futu.futu_data_provider import FutuDataProvider, is_futu_installed
from qf_lib.common.tickers.tickers import FutuTicker
from qf_lib.tests.helpers.testing_tools.containers_comparison import assert_series_equal, assert_dataframes_equal, \
    assert_dataarrays_equal


@pytest.fixture
def data_provider():
    return FutuDataProvider()


def assert_equal(result, expected_value, decimals=2):
    expected_type = type(expected_value)
    assert isinstance(result, expected_type), f"Expected type {expected_type}, but got {type(result)}"

    if isinstance(result, float):
        if isnan(result):
            assert isnan(expected_value)
        else:
            assert_almost_equal(expected_value, result, decimal=decimals)
    elif isinstance(result, QFSeries):
        assert_series_equal(expected_value, result, check_names=False, check_index_type=False)
    elif isinstance(result, QFDataFrame):
        assert_dataframes_equal(expected_value, result, check_names=False, check_index_type=False)
    elif isinstance(result, QFDataArray):
        assert_dataarrays_equal(expected_value, result, check_names=False, check_index_type=False)


@pytest.mark.skipif(not is_futu_installed, reason="requires futu openapi")
@pytest.mark.parametrize(
    "tickers, fields, start_date, end_date, expected_values",
    [
        ("HK.800700", "close", "2025-11-13", "2025-11-13", 5981.30),
        # ("HK.800700", "close", "2025-11-13", "2025-11-13", QFSeries([5981.30], index=["close"])),
        (["HK.800700"], "close", "2025-11-13", "2025-11-13", QFSeries([5981.30], index=[FutuTicker("HK.800700")])),
        ("HK.800700", "close", "2025-11-12", "2025-11-14",
            QFSeries([5933.99, 5981.30, 5812.80],
                        index=date_range("2025-11-12", "2025-11-14", freq="B"))),
        (["HK.800700"], "close", "2025-11-12", "2025-11-14",
            QFDataFrame({FutuTicker("HK.800700"): [5933.99, 5981.30, 5812.80]},
                        index=date_range("2025-11-12", "2025-11-14", freq="B"))),
        ("HK.800700", "close", "2025-11-12", "2025-11-14",
            QFSeries([5933.99, 5981.30, 5812.80],
                        index=date_range("2025-11-12", "2025-11-14", freq="B"))),
        # (["HK.800700"], "close", "2025-11-12", "2025-11-14",
        #     QFDataArray.create(date_range("2025-11-12", "2025-11-14", freq="B"),
        #                 [FutuTicker("HK.800700")], ["close"],
        #                 [[[5933.99]], [[5981.30]], [[5812.80]]])),
        (["HK.800700", "SH.000300"], "close", "2025-11-13", "2025-11-13",
            QFSeries([5981.3000, 4702.0738],
                        index=[FutuTicker("HK.800700"), FutuTicker("SH.000300")])),
        (["HK.800700", "SH.000300"], "close", "2025-11-13", "2025-11-14",
            QFDataFrame({FutuTicker("HK.800700"): [5981.30, 5812.80], FutuTicker("SH.000300"): [4702.0738, 4628.1398]},
                        index=date_range("2025-11-13", "2025-11-14", freq="B"))),
        (["HK.800700", "HK.800700"], "close", "2025-11-13", "2025-11-13",
            QFSeries([5981.30, 5981.30], index=[FutuTicker("HK.800700"), FutuTicker("HK.800700")])),
    ]
)
def test_get_history__daily__real_timer(tickers, fields, start_date, end_date, expected_values, data_provider):
    result = data_provider.get_history(FutuTicker.from_string(tickers), fields, str_to_date(start_date),
                                       str_to_date(end_date), auto_adjust=False)
    assert_equal(result, expected_values)


# @pytest.mark.skipif(not is_futu_installed, reason="requires futu openapi")
# @pytest.mark.parametrize(
#     "tickers, fields, start_date, end_date, expected_values",
#     [
#         ("HAHA", "close", "2025-01-02", "2025-01-02", nan),
#         ("HAHA", "close", "2025-01-02", "2025-01-02", QFSeries([nan], index=["close"])),
#         (["HAHA"], "close", "2025-01-02", "2025-01-02", QFSeries([nan], index=[FutuTicker("HAHA")])),

#         (["HAHA", "SH.000300"], "close", "2025-01-02", "2025-01-02",
#          QFSeries([nan, 3820.40], index=["HAHA", "SH.000300"])),
#         (["HAHA", "SH.000300"], "close", "2025-01-02", "2025-01-03",
#          QFDataArray.create(date_range("2025-01-02", "2025-01-03", freq="B"),
#                             ["HAHA", "SH.000300"],
#                             ["close"], [[[nan], [3820.40]], [[nan], [3775.16]], ])),
#     ]
# )
# def test_incorrect_inputs(tickers, fields, start_date, end_date, expected_values, data_provider):
#     result = data_provider.get_history(FutuTicker.from_string(tickers), fields, str_to_date(start_date),
#                                        str_to_date(end_date), auto_adjust=False)

#     assert_equal(result, expected_values)


# @pytest.mark.skipif(not is_futu_installed, reason="requires futu openapi")
# @pytest.mark.parametrize(
#     "tickers, fields, start_date, end_date, frequency, expected_values",
#     [
#         ("HK.800700", "close", datetime(2025, 1, 1), datetime(2025, 1, 7), Frequency.WEEKLY, 4260.82),
#         ("HK.800700", "close", datetime(2025, 1, 4), datetime(2025, 1, 7), Frequency.WEEKLY, 4260.82),
#         ("HK.800700", "close", datetime(2024, 12, 1), datetime(2024, 12, 31), Frequency.MONTHLY, 4468.11),
#     ]
# )
# def test_get_history__various_frequencies_real_timer(tickers, fields, start_date, end_date, frequency,
#                                                      expected_values, data_provider):
#     result = data_provider.get_history(FutuTicker.from_string(tickers), fields, start_date, end_date, frequency,
#                                        auto_adjust=False)
#     assert_equal(result, expected_values)


@pytest.mark.skipif(not is_futu_installed, reason="requires futu openapi")
@pytest.mark.parametrize(
    "tickers, fields, start_date, end_date, frequency, expected_values, current_time",
    [
        (["HK.800700"], "close", datetime(2025, 1, 2), datetime(2025, 1, 6), Frequency.DAILY,
         QFDataFrame({FutuTicker("HK.800700"): [4357.53, 4403.12, 4395.08]},
                     index=date_range("2025-01-02", "2025-01-06", freq="B")),
         datetime(2025, 1, 6, 15)),
        (["HK.800700"], "close", datetime(2025, 1, 2), datetime(2025, 1, 6), Frequency.DAILY,
         QFDataFrame({FutuTicker("HK.800700"): [4357.53, 4403.12]},
                     index=date_range("2025-01-02", "2025-01-05", freq="B")),
         datetime(2025, 1, 6, 14)),
        (["HK.800700"], "close", datetime(2025, 1, 1), datetime(2025, 1, 7), Frequency.DAILY, 
         QFDataFrame(index=DatetimeIndex([]), 
                     columns=[FutuTicker.from_string("HK.800700")]),
         datetime(2025, 1, 2)),
        (["HK.800700"], "close", datetime(2025, 1, 1), datetime(2025, 1, 7), Frequency.DAILY,
         QFDataFrame({FutuTicker("HK.800700"): [4357.53]},
                     index=date_range("2025-01-02", "2025-01-02", freq="B")),
        datetime(2025, 1, 3))

    ]
)
def test_get_history__settable_timer(tickers, fields, start_date, end_date, frequency,
                                     expected_values, current_time, data_provider):
    MarketCloseEvent.set_trigger_time({"hour": 15, "minute": 0, "second": 0, "microsecond": 0})
    MarketOpenEvent.set_trigger_time({"hour": 9, "minute": 30, "second": 0, "microsecond": 0})

    data_provider.timer = SettableTimer(current_time)
    result = data_provider.get_history(FutuTicker.from_string(tickers), fields, start_date, end_date, frequency,
                                       auto_adjust=False)
    assert_equal(result, expected_values)
