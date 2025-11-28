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
from qf_lib.data_providers.qlib.qlib_data_provider import QLibDataProvider, is_qlib_installed
from qf_lib.common.tickers.tickers import QLibTicker
from qf_lib.tests.helpers.testing_tools.containers_comparison import assert_series_equal, assert_dataframes_equal, \
    assert_dataarrays_equal


@pytest.fixture
def data_provider():
    tickers = [QLibTicker('SH000300'), QLibTicker('SH601228'), QLibTicker('SH600021')]
    return QLibDataProvider(tickers=tickers, frequency=Frequency.DAILY)


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


@pytest.mark.skipif(not is_qlib_installed, reason="requires qlib")
@pytest.mark.parametrize(
    "tickers, fields, start_date, end_date, expected_values",
    [
        ("SH000300", "close", "2025-01-02", "2025-01-02", 3820.395264),
        ("SH000300", ["close"], "2025-01-02", "2025-01-02", QFSeries([3820.395264], index=["close"])),
        (["SH000300"], "close", "2025-01-02", "2025-01-02", QFSeries([3820.395264], index=[QLibTicker("SH000300")])),
        ("SH000300", ["close"], "2025-01-02", "2025-01-06",
         QFDataFrame({"close": [3820.395264, 3775.164795, 3768.969482]}, index=date_range("2025-01-02", "2025-01-06", freq="B"))),
        (["SH000300"], "close", "2025-01-02", "2025-01-06",
         QFDataFrame({QLibTicker("SH000300"): [3820.395264, 3775.164795, 3768.969482]},
                     index=date_range("2025-01-02", "2025-01-06", freq="B"))),
        ("SH000300", "close", "2025-01-01", "2025-01-06",
         QFSeries([3820.395264, 3775.164795, 3768.969482], date_range("2025-01-02", "2025-01-06", freq="B"))),
        (["SH000300"], ["close"], "2025-01-01", "2025-01-06",
         QFDataArray.create(date_range("2025-01-02", "2025-01-06", freq="B"), [QLibTicker("SH000300")], ["close"],
                            [[[3820.395264]], [[3775.164795]], [[3768.969482]]])),
        (["SH000300", "SH601228"], "close", "2025-01-02", "2025-01-02",
         QFSeries([3820.395264, 3.27], index=[QLibTicker("SH000300"), QLibTicker("SH601228")])),
        (["SH000300", "SH601228"], ["close"], "2025-01-02", "2025-01-03",
         QFDataArray.create(date_range("2025-01-02", "2025-01-03", freq="B"),
                            [QLibTicker("SH000300"), QLibTicker("SH601228")],
                            ["close"], [[[3820.395264], [3.27]], [[3775.164795], [3.23]]])),
        (["SH000300", "SH000300"], "close", "2025-01-02", "2025-01-02",
         QFSeries([3820.395264], index=[QLibTicker("SH000300")])),
    ]
)
def test_get_history__daily__real_timer(tickers, fields, start_date, end_date, expected_values, data_provider):
    result = data_provider.get_history(QLibTicker.from_string(tickers), fields, str_to_date(start_date),
                                       str_to_date(end_date), frequency=Frequency.DAILY)
    assert_equal(result, expected_values)


@pytest.mark.skipif(not is_qlib_installed, reason="requires qlib")
@pytest.mark.parametrize(
    "tickers, fields, start_date, end_date, frequency, expected_values, current_time",
    [
        (["SH000300"], "close", datetime(2025, 1, 2), datetime(2025, 1, 6), Frequency.DAILY,
         QFDataFrame({QLibTicker("SH000300"): [3820.395264, 3775.164795, 3768.969482]},
                     index=date_range("2025-01-02", "2025-01-06", freq="B")),
         datetime(2025, 1, 6, 15)),
        (["SH000300"], "close", datetime(2025, 1, 2), datetime(2025, 1, 6), Frequency.DAILY,
         QFDataFrame({QLibTicker("SH000300"): [3820.395264, 3775.164795]},
                     index=date_range("2025-01-02", "2025-01-05", freq="B")),
         datetime(2025, 1, 6, 14)),
        ("SH000300", "close", datetime(2025, 1, 1), datetime(2025, 1, 7), Frequency.DAILY, QFSeries([3820.395264, 3775.164795, 3768.969482, 3796.105713], index=[datetime(2025, 1, 2), datetime(2025, 1, 3), datetime(2025, 1, 6), datetime(2025, 1, 7)]), datetime(2025, 1, 8)),
        ("SH000300", "close", datetime(2025, 1, 1), datetime(2025, 1, 7), Frequency.DAILY, QFSeries(index=DatetimeIndex([])), datetime(2025, 1, 2)),
        ("SH000300", "close", datetime(2025, 1, 1), datetime(2025, 1, 7), Frequency.DAILY, QFSeries([3820.395264], index=[datetime(2025, 1, 2)]), datetime(2025, 1, 3)),
    ]
)
def test_get_history__settable_timer(tickers, fields, start_date, end_date, frequency,
                                     expected_values, current_time, data_provider):
    MarketCloseEvent.set_trigger_time({"hour": 15, "minute": 0, "second": 0, "microsecond": 0})
    MarketOpenEvent.set_trigger_time({"hour": 9, "minute": 30, "second": 0, "microsecond": 0})

    data_provider.timer = SettableTimer(current_time)
    result = data_provider.get_history(QLibTicker.from_string(tickers), fields, start_date, end_date, frequency)
    assert_equal(result, expected_values)