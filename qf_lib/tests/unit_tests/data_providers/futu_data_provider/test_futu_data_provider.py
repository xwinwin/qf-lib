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
from unittest.mock import patch

import pytest
import pandas as pd
from numpy import nan

from qf_lib.backtesting.events.time_event.regular_time_event.market_close_event import MarketCloseEvent
from qf_lib.backtesting.events.time_event.regular_time_event.market_open_event import MarketOpenEvent
from qf_lib.common.enums.frequency import Frequency
from qf_lib.common.utils.dateutils.string_to_date import str_to_date
from qf_lib.common.utils.dateutils.timer import SettableTimer
from qf_lib.containers.dataframe.qf_dataframe import QFDataFrame
from qf_lib.containers.series.qf_series import QFSeries
from qf_lib.data_providers.futu.futu_data_provider import FutuDataProvider, is_futu_installed
from qf_lib.common.tickers.tickers import FutuTicker
from qf_lib.tests.helpers.testing_tools.containers_comparison import assert_series_equal


@pytest.fixture
def data_provider():
    return FutuDataProvider()


@pytest.fixture
def mock_daily_request_history_kline():
    """ Fixture to mock futu.request_history_kline to return hardcoded data. """

    def _mock_request_history_kline(tickers, start, end, **kwargs):
        # Hardcoded data for testing
        data = {
            "AAPL": pd.DataFrame({
                ("time_key"): pd.date_range("2025-01-01", "2025-01-08"), 
                ("open"): [140.0, 142.0, None, 142.0, 143.0, 144.0, 145.0, 146.0],
                ("close"): [150.0, 151.0, None, 152.0, 153.0, 154.0, 155.0, 156.0],
                ("volume"): [1000000 + i * 100000 for i in range(8)],
            }, index=pd.date_range("2025-01-01", "2025-01-08")),
            "MSFT": pd.DataFrame({
                ("time_key"): pd.date_range("2025-01-01", "2025-01-08"),
                ("close"): [300.0, None, 305.0, 310.0, 315.0, 320.0, 330.0, 340.0],
                ("volume"): [2000000 + i * 100000 for i in range(8)],
            }, index=pd.date_range("2025-01-01", "2025-01-08")),
        }

        try:
            if isinstance(tickers, list):
                return 0, pd.concat([data[t] for t in tickers], axis=1).loc[start:end], None
            else:
                return 0, data[tickers].loc[start:end], None
        except KeyError:
            raise NotImplementedError(f"No data mocked for {tickers}") from None

    return _mock_request_history_kline


@pytest.mark.skipif(not is_futu_installed, reason="requires futu openapi")
@patch("futu.OpenQuoteContext.request_history_kline")
@pytest.mark.parametrize(
    "tickers, fields, start_date, end_date, frequency, expected_type, expected_values",
    [
        ("AAPL", "close", "2025-01-01", "2025-01-01", Frequency.DAILY, float, 150.0),
        ("AAPL", ["close"], "2025-01-02", "2025-01-02", Frequency.DAILY, QFSeries, QFSeries([151.0], index=["close"])),
        ("AAPL", "close", "2025-01-01", "2025-01-02", Frequency.DAILY, QFSeries,
         QFSeries([150.0, 151.0], pd.date_range("2025-01-01", "2025-01-02"))),
    ]
)
def test_get_history__real_timer(mock_requst_history_kline, tickers, fields, start_date,
                                 end_date, frequency, expected_type, expected_values,
                                 mock_daily_request_history_kline,
                                 data_provider):
    mock_requst_history_kline.side_effect = mock_daily_request_history_kline

    result = data_provider.get_history(FutuTicker.from_string(tickers), fields, str_to_date(start_date),
                                       str_to_date(end_date), frequency)

    assert isinstance(result, expected_type), f"Expected type {expected_type}, but got {type(result)}"

    if isinstance(result, float):
        assert result == expected_values, f"Expected value {expected_values}, but got {result}"
    elif isinstance(result, QFSeries):
        assert_series_equal(expected_values, result, check_names=False, check_index_type=False)


@pytest.mark.skipif(not is_futu_installed, reason="requires futu openapi")
@patch("futu.OpenQuoteContext.request_history_kline")
@pytest.mark.parametrize(
    "tickers, fields, start_date, end_date, frequency, expected_type, expected_values, current_time",
    [
        ("AAPL", "close", "2025-01-01", "2025-01-01", Frequency.DAILY, float, 150.0, datetime(2025, 1, 3, 14)),
        ("AAPL", "close", "2025-01-01", "2025-01-02", Frequency.DAILY, QFSeries,
         QFSeries([150, 151], pd.date_range("2025-01-01", "2025-01-02")), datetime(2025, 1, 3, 14)),
        ("AAPL", "close", "2025-01-01", "2025-01-02", Frequency.DAILY, QFSeries,
         QFSeries([150, 151], pd.date_range("2025-01-01", "2025-01-02")), datetime(2025, 1, 2, 16)),
        ("AAPL", "close", "2025-01-01", "2025-01-02", Frequency.DAILY, QFSeries,
         QFSeries([150], pd.date_range("2025-01-01", "2025-01-01")), datetime(2025, 1, 2, 14, 45)),
        ("AAPL", "close", "2025-01-01", "2025-01-02", Frequency.DAILY, QFSeries,
         QFSeries([150], pd.date_range("2025-01-01", "2025-01-01")), datetime(2025, 1, 1, 15, 45)),

        # Get history, daily, doesn't return Open price before the market closes
        ("AAPL", "open", "2025-01-01", "2025-01-02", Frequency.DAILY, QFSeries,
         QFSeries([140, ], pd.date_range("2025-01-01", "2025-01-01")), datetime(2025, 1, 2, 14)),
        ("AAPL", "open", "2025-01-01", "2025-01-02", Frequency.DAILY, QFSeries,
         QFSeries([140, 142], pd.date_range("2025-01-01", "2025-01-02")), datetime(2025, 1, 2, 16)),

        (["AAPL", "MSFT"], "close", "2025-01-01", "2025-01-02", Frequency.DAILY, QFDataFrame,
         QFDataFrame.from_dict({FutuTicker('AAPL'): {str_to_date('2025-01-01'): 150.0,
                                                         str_to_date('2025-01-02'): 151.0},
                                FutuTicker('MSFT'): {str_to_date('2025-01-01'): 300.0,
                                                         str_to_date('2025-01-02'): nan}}),
         datetime(2025, 1, 2, 16)),
        (["AAPL", "AAPL"], "close", "2025-01-01", "2025-01-02", Frequency.DAILY, QFDataFrame,
         QFDataFrame.from_dict({FutuTicker('AAPL'): {str_to_date('2025-01-01'): 150.0,
                                                         str_to_date('2025-01-02'): 151.0},
                                FutuTicker('AAPL'): {str_to_date('2025-01-01'): 150.0,
                                                         str_to_date('2025-01-02'): 151.0}}),
         datetime(2025, 1, 2, 16)),
    ]
)
def test_get_history__settable_timer(mock_requst_history_kline, tickers, fields, start_date,
                                     end_date, frequency, expected_type, expected_values, current_time,
                                     mock_daily_request_history_kline,
                                     data_provider):
    MarketOpenEvent.set_trigger_time({"hour": 9, "minute": 0, "second": 0, "microsecond": 0})
    MarketCloseEvent.set_trigger_time({"hour": 15, "minute": 0, "second": 0, "microsecond": 0})

    data_provider.timer = SettableTimer(current_time)
    mock_requst_history_kline.side_effect = mock_daily_request_history_kline

    result = data_provider.get_history(FutuTicker.from_string(tickers), fields, str_to_date(start_date),
                                       str_to_date(end_date), frequency)

    assert isinstance(result, expected_type), f"Expected type {expected_type}, but got {type(result)}"

    if isinstance(result, float):
        assert result == expected_values, f"Expected value {expected_values}, but got {result}"
    elif isinstance(result, QFSeries):
        assert_series_equal(expected_values, result, check_names=False, check_index_type=False)