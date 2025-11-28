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
import warnings
from datetime import datetime
from typing import Set, Type, Union, Sequence, Dict, Optional

import pandas as pd

from qf_lib.common.enums.frequency import Frequency
from qf_lib.common.enums.price_field import PriceField
from qf_lib.common.tickers.tickers import Ticker, FutuTicker
from qf_lib.common.utils.dateutils.relative_delta import RelativeDelta
from qf_lib.common.utils.dateutils.timer import Timer
from qf_lib.common.utils.miscellaneous.to_list_conversion import convert_to_list
from qf_lib.containers.dataframe.qf_dataframe import QFDataFrame
from qf_lib.containers.qf_data_array import QFDataArray
from qf_lib.containers.series.qf_series import QFSeries
from qf_lib.data_providers.abstract_price_data_provider import AbstractPriceDataProvider
from qf_lib.data_providers.helpers import normalize_data_array, tickers_dict_to_data_array

try:
    import futu as ft

    is_futu_installed = True
except ImportError:
    is_futu_installed = False


class FutuDataProvider(AbstractPriceDataProvider):
    """
    Data Provider using the futu openapi library to provide historical data of various frequencies.

    Parameters
    -----------
    timer: Timer
        Might be either SettableTimer or RealTimer depending on the use case. If no parameter is passed, a default
        RealTimer object will be used.
    """
    def __init__(self, timer: Optional[Timer] = None):
        super().__init__(timer)

        if not is_futu_installed:
            warnings.warn("futu openapi is not installed. If you would like to use FutuDataProvider first install the"
                          " futu openapi library.")
            exit(1)

    def price_field_to_str_map(self, *args) -> Dict[PriceField, str]:
        return {
            PriceField.Open: 'open',
            PriceField.High: 'high',
            PriceField.Low: 'low',
            PriceField.Close: 'close',
            PriceField.Volume: 'volume'
        }

    def _field_to_futu_field(self, field: str) -> str|None:
        fields_to_futu_map = {
            'open': ft.KL_FIELD.OPEN,
            'high': ft.KL_FIELD.HIGH,
            'low': ft.KL_FIELD.LOW,
            'close': ft.KL_FIELD.CLOSE,
            'volume': ft.KL_FIELD.TRADE_VAL
        }

        if isinstance(field, PriceField):
            f = field
            field_str_map = self.price_field_to_str_map()
            try:
                f = fields_to_futu_map[field]
            except KeyError:
                raise ValueError(f"Field must be one of the supported fields: {field_str_map.keys()}.") from None
        try:
            f = fields_to_futu_map[field]
        except KeyError:
            raise TypeError("Field must be either PriceField or str.") from None
        
        return f

    def get_history(self, tickers: Union[Ticker, Sequence[Ticker]], fields: Union[str, Sequence[str]],
                    start_date: datetime, end_date: Optional[datetime] = None, frequency: Optional[Frequency] = None,
                    look_ahead_bias: bool = False, auto_adjust: bool = True, **kwargs) \
            -> Union[QFSeries, QFDataFrame, QFDataArray]:
        """
        Gets historical attributes (fields) of different securities (tickers).

        Parameters
        ----------
        tickers: FutuTicker, Sequence[FutuTicker]
            tickers for securities which should be retrieved
        fields: None, str, Sequence[str]
            fields of securities which should be retrieved.
        start_date: datetime
            date representing the beginning of historical period from which data should be retrieved
        end_date: datetime
            date representing the end of historical period from which data should be retrieved;
            if no end_date was provided, by default the current date will be used
        frequency: Frequency
            frequency of the data. This data provider supports Monthly, Weekly, Daily frequencies along with intraday
            frequencies at the following intervals: 60, 30, 15, 5 and 1 minute.
        look_ahead_bias: bool
            if set to False, the look-ahead bias will be taken care of to make sure no future data is returned
        auto_adjust: bool
            Adjust back all OHLC prices automatically. Default values is True.

        Returns
        -------
        QFSeries, QFDataFrame, QFDataArray, float, str
            If possible the result will be squeezed, so that instead of returning a QFDataArray, data of lower
            dimensionality will be returned. The results will be either a QFDataArray (with 3 dimensions: date, ticker,
            field), a QFDataFrame (with 2 dimensions: date, ticker or field; it is also possible to get 2 dimensions
            ticker and field if single date was provided), a QFSeries (with 1 dimensions: date) or a float / str
            (in case if a single ticker, field and date were provided).
            If no data is available in the database or a non existing ticker was provided an empty structure
            (nan, QFSeries, QFDataFrame or QFDataArray) will be returned.
        """

        frequency = frequency or self.frequency or Frequency.DAILY
        original_end_date = (end_date or self.timer.now()) + RelativeDelta(second=0, microsecond=0)
        # In case of low frequency (daily or lower) shift the original end date to point to 23:59
        if frequency <= Frequency.DAILY:
            original_end_date += RelativeDelta(hour=23, minute=59)

        end_date = original_end_date if look_ahead_bias else (
            self.get_end_date_without_look_ahead(original_end_date, frequency))
        start_date = self._adjust_start_date(start_date, frequency)
        got_single_date = self._got_single_date(start_date, original_end_date, frequency)

        tickers, got_single_ticker = convert_to_list(tickers, FutuTicker)
        fields, got_single_field = convert_to_list(fields, str)

        futu_ktype = self._frequency_to_period(frequency)
        futu_fields = [self._field_to_futu_field(field) for field in fields]

        tickers_df_dict = {}
        available_fields = list()

        quote_ctx = ft.OpenQuoteContext(host='127.0.0.1', port=11111)

        for ticker in tickers:
            df = None
            ret, df, page_req_key = quote_ctx.request_history_kline(ticker.as_string(), start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'), ktype=futu_ktype, session=ft.Session.ALL)  # 每页5个，请求第一页
            if ret != ft.RET_OK:
                warnings.warn(f'get {ticker.as_string()} history kline error: {df}')
                continue

            while page_req_key != None:  # 请求后面的所有结果
                ret, data, page_req_key = quote_ctx.request_history_kline(ticker.as_string(), start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'), ktype=futu_ktype, page_req_key=page_req_key, session=ft.Session.ALL) # 请求翻页后的数据
                if ret == ft.RET_OK:
                    df = pd.concat(df, data)
                else:
                    warnings.warn(f'get {ticker.as_string()} history kline error: {data}, page_req_key: {page_req_key}')
                    break

            df.reset_index(inplace=True)
            df.index = pd.to_datetime(df['time_key'])
            df = df[~df.index.duplicated(keep='first')]
            df = df.drop('time_key', axis=1)

            if fields:
                available_fields = list(fields)
                df = df.loc[:, df.columns.isin(fields)]
                fields_diff = set(fields).difference(df.columns)
                if fields_diff:
                    self.logger.info(f"Not all fields are available for {ticker}. Difference: {fields_diff}")
            else:
                available_fields = df.columns.tolist()

            tickers_df_dict[ticker] = df

        quote_ctx.close() # 结束后记得关闭当条连接，防止连接条数用尽

        qf_data_array = tickers_dict_to_data_array(tickers_df_dict, list(tickers_df_dict.keys()) if tickers_df_dict else list(), available_fields)
        
        return normalize_data_array(
            qf_data_array, tickers, fields, got_single_date, got_single_ticker, got_single_field, use_prices_types=False
        )

    def supported_ticker_types(self) -> Set[Type[Ticker]]:
        return {FutuTicker}

    @staticmethod
    def _frequency_to_period(freq: Frequency):
        frequencies_mapping = {
            Frequency.MIN_1: ft.KLType.K_1M,
            Frequency.MIN_5: ft.KLType.K_5M,
            Frequency.MIN_15: ft.KLType.K_15M,
            Frequency.MIN_30: ft.KLType.K_30M,
            Frequency.MIN_60: ft.KLType.K_60M,
            Frequency.DAILY: ft.KLType.K_DAY,
            Frequency.WEEKLY: ft.KLType.K_WEEK,
            Frequency.MONTHLY: ft.KLType.K_MON,
            Frequency.QUARTERLY: ft.KLType.K_QUARTER,
        }

        try:
            return frequencies_mapping[freq]
        except KeyError:
            raise ValueError(f"Frequency must be one of the supported frequencies: {frequencies_mapping.keys()}.") \
                from None
