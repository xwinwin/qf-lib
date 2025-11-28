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
from pathlib import Path
from typing import Sequence, Union, List, Dict, Optional
import warnings
import os

import pandas as pd

from qf_lib.common.enums.frequency import Frequency
from qf_lib.common.enums.security_type import SecurityType
from qf_lib.common.enums.price_field import PriceField
from qf_lib.common.tickers.tickers import Ticker
from qf_lib.common.utils.logging.qf_parent_logger import qf_logger
from qf_lib.common.utils.miscellaneous.to_list_conversion import convert_to_list
from qf_lib.containers.dataframe.qf_dataframe import QFDataFrame
from qf_lib.common.tickers.tickers import QLibTicker
from qf_lib.containers.futures.future_tickers.future_ticker import FutureTicker
from qf_lib.data_providers.helpers import normalize_data_array, tickers_dict_to_data_array
from qf_lib.data_providers.preset_data_provider import PresetDataProvider

try:
    import qlib
    from qlib.data import D
    from qlib.constant import REG_CN
    is_qlib_installed = True
except ImportError:
    is_qlib_installed = False

class QLibDataProvider(PresetDataProvider):
    """
    Generic Data Provider that loads microsoft qlib format data files. All the files should have a certain naming convention (see Notes).
    Additionally, the data provider requires providing mapping between file names of the qlib files
    price fields in the form of dictionary where the key is a column name in the csv file, and the value is
    a corresponding PriceField. Please note that this is required to use get_price method

    Parameters
    -----------
    path: str
        it should be either path to the directory containing the qlib data files
    tickers: Ticker, Sequence[Ticker]
        one or a list of tickers, used further to download the prices data
    index_col: str
        Label of the dates / timestamps column, which will be later on used to index the data.
        No need to repeat it in the fields.
    field_to_price_field_dict: Optional[Dict[str, PriceField]]
        mapping of header names to PriceField. It is required to call get_price method which uses PriceField enum.
        In the mapping, the key is a column name, and the value is a corresponding PriceField.
        for example if header for open price is called 'Open price' put mapping  {'Open price': Pricefield:Open}
        Preferably map all: open, high, low, close to corresponding price fields.
    fields: Optional[str, List[str]]
        all columns that will be loaded to the QLibDataProvider from given file.
        these fields will be available in get_history method.
        By default all fields (columns) are loaded.
    start_date: Optional[datetime]
        first date to be downloaded
    end_date: Optional[datetime]
        last date to be downloaded
    frequency: Optional[Frequency]
        frequency of the data. The parameter is optional, and by default equals to daily Frequency.
    dateformat: Optional[str]
        the strftime to parse time, e.g. "%d/%m/%Y". Parameter is Optional and if not provided, the data provider will
        try to infer the dates format from the data. By default None.
    ticker_col: Optional[str]
        column name with the tickers

    Notes
    -----
        - FutureTickers are not supported by this data provider.
        - By default, data for each ticker should be in a separate file named after this tickers' string representation
        (in most cases it is simply its name, to check what is the string representation of a given ticker use
        Ticker.as_string() function). However, you can also load one file containing all data with specified tickers in
        one column row by row as it is specified in demo example file daily_data.csv or intraday_data.csv.
        In order to do so you need to specify the name of the ticker column in ticker_col and specify the path to the
        file.
        - Please note that when using ticker_col it is required to provide the path to specific file (loading is not
        based on ticker names as it is in the default approach)
        - By providing mapping field_to_price_field_dict you are able to use get_price method which allows you to
        aggregate intraday data (currently, get_history does not allow using intraday data aggregation)

    Example
    -----
        start_date = str_to_date("2018-01-01")
        end_date = str_to_date("2022-01-01")

        index_column = 'data'
        field_to_price_field_dict = {
            'open': PriceField.Open,
            'high': PriceField.High,
            'low': PriceField.Low,
            'close': PriceField.Close,
            'volume': PriceField.Volume,
        }

        tickers = #create your ticker here. ticker.as_string() should match file name, unless you specify ticker_col
        path = "~/.qlib/qlib_data/cn_data"
        data_provider = QLibDataProvider(path, tickers, field_to_price_field_dict, start_date,
                                        end_date, Frequency.DAILY)
    """

    def __init__(self, path: str = '', region: str = '', tickers: Union[Ticker, Sequence[Ticker]] = [],
                 field_to_price_field_dict: Optional[Dict[str, PriceField]] = None,
                 fields: Optional[Union[str, List[str]]] = None, start_date: Optional[datetime] = None,
                 end_date: Optional[datetime] = None, frequency: Frequency = Frequency.DAILY,
                 dateformat: Optional[str] = None):

        self.logger = qf_logger.getChild(self.__class__.__name__)

        if not is_qlib_installed:
            self.logger.warning("QLib is not installed. If you would like to use QLibDataProvider first install the "
                               "microsoft qlib library. You can follow next step here:" \
                               "pip install pyqlib")
            warnings.warn("QLib is not installed. If you would like to use QLibDataProvider first install the "
                          "microsoft qlib library. You can follow next step here:" \
                          "pip install pyqlib")
            exit(1)

        region = region if region != '' else str(os.getenv('QLIB_REGION', REG_CN))

        path = path if path != '' else os.getenv('QLIB_DIR', f'{os.path.expanduser("~")}/.qlib/qlib_data/{region}_data')
        if path == '':
            self.logger.warning("No qlib data directory specified")
            warnings.warn("No qlib data directory specified")
            exit(1)

        if not Path(path).exists():
            self.logger.warning(f"The specified qlib data directory: {path} does not exist")
            warnings.warn(f"The specified qlib data directory: {path} does not exist")
            exit(1)

        qlib.init(provider_uri=path, region=region)

        if fields:
            fields, _ = convert_to_list(fields, str)

        # Convert to list and remove duplicates
        tickers, _ = convert_to_list(tickers, Ticker)
        tickers = list(dict.fromkeys(tickers))
        assert len([t for t in tickers if isinstance(t, FutureTicker)]) == 0, "FutureTickers are not supported by " \
                                                                              "this data provider"

        data_array, start_date, end_date, real_tickers, available_fields = self._get_data(path, tickers, fields, start_date, end_date,
                                                                            frequency, field_to_price_field_dict,
                                                                            dateformat)

        normalized_data_array = normalize_data_array(data_array, real_tickers, available_fields, False, False, False)

        super().__init__(data=normalized_data_array,
                         start_date=start_date,
                         end_date=end_date,
                         frequency=frequency)

    def _get_data(self, path: str, tickers: Sequence[Ticker], fields: Optional[Sequence[str]], start_date: datetime,
                  end_date: datetime, frequency: Frequency, field_to_price_field_dict: Optional[Dict[str, PriceField]],
                  dateformat: str | None):

        tickers_str_mapping = {}
        tickers_prices_dict = {}
        available_fields = set()

        freq_to_qlib_freq = {
            Frequency.MIN_1: "1min",
            Frequency.MIN_5: "5min",
            Frequency.MIN_10: "10min",
            Frequency.MIN_15: "15min",
            Frequency.MIN_30: "30min",
            Frequency.MIN_60: "60min",
            Frequency.DAILY: "day",
            Frequency.WEEKLY: "week",
            Frequency.MONTHLY: "month",
            Frequency.QUARTERLY: "quarter",
            Frequency.YEARLY: "year"
        }

        column_rename_mapping = {
            '$open/$factor': 'open',
            '$close/$factor': 'close',
            '$high/$factor': 'high',
            '$low/$factor': 'low',
            '$volume/$factor': 'volume'
        }

        def _process_df(df, ticker_str):
            df.index = pd.to_datetime(df['datetime'], format=dateformat)
            df = df[~df.index.duplicated(keep='first')]
            df = df.drop('datetime', axis=1)

            start_time = start_date or df.index[0]
            end_time = end_date or df.index[-1]

            if fields:
                df = df.loc[start_time:end_time, df.columns.isin(fields)]
                fields_diff = set(fields).difference(df.columns)
                if fields_diff:
                    self.logger.info(f"Not all fields are available for {path}. Difference: {fields_diff}")
            else:
                df = df.loc[start_time:end_time, :]
                available_fields.update(df.columns.tolist())

            if field_to_price_field_dict:
                for key, value in field_to_price_field_dict.items():
                    df[value] = df[key]

            if ticker_str in tickers_str_mapping:
                tickers_prices_dict[tickers_str_mapping[ticker_str]] = df
            else:
                self.logger.info(f'Ticker {ticker_str} was not requested in the list of tickers. Skipping.')

        for ticker in tickers:
            if ticker.security_type == SecurityType.STOCK:
                tickers_str_mapping[ticker.as_string()] = ticker
            elif ticker.security_type == SecurityType.INDEX:
                instruments = D.instruments(market=ticker.as_string())
                instrument_list = D.list_instruments(instruments=instruments, as_list=True, freq=freq_to_qlib_freq[frequency])
                for instrument in instrument_list:
                    tickers_str_mapping[instrument] = QLibTicker(instrument, SecurityType.STOCK)
            else:
                raise ValueError("Only Stock and Stock Index are supported by QLibDataProvider")

        instruments = list(tickers_str_mapping.keys())

        df = D.features(instruments=instruments, fields=list(column_rename_mapping.keys()), start_time=start_date, end_time=end_date)
        
        # reset index to have datetime as a column
        df.reset_index(inplace=True)

        # rename columns
        df = df.rename(columns=column_rename_mapping)

        df = QFDataFrame(df)

        available_tickers = df['instrument'].dropna().unique().tolist()
        for ticker_str in available_tickers:
            sliced_df = df[df['instrument'] == ticker_str]
            _process_df(sliced_df, ticker_str)

        if not tickers_prices_dict.values():
            raise ImportError("No data was found. Check the correctness of all data")

        if fields:
            available_fields = list(fields)
        else:
            available_fields = list(available_fields)

        if field_to_price_field_dict:
            available_fields.extend(list(field_to_price_field_dict.values()))

        if not start_date:
            start_date = min(list(df.index.min() for df in tickers_prices_dict.values()))

        if not end_date:
            end_date = max(list(df.index.max() for df in tickers_prices_dict.values()))

        real_tickers = list(tickers_prices_dict.keys())

        result = tickers_dict_to_data_array(tickers_prices_dict, real_tickers, available_fields), \
            start_date, end_date, real_tickers, available_fields

        return result
