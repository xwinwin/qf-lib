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

from qf_lib.common.enums.price_field import PriceField
from qf_lib.common.enums.security_type import SecurityType
from qf_lib.common.tickers.tickers import FutuTicker
from qf_lib.data_providers.futu.futu_data_provider import FutuDataProvider

"""
The purpose of this script is to provide an example of how to use the FutuDataProvider.
No authentication or API key is needed to run this demo as it uses only the freely available
cryptocurrencies data.
"""

def main():
    data_provider = FutuDataProvider()

    # Return the latest 5 close prices of HK.800700 index
    prices = data_provider.historical_price([FutuTicker("HK.800700", SecurityType.STOCK), FutuTicker("SH.000300", SecurityType.STOCK)], [PriceField.Open, PriceField.Close],
                                           nr_of_bars=5)
    print(prices)

if __name__ == '__main__':
    main()
