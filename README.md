# kraken-btc-price-interval

* Download the historical OHLCVT data from Kraken https://support.kraken.com/hc/en-us/articles/360047124832-Downloadable-historical-OHLCVT-Open-High-Low-Close-Volume-Trades-data.
* Extract it under `kraken-btc-price-interval/data/`.<br>
![image](https://github.com/tcm5343/kraken-btc-price-interval/assets/48961675/ec79596c-68f9-4f08-9737-87f2699a9428)
* Install requirements `pip install requirements.txt`
* Run the program `python3 src/main.py`

As output, you will see the data with the custom time interval. The lower limit of the
interval is one minute. To change the interval, modify the `INTERVAL` constant in `src/main.py`.
