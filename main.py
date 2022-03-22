import csv
import hashlib
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import List, Union

import requests


@dataclass
class CoinCandle:
    time_period_start: Union[str, datetime]
    time_period_end: str
    time_open: str
    time_close: str
    price_open: float
    price_high: float
    price_low: float
    price_close: float
    volume_traded: float
    trades_count: float

    # ----- Calculated
    id: str = None

    # candle length from lowest price open || close - variation in %
    variation: float = None
    price_sell: float = None
    price_stop: float = None

    # When candle is buyer or seller
    is_ascending: bool = None
    is_descending: bool = None


def _fetch_coin_history():
    url = 'https://rest.coinapi.io/v1/ohlcv/BITFINEX_BTC_USD/history?period_id=5MIN&time_start=2022-03-18T23:00:00' \
          '&limit=2 '
    headers = {'X-CoinAPI-Key': '9948C311-E1C1-4AB2-8E4B-F649D6D7CAEA'}
    return requests.get(url, headers=headers).json()


def _calculate_extra_fields(coin_candle: CoinCandle, row_fields_values: List[str]):
    def _to_datetime(dt):
        return datetime.strptime(dt[:22], "%Y-%m-%dT%H:%M:%S.%f")

    # basic
    coin_candle.time_period_start = _to_datetime(coin_candle.time_period_start)
    coin_candle.time_period_end = _to_datetime(coin_candle.time_period_end)
    coin_candle.time_open = _to_datetime(coin_candle.time_open)
    coin_candle.time_close = _to_datetime(coin_candle.time_close)

    coin_candle.price_low = float(coin_candle.price_low)
    coin_candle.price_high = float(coin_candle.price_high)
    coin_candle.price_open = float(coin_candle.price_open)
    coin_candle.price_close = float(coin_candle.price_close)
    coin_candle.volume_traded = float(coin_candle.volume_traded)
    coin_candle.trades_count = float(coin_candle.trades_count)

    coin_candle.is_ascending = coin_candle.price_open <= coin_candle.price_close
    coin_candle.is_descending = not coin_candle.is_ascending

    # candle length from lowest price open || close - variation in %
    open_and_close_diff = abs(coin_candle.price_close - coin_candle.price_open)
    lower_price = min(coin_candle.price_open, coin_candle.price_close)
    coin_candle.variation = open_and_close_diff / lower_price

    # id calculation
    all_values = ";".join([str(v) for v in asdict(coin_candle).values()])
    coin_candle.id = hashlib.sha224(all_values.encode("utf-8")).hexdigest()


def _read_coin_history_from_csv() -> List[CoinCandle]:
    output = []

    with open('dump_btc_bitfinex_5m.csv', 'r', encoding='UTF8') as f:
        header = True
        for row_fields_values in csv.reader(f, delimiter=";"):
            if header:
                header = False
                continue

            coin_candle = CoinCandle(*row_fields_values)
            _calculate_extra_fields(coin_candle, row_fields_values)
            output.append(coin_candle)

    return sorted(output, key=lambda item: item.time_close)


def import_btc_to_csv():
    response = _fetch_coin_history()
    header = asdict(response[0]).keys()

    with open('out.csv', 'w', encoding='UTF8') as f:
        writer = csv.writer(f)

        # write the header
        writer.writerow(header)

        for row_object in response:
            # write the data
            row = []
            for field in header:
                try:
                    row.append(row_object[field])
                except KeyError:
                    row.append("-")

            writer.writerow(row)


def analyse_trade_result(
        current_candle: CoinCandle,
        candles_history: List[CoinCandle]
) -> str:
    # trading fields
    price_sell = (
            (current_candle.price_close * current_candle.variation) + current_candle.price_close
    )
    price_stop = current_candle.price_open

    current_candle.price_sell, current_candle.price_stop = price_sell, price_stop

    gain_candle = list(filter(
        lambda candle:
        current_candle.time_close < candle.time_close
        and candle.price_open <= current_candle.price_sell <= candle.price_high,
        candles_history
    )) or None

    stop_candle = list(filter(
        lambda candle:
        current_candle.time_close < candle.time_close
        and candle.price_close >= current_candle.price_stop >= candle.price_low,
        candles_history
    )) or None

    if gain_candle is not None and stop_candle is not None:
        if gain_candle[0].time_close < stop_candle[0].time_close:
            return 'gain'
        else:
            return 'loss'
    elif gain_candle is not None:
        return 'gain'
    elif stop_candle is not None:
        return 'loss'
    else:
        return 'not-found-candle'


def main():
    # some settings
    config_candle_variation = 0.002
    config_next_candle_is_ascending = False
    config_check_trend = True
    config_check_volume = True

    gains = []
    losses = []
    not_found_candle = []

    candles_history = _read_coin_history_from_csv()

    for index, current_candle in enumerate(candles_history):
        # Only checks for ascending candles
        if current_candle.variation < config_candle_variation or current_candle.is_descending:
            continue

        try:
            next_candle = candles_history[index + 1]
        except IndexError:
            break

        # Should next candle be ascending?
        if config_next_candle_is_ascending and next_candle.is_descending:
            continue

        # The current candle must break the next candle
        if next_candle.price_close > current_candle.price_close:
            continue

        # -----
        # Is necessary include more checks here? Maybe, but firstly let's keep it simple.
        # -----

        # If the candle arrive here, it' a trade! Let's check if this will result a gain or loss.
        result = analyse_trade_result(current_candle, candles_history)

        if result == 'gain':
            gains.append(current_candle)
        elif result == 'loss':
            losses.append(current_candle)
        else:
            not_found_candle.append(current_candle)

    # -----------------------------------------------------------------
    total_gain = len(gains)
    total_loss = len(losses)
    total_not_found = len(not_found_candle)

    if not total_gain and not total_loss and not total_not_found:
        print("=======================================")
        print("Ops, no results were found, check it out!")
        print("=======================================")
        return

    success_rate = (total_gain / (total_gain + total_loss + total_not_found)) * 100
    failure_rate = (total_loss / (total_gain + total_loss + total_not_found)) * 100

    total_success_variation = sum(map(lambda candle: candle.variation, gains))
    total_failure_variation = sum(map(lambda candle: candle.variation, losses))
    result_variation = total_success_variation - total_failure_variation

    print("=======================================")
    print(f"Gains: {total_gain} - {success_rate}% - Results: {total_success_variation}% ")
    print(f"Stops: {total_loss} - {failure_rate}% - Results: {total_failure_variation}% ")
    print(f"Result: {result_variation}%")
    print("---------------------------------------")
    print(f"Not found candles: {len(not_found_candle)}")
    print("=======================================")


if __name__ == '__main__':
    main()
