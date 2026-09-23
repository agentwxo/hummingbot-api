from decimal import Decimal
from typing import Dict, List, Set, Optional
import pandas as pd
import time
import math
import logging
import sqlite3
import os
from types import SimpleNamespace
from pydantic import Field

from hummingbot.core.data_type.common import PriceType, TradeType, OrderType, PositionMode, PositionAction
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.order_executor.data_types import (
    OrderExecutorConfig,
    ExecutionStrategy,
)
from hummingbot.strategy_v2.models.executor_actions import (
    CreateExecutorAction,
    StopExecutorAction,
    ExecutorAction,
)


class AroonOscillatorConfig(ControllerConfigBase):
    controller_name: str = "aroon_oscillator"
    controller_type: str = "generic"

    candles_config: List[CandlesConfig] = Field(default_factory=list)

    connector_name: str = "kucoin"
    trading_pair: str = "XCAD-USDT"
    candles_connector_name: str = "kucoin"
    candles_trading_pair: str = "XCAD-USDT"
    candles_interval: str = "1m"

    total_amount_quote: Decimal = Decimal("100")
    order_amount: Decimal = Decimal("10")

    minimum_spread: Decimal = Decimal("0.01")
    maximum_spread: Decimal = Decimal("0.05")
    aroon_osc_strength_factor: Decimal = Decimal("0.5")

    period_length: int = 25
    period_duration: float = 60.0
    minimum_periods: int = -1

    order_levels: int = 1
    order_level_amount: Decimal = Decimal("0")
    order_level_spread: Decimal = Decimal("0.01")

    price_type: int = PriceType.MidPrice.value
    price_ceiling: Decimal = Decimal("0")
    price_floor: Decimal = Decimal("0")

    leverage: int = 1
    position_mode: PositionMode = PositionMode.HEDGE

    recreate_order_interval: float = 0.01
    order_lifetime: float = 10.0

    take_profit_long: Decimal = Decimal("0.01")
    take_profit_short: Decimal = Decimal("0.01")
    take_profit_order_lifetime: float = 1800.0

    post_cancel_delay: float = 0.01
    take_profit_respect_limits: bool = True
    
    # Путь к базе данных SQLite для расчета позиций
    database_path: str = "data/conf_v2_with_controllers_.sqlite"

    def update_markets(self, markets: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
        if self.connector_name not in markets:
            markets[self.connector_name] = set()
        markets[self.connector_name].add(self.trading_pair)
        return markets

class AroonOscillatorController(ControllerBase):
    def __init__(self, config: AroonOscillatorConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config: AroonOscillatorConfig = config
        self.config.candles_config = [
            CandlesConfig(
                connector=self.config.candles_connector_name,
                trading_pair=self.config.candles_trading_pair,
                interval=self.config.candles_interval
            )
        ]
        # Если базовый ControllerBase не предоставляет `self.log`, создаём локальный логгер.
        # Это устраняет ошибку "'AroonOscillatorController' object has no attribute 'log'".
        if not hasattr(self, "log") or getattr(self, "log") is None:
            self.log = logging.getLogger(f"AroonOscillatorController.{id(self)}")

        self.processed_data: Dict[str, object] = {
            "reference_price": Decimal("0"),
            "aroon": {},
            "adjusted_ask_spread": Decimal("0"),
            "adjusted_bid_spread": Decimal("0"),
        }
        self._last_order_time: float = 0.0
        self._last_proposed_spreads: Dict[str, Decimal] = {}
        self._last_cancel_time: float = 0.0
        self._pending_post_cancel: bool = False
        self._network_error_count: int = 0
        self._network_cooldown_until: float = 0.0
        self._max_network_errors: int = 5
        self._network_cooldown_base: float = 10.0
        self._max_creates_per_cycle: int = 10
        self._last_executor_check: float = time.time()
        self._executor_check_interval: float = 1.0
        self._decimal_cache: Dict[str, Decimal] = {
            "0": Decimal("0"), "1": Decimal("1"), "100": Decimal("100"),
            "0.5": Decimal("0.5"), "1.00000000": Decimal("1.00000000")
        }
        self._last_aroon_calculation: Optional[Dict] = None
        self._last_candles_hash: Optional[int] = None
        self._max_executors_limit: int = 500
        self._startup_time: float = time.time()
        self._startup_grace_period: float = 5.0
        self._is_ready: bool = False
        self._entry_executor_map: Dict[str, str] = {}

    def _get_decimal(self, key: str) -> Decimal:
        if key not in self._decimal_cache:
            self._decimal_cache[key] = Decimal(key)
        return self._decimal_cache[key]

    def _to_unix_ts(self, val) -> int:
        if hasattr(val, "timestamp"):
            try:
                return int(val.timestamp())
            except:
                pass
        try:
            ts = pd.to_datetime(val)
            return int(ts.timestamp())
        except:
            pass
        try:
            v = float(val)
            if v > 1e14:
                return int(v / 1e9)
            elif v > 1e11:
                return int(v / 1e3)
            else:
                return int(v)
        except:
            return 0

    def _normalize_ts(self, ts_val) -> float:
        if ts_val is None:
            return 0.0
        try:
            ts = float(ts_val)
        except Exception:
            try:
                return float(self._to_unix_ts(ts_val))
            except Exception:
                return 0.0
        if ts > 1e14:
            return ts / 1e9
        if ts > 1e11:
            return ts / 1e3
        return ts

    def _compute_aroon_from_candles(self, df: Optional[pd.DataFrame]) -> Dict[str, object]:
        res = {"aroon_up": 0.0, "aroon_down": 0.0, "aroon_osc": 0.0, "periods": 0,
               "period_start": 0, "period_end": 0, "high": 0.0, "low": 0.0}
        if df is None or df.empty:
            return res
        period = int(self.config.period_length)
        recent_data = df.iloc[-min(len(df), period):]
        try:
            current_hash = hash(recent_data.to_string())
            if (self._last_candles_hash == current_hash and
                self._last_aroon_calculation is not None):
                return self._last_aroon_calculation
        except:
            current_hash = None
        columns_lower = [c.lower() for c in df.columns]
        has_high_low = "high" in columns_lower and "low" in columns_lower
        if not has_high_low:
            return res
        df_len = len(df)
        start_idx = max(0, df_len - period)
        high_col_idx = columns_lower.index("high")
        low_col_idx = columns_lower.index("low")
        original_columns = df.columns.tolist()
        high_col_name = original_columns[high_col_idx]
        low_col_name = original_columns[low_col_idx]
        high_values = df[high_col_name].iloc[start_idx:].dropna()
        low_values = df[low_col_name].iloc[start_idx:].dropna()
        min_len = min(len(high_values), len(low_values))
        if min_len == 0:
            return res
        high_values = high_values.iloc[:min_len]
        low_values = low_values.iloc[:min_len]
        count = len(high_values)
        res["periods"] = count
        if count == 0:
            return res
        try:
            highs = high_values.astype(float).values
            lows = low_values.astype(float).values
            last_high_index = len(highs) - 1 - (highs[::-1] >= highs.max()).argmax()
            last_low_index = len(lows) - 1 - (lows[::-1] <= lows.min()).argmax()
        except:
            highs = list(high_values.astype(float).values)
            lows = list(low_values.astype(float).values)
            last_high_index = -1
            max_h = float("-inf")
            for i, v in enumerate(highs):
                if v >= max_h:
                    max_h = v
                    last_high_index = i
            last_low_index = -1
            min_l = float("inf")
            for i, v in enumerate(lows):
                if v <= min_l:
                    min_l = v
                    last_low_index = i
        if count > 0:
            aroon_up = ((last_high_index + 1) / period) * 100.0
            aroon_down = ((last_low_index + 1) / period) * 100.0
        else:
            aroon_up = 0.0
            aroon_down = 0.0
        aroon_osc = aroon_up - aroon_down
        try:
            start_idx_orig = high_values.index[0]
            end_idx_orig = high_values.index[-1]
            start_ts = self._to_unix_ts(start_idx_orig)
            end_ts = self._to_unix_ts(end_idx_orig)
        except:
            start_ts = 0
            end_ts = 0
        try:
            max_high = float(highs.max()) if hasattr(highs, 'max') else max(highs)
            min_low = float(lows.min()) if hasattr(lows, 'min') else min(lows)
        except:
            max_high = 0.0
            min_low = 0.0
        res.update({
            "aroon_up": float(aroon_up),
            "aroon_down": float(aroon_down),
            "aroon_osc": float(aroon_osc),
            "period_start": int(start_ts),
            "period_end": int(end_ts),
            "high": max_high,
            "low": min_low,
        })
        if current_hash is not None:
            self._last_candles_hash = current_hash
            self._last_aroon_calculation = res
        return res

    async def update_processed_data(self):
        try:
            price = self.market_data_provider.get_price_by_type(
                self.config.connector_name,
                self.config.trading_pair,
                PriceType(self.config.price_type)
            )
            if price is None or (isinstance(price, float) and (price != price)):
                self.processed_data["reference_price"] = self._get_decimal("0")
            else:
                self.processed_data["reference_price"] = Decimal(str(price))
            self._network_error_count = 0
        except Exception as e:
            self._network_error_count += 1
            if not self._is_ready:
                self.log.warning(f"AroonOscillatorController: startup error getting price (attempt {self._network_error_count}): {str(e)}")
            elif self._network_error_count >= self._max_network_errors:
                exponent = max(0, self._network_error_count - self._max_network_errors)
                cooldown = self._network_cooldown_base * (2 ** exponent)
                self._network_cooldown_until = time.time() + cooldown
                self.log.warning(f"AroonOscillatorController: network errors reached {self._network_error_count}, entering cooldown for {cooldown} seconds.")
            self.processed_data["reference_price"] = self._get_decimal("0")

    def _should_recreate_orders(self) -> bool:
        if time.time() < getattr(self, "_network_cooldown_until", 0.0):
            return False
        return time.time() - self._last_order_time >= float(self.config.recreate_order_interval)

    def _get_executor_timestamp(self, executor) -> float:
        ts = getattr(executor, "timestamp", None)
        if ts is not None:
            try:
                return float(self._normalize_ts(ts))
            except:
                pass
        cfg = getattr(executor, "config", None)
        if cfg is not None:
            ts2 = getattr(cfg, "timestamp", None)
            if ts2 is not None:
                try:
                    return float(self._normalize_ts(ts2))
                except:
                    pass
        created_ts = getattr(executor, "created_timestamp", None)
        if created_ts is not None:
            try:
                return float(self._normalize_ts(created_ts))
            except:
                pass
        return 0.0

    def _get_executor_level_id(self, executor) -> Optional[str]:
        level_id = None
        custom_info = getattr(executor, "custom_info", None)
        if custom_info:
            try:
                # custom_info может быть dict или SimpleNamespace
                if isinstance(custom_info, dict):
                    level_id = custom_info.get("level_id")
                else:
                    level_id = getattr(custom_info, "level_id", None)
            except:
                level_id = None
        if level_id is None:
            config = getattr(executor, "config", None)
            if config is not None:
                level_id = getattr(config, "level_id", None)
        return level_id

    # --------------------
    # Новая логика: попытка извлечь реальные исполненные объёмы из executors_info
    # и синхронизировать self.positions_held с ними.
    # Это учитывает частичные исполнения, когда executor'ы имеют заполненную часть.
    # --------------------

    # --------------------
    # НОВАЯ ЛОГИКА: Расчет позиций из базы данных SQLite
    # Читает таблицу TradeFill и вычисляет текущую позицию на основе реальных сделок
    # --------------------
    def _calculate_positions_from_database(self):
        """
        Рассчитывает позиции на основе исторических сделок из таблицы TradeFill в SQLite базе данных.
        ВАЖНО: реализован FIFO расчёт средней цены входа для оставшейся позиции,
        чтобы корректно работать после произвольного числа частичных закрытий (TP).
        """
        try:
            # Проверяем существование базы данных
            db_path = self.config.database_path
            if not os.path.exists(db_path):
                self.log.warning(f"Database file not found: {db_path}")
                self.positions_held = []
                return
            
            # Подключаемся к базе данных
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Получаем базовый и квотируемый актив из trading_pair
            if '-' in self.config.trading_pair:
                base_asset, quote_asset = self.config.trading_pair.split('-')
            elif '/' in self.config.trading_pair:
                base_asset, quote_asset = self.config.trading_pair.split('/')
            else:
                # Пытаемся угадать (например BTCUSDT -> BTC, USDT)
                if len(self.config.trading_pair) > 4:
                    base_asset = self.config.trading_pair[:-4]
                    quote_asset = self.config.trading_pair[-4:]
                else:
                    base_asset = self.config.trading_pair
                    quote_asset = "USDT"
            
            # Читаем сделки из базы данных (сортируем по времени для FIFO)
            query = """
                SELECT timestamp, trade_type, price, amount, trade_fee_in_quote
                FROM TradeFill
                WHERE market = ? AND base_asset = ? AND quote_asset = ?
                ORDER BY timestamp ASC, rowid ASC
            """
            
            cursor.execute(query, (self.config.connector_name, base_asset, quote_asset))
            trades = cursor.fetchall()
            conn.close()
            
            if not trades:
                self.log.debug(f"No trades found in database for {self.config.trading_pair}")
                self.positions_held = []
                return

            # Преобразуем и нормализуем сделки: DB хранит умноженные на 1e6
            normalized_trades = []
            for timestamp, trade_type, price_raw, amount_raw, fee_raw in trades:
                try:
                    price = Decimal(str(price_raw)) / Decimal("1000000")
                except:
                    price = Decimal(str(price_raw))
                try:
                    amount = Decimal(str(amount_raw)) / Decimal("1000000")
                except:
                    amount = Decimal(str(amount_raw))
                try:
                    fee = Decimal(str(fee_raw or 0)) / Decimal("1000000")
                except:
                    fee = Decimal(str(fee_raw or 0))
                normalized_trades.append({
                    "timestamp": timestamp,
                    "type": trade_type,
                    "price": price,
                    "amount": amount,
                    "fee": fee
                })

            # FIFO логика для расчёта оставшейся позиции и её средней цены
            # Массив лотов для BUY (каждый элемент: {"amount": Decimal, "price": Decimal})
            buy_lots: List[Dict[str, Decimal]] = []
            # Массив лотов для SELL (если шортим) - аналогично
            sell_lots: List[Dict[str, Decimal]] = []

            for tr in normalized_trades:
                ttype = tr["type"].upper()
                amount = tr["amount"]
                price = tr["price"]

                if amount <= 0:
                    continue

                if ttype == "BUY":
                    # Попытка закрыть существующие sell_lots (если были шорты) по FIFO
                    remaining = amount
                    while remaining > 0 and sell_lots:
                        lot = sell_lots[0]
                        lot_amt = lot["amount"]
                        if lot_amt <= remaining:
                            # закрываем весь lot
                            remaining -= lot_amt
                            sell_lots.pop(0)
                        else:
                            # частично закрываем lot
                            lot["amount"] = lot_amt - remaining
                            remaining = Decimal("0")
                    # если что-то осталось — добавляем как новый buy lot
                    if remaining > 0:
                        buy_lots.append({"amount": remaining, "price": price})
                elif ttype == "SELL":
                    # Аналогично: закрываем buy_lots FIFO
                    remaining = amount
                    while remaining > 0 and buy_lots:
                        lot = buy_lots[0]
                        lot_amt = lot["amount"]
                        if lot_amt <= remaining:
                            remaining -= lot_amt
                            buy_lots.pop(0)
                        else:
                            lot["amount"] = lot_amt - remaining
                            remaining = Decimal("0")
                    # если осталось => это открытый шорт (sell_lots)
                    if remaining > 0:
                        sell_lots.append({"amount": remaining, "price": price})
                else:
                    # неизвестный тип — пропускаем
                    continue

            # Сформируем positions_held из оставшихся лотов
            new_positions = []
            # Длинная позиция (есть buy_lots)
            if buy_lots:
                total_amount = sum(l["amount"] for l in buy_lots)
                total_quote = sum(l["amount"] * l["price"] for l in buy_lots)
                avg_price = (total_quote / total_amount) if total_amount > 0 else None
                pos = SimpleNamespace()
                setattr(pos, "trading_pair", self.config.trading_pair)
                setattr(pos, "amount", total_amount)
                setattr(pos, "amount_quote", total_quote)
                setattr(pos, "side", TradeType.BUY)
                setattr(pos, "entry_price", avg_price)
                setattr(pos, "avg_entry_price", avg_price)
                new_positions.append(pos)
            # Короткая позиция (есть sell_lots)
            if sell_lots:
                total_amount = sum(l["amount"] for l in sell_lots)
                total_quote = sum(l["amount"] * l["price"] for l in sell_lots)
                avg_price = (total_quote / total_amount) if total_amount > 0 else None
                pos = SimpleNamespace()
                setattr(pos, "trading_pair", self.config.trading_pair)
                setattr(pos, "amount", total_amount)
                setattr(pos, "amount_quote", total_quote)
                setattr(pos, "side", TradeType.SELL)
                setattr(pos, "entry_price", avg_price)
                setattr(pos, "avg_entry_price", avg_price)
                new_positions.append(pos)

            # Обновляем positions_held
            self.positions_held = new_positions

            # Логируем результат
            if new_positions:
                for pos in new_positions:
                    side = getattr(pos, "side", "")
                    amount = getattr(pos, "amount", 0)
                    avg_price = getattr(pos, "avg_entry_price", 0)
                    self.log.info(f"Position from DB (FIFO): {self.config.trading_pair} {side} "
                                f"amount={amount}, avg_price={avg_price}")
            else:
                self.log.debug(f"No net position for {self.config.trading_pair} after FIFO processing")
                
        except Exception as e:
            self.log.error(f"Error calculating positions from database: {str(e)}")
            import traceback
            self.log.error(traceback.format_exc())
            if not hasattr(self, "positions_held"):
                self.positions_held = []

    # --------------------
    # Конец новой логики
    # --------------------

    # --------------------
    # НОВАЯ ФУНКЦИЯ: Динамический расчет размера ордера
    # Учитывает total_amount_quote, активные ордера и средства в тейк-профитах
    # --------------------
    def _calculate_dynamic_order_amount(self) -> Decimal:
        """
        Динамически рассчитывает максимально доступный размер ордера в USDT.
        
        Логика:
        1. Берем total_amount_quote как общий лимит капитала
        2. Вычитаем средства, занятые в активных ордерах на открытие
        3. Вычитаем средства, занятые в тейк-профит ордерах
        4. Возвращаем минимум из (доступный_капитал, order_amount)
        5. Если результат < 5 USDT, возвращаем 0 (не создаем ордер)
        """
        try:
            total_capital = Decimal(str(self.config.total_amount_quote))
            configured_order_amount = Decimal(str(self.config.order_amount))
            min_order_usdt = Decimal("5")  # Минимальный размер ордера 5 USDT
            
            # Рассчитываем занятые средства в активных ордерах
            capital_in_active_orders = Decimal("0")
            capital_in_tp_orders = Decimal("0")
            
            for executor in self.executors_info:
                try:
                    # Проверяем, что executor принадлежит этому контроллеру
                    if getattr(executor, "controller_id", None) != self.config.id:
                        continue
                    
                    # Проверяем, что executor активен
                    is_active = getattr(executor, "is_active", False)
                    if not is_active:
                        continue
                    
                    # Получаем level_id
                    level_id = self._get_executor_level_id(executor)
                    
                    # Получаем конфигурацию executor
                    config = getattr(executor, "config", None)
                    if config is None:
                        continue
                    
                    # Получаем цену и количество
                    price = getattr(config, "price", None)
                    amount = getattr(config, "amount", None)
                    
                    if price is None or amount is None:
                        continue
                    
                    price_dec = Decimal(str(price))
                    amount_dec = Decimal(str(amount))
                    
                    # Рассчитываем объем в quote валюте
                    order_value_quote = price_dec * amount_dec
                    
                    # Определяем тип ордера и добавляем к соответствующей категории
                    if level_id in {"tp_long", "tp_short"}:
                        # Это тейк-профит ордер
                        capital_in_tp_orders += order_value_quote
                    elif isinstance(level_id, str) and (level_id.startswith("aroon_buy") or level_id.startswith("aroon_sell")):
                        # Это основной ордер на открытие позиции
                        capital_in_active_orders += order_value_quote
                        
                except Exception as e:
                    # Игнорируем ошибки для отдельных executors
                    continue
            
            # Рассчитываем доступный капитал
            capital_used = capital_in_active_orders + capital_in_tp_orders
            available_capital = total_capital - capital_used
            
            # Определяем итоговый размер ордера
            if available_capital <= 0:
                self.log.warning(f"No available capital: total_capital={total_capital}, "
                               f"capital_used={capital_used}")
                return Decimal("0")
            
            # Выбираем минимум из доступного капитала и сконфигурированного order_amount
            dynamic_order_amount = min(available_capital, configured_order_amount)
            
            # Проверяем минимальный размер ордера (5 USDT)
            if dynamic_order_amount < min_order_usdt:
                return Decimal("0")
            
            return dynamic_order_amount
            
        except Exception as e:
            self.log.error(f"Error calculating dynamic order amount: {str(e)}")
            import traceback
            self.log.error(traceback.format_exc())
            # В случае ошибки возвращаем сконфигурированное значение
            return Decimal(str(self.config.order_amount))
    
    # --------------------
    # Конец новой функции
    # --------------------

    def _create_take_profit_action(self, entry_price: Decimal, trading_pair: str, side: TradeType) -> Optional[CreateExecutorAction]:
        if side == TradeType.BUY:
            tp_pct = self.config.take_profit_long
            pos_side = TradeType.BUY
            tp_side = TradeType.SELL
            level_id = "tp_long"
        else:
            tp_pct = self.config.take_profit_short
            pos_side = TradeType.SELL
            tp_side = TradeType.BUY
            level_id = "tp_short"
        try:
            tp_pct = Decimal(str(tp_pct))
        except:
            tp_pct = self._get_decimal("0")
        if tp_pct <= 0:
            return None
        filled_amount = self._get_decimal("0")
        avg_entry_price = None
        for pos in self.positions_held:
            try:
                pos_trading_pair = getattr(pos, "trading_pair", None)
                pos_side_attr = getattr(pos, "side", None)
                pos_amount = getattr(pos, "amount", 0)
                if (pos_trading_pair == trading_pair and
                    pos_side_attr == pos_side and
                    pos_amount > 0):
                    filled_amount = Decimal(str(pos_amount))
                    avg_price = (getattr(pos, "entry_price", None) or
                                getattr(pos, "avg_entry_price", None))
                    if avg_price is not None:
                        avg_entry_price = Decimal(str(avg_price))
                    else:
                        amt_q = getattr(pos, "amount_quote", None)
                        if amt_q and pos_amount:
                            try:
                                avg_entry_price = Decimal(str(amt_q)) / Decimal(str(pos_amount))
                            except:
                                avg_entry_price = None
                    break
            except:
                continue
        if filled_amount <= 0:
            return None
        # --- исправленный расчёт цены входа и тейк-профита ---
        if avg_entry_price is None or avg_entry_price == 0:
            # если позиция в базе ещё не обновилась, берём текущую цену входа из ордера
            avg_entry_price = Decimal(str(entry_price))

        one = self._get_decimal("1")
        quantize_precision = self._get_decimal("1.00000000")

        if tp_side == TradeType.SELL:
            # TP для BUY позиции — цена продажи должна быть выше входа
            tp_price = (avg_entry_price * (one + tp_pct)).quantize(quantize_precision)
            # защита от убытка при рыночных скачках
            min_tp_price = (avg_entry_price * Decimal("1.0001")).quantize(quantize_precision)
            if tp_price < min_tp_price:
                tp_price = min_tp_price
        else:
            # TP для SELL позиции — цена покупки должна быть ниже входа
            tp_price = (avg_entry_price * (one - tp_pct)).quantize(quantize_precision)
            # защита от убытка при резких движениях
            max_tp_price = (avg_entry_price * Decimal("0.9999")).quantize(quantize_precision)
            if tp_price > max_tp_price:
                tp_price = max_tp_price
        if self.config.take_profit_respect_limits:
            try:
                price_floor = Decimal(str(self.config.price_floor))
                if (tp_side == TradeType.SELL and
                    price_floor != self._get_decimal("0") and
                    tp_price < price_floor):
                    return None
            except:
                pass
            try:
                price_ceiling = Decimal(str(self.config.price_ceiling))
                if (tp_side == TradeType.BUY and
                    price_ceiling != self._get_decimal("0") and
                    tp_price > price_ceiling):
                    return None
            except:
                pass
        executor_cfg = OrderExecutorConfig(
            timestamp=time.time(),
            level_id=level_id,
            connector_name=self.config.connector_name,
            trading_pair=trading_pair,
            price=tp_price,
            amount=filled_amount,
            execution_strategy=ExecutionStrategy.LIMIT_MAKER,
            leverage=int(self.config.leverage),
            side=tp_side,
            position_action=PositionAction.CLOSE
        )
        return CreateExecutorAction(controller_id=self.config.id, executor_config=executor_cfg)

    def determine_executor_actions(self) -> List[ExecutorAction]:
        actions: List[ExecutorAction] = []
        now = time.time()
        if not self._is_ready and (now - self._startup_time) < self._startup_grace_period:
            return []
        elif not self._is_ready:
            self._is_ready = True
            self.log.info("AroonOscillatorController startup complete - ready for operation")
        if now - self._last_executor_check < self._executor_check_interval:
            return []
        self._last_executor_check = now

        # --- Синхронизируем позиции с частичными исполнениями в executors (важно: делаем это РАНО, до принятия решений) ---
        # Это ключевая правка: позволяет корректно определить, что позиция открыта даже при дробных fill'ах.
        try:
            self._calculate_positions_from_database()
        except Exception:
            # не критично — продолжим, но логируем
            try:
                self.log.exception("Error during position reconciliation")
            except:
                pass

        if len(self.executors_info) > self._max_executors_limit:
            active_execs = [ex for ex in self.executors_info if getattr(ex, "is_active", False)]
            inactive_execs = [ex for ex in self.executors_info if not getattr(ex, "is_active", False)]
            if len(inactive_execs) > 100:
                inactive_execs = inactive_execs[-100:]
            self.executors_info = active_execs + inactive_execs
            self.log.info(f"Executor cleanup: kept {len(active_execs)} active and {len(inactive_execs)} inactive executors")
        try:
            lifetime = float(self.config.order_lifetime)
        except:
            lifetime = 0.0
        try:
            tp_lifetime = float(self.config.take_profit_order_lifetime)
        except:
            tp_lifetime = 0.0
        main_executors_to_stop = []
        tp_executors_to_stop = []
        existing_tp_by_lvl = {}
        existing_tp_filled: Dict[str, Decimal] = {}
        # Собираем существующие активные основные ордера для проверки (используется позднее, чтобы запретить пересоздание при наличии активных)
        active_main_count = 0
        for executor in self.executors_info:
            if getattr(executor, "controller_id", None) != self.config.id:
                continue
            level_id = self._get_executor_level_id(executor)
            is_active = getattr(executor, "is_active", False)
            if level_id in {"tp_long", "tp_short"}:
                existing_tp_by_lvl[level_id] = executor
                # получим текущее заполнение этого TP (если доступно)
                try:
                    fi = self._extract_filled_from_executor(executor)
                    if fi and fi.get("filled", None):
                        existing_tp_filled[level_id] = Decimal(str(fi.get("filled")))
                    else:
                        existing_tp_filled[level_id] = Decimal("0")
                except:
                    existing_tp_filled[level_id] = Decimal("0")
            if not is_active:
                continue
            # считаем активные основные (aroon_buy/aroon_sell)
            if isinstance(level_id, str) and (level_id.startswith("aroon_buy") or level_id.startswith("aroon_sell")):
                active_main_count += 1
            ts = self._get_executor_timestamp(executor)
            # ОТМЕНА ТОЛЬКО ПО ИСТЕЧЕНИЮ ВРЕМЕНИ ЖИЗНИ
            if (level_id and
                level_id not in {"tp_long", "tp_short"} and
                not (isinstance(level_id, str) and level_id.startswith("tp_")) and
                lifetime > 0 and
                ts > 0 and
                (now - ts >= lifetime)):
                main_executors_to_stop.append(executor)
            elif (level_id in {"tp_long", "tp_short"} and
                  tp_lifetime > 0 and
                  ts > 0 and
                  (now - ts >= tp_lifetime)):
                # ИСПРАВЛЕНИЕ: при отмене TP НЕ закрываем позицию market-order'ом
                # (раньше использовалось keep_position=False, что могло привести к рыночному закрытию позиции)
                tp_executors_to_stop.append(executor)
        for executor in main_executors_to_stop:
            actions.append(StopExecutorAction(
                controller_id=self.config.id,
                executor_id=executor.id,
                keep_position=True
            ))
        # Для TP отмен используем keep_position=True, чтобы не инициировать market-close (и тем самым не получить taker)
        for executor in tp_executors_to_stop:
            actions.append(StopExecutorAction(
                controller_id=self.config.id,
                executor_id=executor.id,
                keep_position=True
            ))
        # ПЕРЕСОЗДАНИЕ ОРДЕРОВ ТОЛЬКО ПО ИСТЕЧЕНИЮ ИНТЕРВАЛА И ЕСЛИ НЕТ АКТИВНЫХ ОСНОВНЫХ ОРДЕРОВ
        # (это предотвращает постоянное пересоздавание/отмену, когда есть активные основные ордера)
        if not main_executors_to_stop and not tp_executors_to_stop:
            # если есть активные основные ордера — не пересоздаём сетку
            if active_main_count > 0:
                # есть активные BUY/SELL (основные) — отказываемся пересоздавать сейчас
                return actions
            if not self._should_recreate_orders():
                return actions
            # обновляем метку начала пересоздания — запомним время, когда мы действительно приступаем к созданию новых
            self._last_order_time = now
            candles = None
            try:
                candles = self.market_data_provider.get_candles_df(
                    self.config.candles_connector_name,
                    self.config.candles_trading_pair,
                    self.config.candles_interval
                )
                self._network_error_count = 0
            except Exception as e:
                self._network_error_count += 1
                if not self._is_ready:
                    self.log.warning(f"AroonOscillatorController: startup error getting candles (attempt {self._network_error_count}): {str(e)}")
                    candles = None
                elif self._network_error_count >= self._max_network_errors:
                    exponent = max(0, self._network_error_count - self._max_network_errors)
                    cooldown = self._network_cooldown_base * (2 ** exponent)
                    self._network_cooldown_until = time.time() + cooldown
                    self.log.warning(f"AroonOscillatorController: error getting candles, entering cooldown for {cooldown} seconds.")
                    candles = None
                else:
                    candles = None
            aroon = self._compute_aroon_from_candles(candles)
            self.processed_data["aroon"] = aroon
            ref_price = self.processed_data.get("reference_price", self._get_decimal("0"))
            if ref_price == 0:
                return actions
            min_spread = Decimal(str(self.config.minimum_spread))
            max_spread = Decimal(str(self.config.maximum_spread))
            diff = max_spread - min_spread
            half = self._get_decimal("0.5")
            one = self._get_decimal("1")
            hundred = self._get_decimal("100")
            ask_spread = bid_spread = min_spread + diff * half
            if aroon["periods"] >= int(self.config.minimum_periods):
                aroon_up_dec = Decimal(str(aroon["aroon_up"])) / hundred
                aroon_down_dec = Decimal(str(aroon["aroon_down"])) / hundred
                aroon_osc_dec = Decimal(str(aroon["aroon_osc"])) / hundred
                ask_increase = diff * (one - aroon_up_dec)
                bid_increase = diff * (one - aroon_down_dec)
                trend_factor = aroon_osc_dec * Decimal(str(self.config.aroon_osc_strength_factor))
                ask_spread = (min_spread + ask_increase) * (one + trend_factor)
                bid_spread = (min_spread + bid_increase) * (one - trend_factor)
                ask_spread = max(min_spread, min(ask_spread, max_spread))
                bid_spread = max(min_spread, min(bid_spread, max_spread))
            quantize_precision = self._get_decimal("1.00000000")
            base_buy_price = (ref_price * (one - bid_spread)).quantize(quantize_precision)
            base_sell_price = (ref_price * (one + ask_spread)).quantize(quantize_precision)
            allow_buy = True
            allow_sell = True
            try:
                price_ceiling = Decimal(str(self.config.price_ceiling))
                if price_ceiling != self._get_decimal("0") and base_buy_price > price_ceiling:
                    allow_buy = False
            except:
                pass
            try:
                price_floor = Decimal(str(self.config.price_floor))
                if price_floor != self._get_decimal("0") and base_sell_price < price_floor:
                    allow_sell = False
            except:
                pass
            created_this_cycle = 0
            order_levels = int(self.config.order_levels)
            level_spread_dec = Decimal(str(self.config.order_level_spread))
            
            # ---- ДИНАМИЧЕСКИЙ РАСЧЕТ РАЗМЕРА ОРДЕРА ----
            # Вместо статического order_amount используем динамический расчет
            dynamic_order_amount_usdt = self._calculate_dynamic_order_amount()
            
            # Если динамический размер = 0, пропускаем создание ордеров
            if dynamic_order_amount_usdt <= 0:
                return actions
            
            # Используем dynamic_order_amount_usdt вместо order_amount_dec
            order_amount_dec = dynamic_order_amount_usdt
            level_amount_dec = Decimal(str(self.config.order_level_amount))
            
            # ---- BUY ----
            if allow_buy:
                for level_index in range(order_levels):
                    if created_this_cycle >= self._max_creates_per_cycle:
                        self.log.info("Max create-per-cycle reached, skipping BUY levels.")
                        break
                    if time.time() >= getattr(self, "_network_cooldown_until", 0.0):
                        level_price = (base_buy_price * (one - level_spread_dec * Decimal(str(level_index)))).quantize(quantize_precision)
                        if level_amount_dec > 0:
                            level_amount_base = level_amount_dec
                        else:
                            try:
                                level_amount_base = (order_amount_dec / level_price).quantize(quantize_precision)
                            except:
                                level_amount_base = self._get_decimal("0")
                        if level_amount_base > 0:
                            buy_executor = OrderExecutorConfig(
                                timestamp=time.time(),
                                level_id=f"aroon_buy_{level_index+1}",
                                connector_name=self.config.connector_name,
                                trading_pair=self.config.trading_pair,
                                price=level_price,
                                amount=level_amount_base,
                                execution_strategy=ExecutionStrategy.LIMIT_MAKER,
                                leverage=int(self.config.leverage),
                                side=TradeType.BUY,
                                position_action=PositionAction.OPEN
                            )
                            actions.append(CreateExecutorAction(controller_id=self.config.id, executor_config=buy_executor))
                            created_this_cycle += 1
                            self._last_proposed_spreads[f"aroon_buy_{level_index+1}"] = bid_spread
                    else:
                        self.log.info("Network cooldown active — skipping BUY create actions.")
                        break
            # ---- SELL ----
            if allow_sell:
                for level_index in range(order_levels):
                    if created_this_cycle >= self._max_creates_per_cycle:
                        self.log.info("Max create-per-cycle reached, skipping SELL levels.")
                        break
                    if time.time() >= getattr(self, "_network_cooldown_until", 0.0):
                        level_price = (base_sell_price * (one + level_spread_dec * Decimal(str(level_index)))).quantize(quantize_precision)
                        if level_amount_dec > 0:
                            level_amount_base = level_amount_dec
                        else:
                            try:
                                level_amount_base = (order_amount_dec / level_price).quantize(quantize_precision)
                            except:
                                level_amount_base = self._get_decimal("0")
                        if level_amount_base > 0:
                            sell_executor = OrderExecutorConfig(
                                timestamp=time.time(),
                                level_id=f"aroon_sell_{level_index+1}",
                                connector_name=self.config.connector_name,
                                trading_pair=self.config.trading_pair,
                                price=level_price,
                                amount=level_amount_base,
                                execution_strategy=ExecutionStrategy.LIMIT_MAKER,
                                leverage=int(self.config.leverage),
                                side=TradeType.SELL,
                                position_action=PositionAction.OPEN
                            )
                            actions.append(CreateExecutorAction(controller_id=self.config.id, executor_config=sell_executor))
                            created_this_cycle += 1
                            self._last_proposed_spreads[f"aroon_sell_{level_index+1}"] = ask_spread
                    else:
                        self.log.info("Network cooldown active — skipping SELL create actions.")
                        break
            self.processed_data["adjusted_ask_spread"] = ask_spread
            self.processed_data["adjusted_bid_spread"] = bid_spread
        # СОЗДАНИЕ ТЕЙК-ПРОФИТОВ
        zero_dec = self._get_decimal("0")
        for pos in self.positions_held:
            try:
                if getattr(pos, "trading_pair", None) != self.config.trading_pair:
                    continue
                pos_side = getattr(pos, "side", None)
                if pos_side == TradeType.BUY:
                    target_lvl = "tp_long"
                    tp_exists = target_lvl in existing_tp_by_lvl
                    # ENTRY PRICE для TP — берем из позиции
                    entry_price_attr = getattr(pos, "entry_price", None)
                    if entry_price_attr is not None:
                        entry_price_for_tp = Decimal(str(entry_price_attr))
                    else:
                        amount = getattr(pos, "amount", 1)
                        amount_quote = getattr(pos, "amount_quote", 0)
                        if amount and amount_quote:
                            entry_price_for_tp = Decimal(str(amount_quote)) / Decimal(str(amount))
                        else:
                            entry_price_for_tp = zero_dec
                    # Сформируем действие на TP для текущего заполненного объема позиции (если есть)
                    tp_act = self._create_take_profit_action(
                        entry_price=entry_price_for_tp,
                        trading_pair=self.config.trading_pair,
                        side=TradeType.BUY
                    )
                    if tp_act:
                        if tp_exists:
                            try:
                                ex = existing_tp_by_lvl[target_lvl]
                                ex_cfg = getattr(ex, "config", None)
                                existing_price = getattr(ex_cfg, "price", None) if ex_cfg else None
                                existing_amount_cfg = getattr(ex_cfg, "amount", None) if ex_cfg else None
                                # смотрим реальное заполнение existing TP
                                existing_filled = existing_tp_filled.get(target_lvl, Decimal("0"))
                                # выясняем остаток у существующего TP (если cfg.amount задан)
                                existing_remaining = None
                                try:
                                    if existing_amount_cfg is not None:
                                        existing_remaining = Decimal(str(existing_amount_cfg)) - Decimal(str(existing_filled))
                                        if existing_remaining < 0:
                                            existing_remaining = Decimal("0")
                                except:
                                    existing_remaining = None
                                new_price = tp_act.executor_config.price
                                new_amount = tp_act.executor_config.amount
                                # если у existing есть только частичное исполнение и остался remainder > 0,
                                # мы хотим создать TP на оставшийся остаток (new_amount может уже это отражать).
                                # определим: price_changed / amount_changed (учитывая существующее фактическое заполнение)
                                price_changed = (existing_price is None or
                                               Decimal(str(existing_price)) != Decimal(str(new_price)))
                                # если existing_amount_cfg задан — сравниваем remaining vs new_amount
                                amount_changed = False
                                if existing_amount_cfg is not None:
                                    try:
                                        # существующий общий объём, но часть исполнена -> остаток = existing_amount_cfg - existing_filled
                                        if existing_remaining is not None:
                                            # сравним new_amount с existing_remaining (если new_amount отличается => пересоздать)
                                            amount_changed = (Decimal(str(new_amount)) != Decimal(str(existing_remaining)))
                                        else:
                                            amount_changed = (Decimal(str(existing_amount_cfg)) != Decimal(str(new_amount)))
                                    except:
                                        amount_changed = True
                                else:
                                    # если cfg.amount не задан в existing, сравниваем напрямую
                                    try:
                                        amount_changed = (Decimal(str(existing_filled)) != Decimal(str(new_amount)))
                                    except:
                                        amount_changed = True
                                if price_changed or amount_changed:
                                    # отменим старый TP (keep_position=True чтобы не market-close) и создадим новый
                                    actions.append(StopExecutorAction(
                                        controller_id=self.config.id,
                                        executor_id=ex.id,
                                        keep_position=True
                                    ))
                                    actions.append(tp_act)
                                    existing_tp_by_lvl.pop(target_lvl, None)
                                else:
                                    # если не изменилось — ничего не делаем
                                    pass
                            except Exception:
                                # если что-то упало — всё-таки попробуем создать новый TP
                                actions.append(tp_act)
                        else:
                            actions.append(tp_act)
                elif pos_side == TradeType.SELL:
                    target_lvl = "tp_short"
                    tp_exists = target_lvl in existing_tp_by_lvl
                    entry_price_attr = getattr(pos, "entry_price", None)
                    if entry_price_attr is not None:
                        entry_price_for_tp = Decimal(str(entry_price_attr))
                    else:
                        amount = getattr(pos, "amount", 1)
                        amount_quote = getattr(pos, "amount_quote", 0)
                        if amount and amount_quote:
                            entry_price_for_tp = Decimal(str(amount_quote)) / Decimal(str(amount))
                        else:
                            entry_price_for_tp = zero_dec
                    tp_act = self._create_take_profit_action(
                        entry_price=entry_price_for_tp,
                        trading_pair=self.config.trading_pair,
                        side=TradeType.SELL
                    )
                    if tp_act:
                        if tp_exists:
                            try:
                                ex = existing_tp_by_lvl[target_lvl]
                                ex_cfg = getattr(ex, "config", None)
                                existing_price = getattr(ex_cfg, "price", None) if ex_cfg else None
                                existing_amount_cfg = getattr(ex_cfg, "amount", None) if ex_cfg else None
                                existing_filled = existing_tp_filled.get(target_lvl, Decimal("0"))
                                existing_remaining = None
                                try:
                                    if existing_amount_cfg is not None:
                                        existing_remaining = Decimal(str(existing_amount_cfg)) - Decimal(str(existing_filled))
                                        if existing_remaining < 0:
                                            existing_remaining = Decimal("0")
                                except:
                                    existing_remaining = None
                                new_price = tp_act.executor_config.price
                                new_amount = tp_act.executor_config.amount
                                price_changed = (existing_price is None or
                                               Decimal(str(existing_price)) != Decimal(str(new_price)))
                                amount_changed = False
                                if existing_amount_cfg is not None:
                                    try:
                                        if existing_remaining is not None:
                                            amount_changed = (Decimal(str(new_amount)) != Decimal(str(existing_remaining)))
                                        else:
                                            amount_changed = (Decimal(str(existing_amount_cfg)) != Decimal(str(new_amount)))
                                    except:
                                        amount_changed = True
                                else:
                                    try:
                                        amount_changed = (Decimal(str(existing_filled)) != Decimal(str(new_amount)))
                                    except:
                                        amount_changed = True
                                if price_changed or amount_changed:
                                    actions.append(StopExecutorAction(
                                        controller_id=self.config.id,
                                        executor_id=ex.id,
                                        keep_position=True
                                    ))
                                    actions.append(tp_act)
                                    existing_tp_by_lvl.pop(target_lvl, None)
                                else:
                                    pass
                            except Exception:
                                actions.append(tp_act)
                        else:
                            actions.append(tp_act)
            except Exception:
                try:
                    self.log.exception("Error processing position for TP creation")
                except:
                    pass
        
        # --- СБРОС ЦИКЛА ПОСЛЕ ПОЛНОГО ИСПОЛНЕНИЯ TP (с защитой от частичных fill) ---
        try:
            # Получаем список executor'ов, которые размечены как tp_long / tp_short
            tp_executors = [
                ex for ex in self.executors_info
                if self._get_executor_level_id(ex) in {"tp_long", "tp_short"}
            ]

            # Все TP неактивны (исполнены или отменены)
            all_tp_inactive = all(not getattr(ex, "is_active", False) for ex in tp_executors)

            # Есть ли частично исполненные TP (активные и с filled_amount > 0)
            partially_filled = any(
                (getattr(ex, "filled_amount", 0) or 0) > 0 and getattr(ex, "is_active", False)
                for ex in tp_executors
            )

            # Сбрасываем цикл только если:
            #  - все TP неактивны (т.е. завершены/отменены),
            #  - текущая positions_held пуста (позиция закрыта),
            #  - и нет частично исполненных TP (частичное исполнение -> ждем)
            if all_tp_inactive and not getattr(self, "positions_held", []) and not partially_filled:
                self.log.info("✅ Все тейк-профиты полностью исполнены — начинаем новый торговый цикл.")
                # Сбрасываем таймер создания ордеров, чтобы пересоздание началось с нуля
                self._last_order_time = 0
                # Пересчитываем позиции (синхронизация с базой) — гарантируем чистое состояние
                self._calculate_positions_from_database()
                # ВАЖНО для v2.10: Очищаем entry_executor_map при завершении цикла
                self._entry_executor_map.clear()
            elif partially_filled:
                self.log.info("⚠️ Обнаружен частично исполненный тейк-профит — ожидаем полного закрытия перед новым циклом.")
        except Exception as e:
            self.log.warning(f"Ошибка при проверке окончания цикла TP: {e}")

        return actions

    async def stop_actions_proposal(self) -> List[StopExecutorAction]:
        """
        Предлагает действия по остановке executors при изменении состояния.
        """
        try:
            if self._pending_post_cancel:
                now = time.time()
                if now - self._last_cancel_time >= float(self.config.post_cancel_delay):
                    self._pending_post_cancel = False
        except Exception as e:
            self.log.warning(f"Ошибка при проверке отложенной отмены: {e}")
        return []

    async def on_start(self):
        """
        Переопределяем on_start для инициализации контроллера
        """
        try:
            self._is_ready = False
            self._startup_time = time.time()
            self.log.info("AroonOscillatorController starting up...")
            
            # Инициализируем positions_held
            if not hasattr(self, "positions_held"):
                self.positions_held = []
            
            # Загружаем позиции из базы данных
            try:
                self._calculate_positions_from_database()
            except Exception as e:
                self.log.warning(f"Не удалось загрузить позиции при старте: {e}")
                self.positions_held = []
        except Exception as e:
            self.log.warning(f"Ошибка при инициализации контроллера: {e}")

    async def on_stop_executor(self, executor_id: str, executor):
        """
        Вызывается после остановки executor'а.
        """
        try:
            if getattr(executor, "controller_id", None) != self.config.id:
                return
            level_id = self._get_executor_level_id(executor)
            # если был отменён основной ордер (не TP), можем обновить флаг post_cancel задержки
            if level_id and level_id not in {"tp_long", "tp_short"}:
                self._last_cancel_time = time.time()
                self._pending_post_cancel = True
                
                # ВАЖНО для v2.10: Удаляем из entry_executor_map при остановке
                if level_id in self._entry_executor_map:
                    del self._entry_executor_map[level_id]
                elif level_id.startswith("aroon_buy"):
                    if "aroon_buy" in self._entry_executor_map:
                        del self._entry_executor_map["aroon_buy"]
                elif level_id.startswith("aroon_sell"):
                    if "aroon_sell" in self._entry_executor_map:
                        del self._entry_executor_map["aroon_sell"]
        except Exception as e:
            self.log.warning(f"Ошибка в on_stop_executor: {e}")

    async def on_executor_filled(self, executor_id: str, executor):
        """
        Вызывается после полного или частичного исполнения ордера.
        
        Обновляем позиции из базы данных, чтобы позиции были актуальными
        перед созданием TP ордеров.
        """
        try:
            if getattr(executor, "controller_id", None) != self.config.id:
                return
            
            level_id = self._get_executor_level_id(executor)
            if level_id and (level_id.startswith("aroon_buy") or level_id.startswith("aroon_sell")):
                # Основной ордер исполнен - обновляем позиции
                self.log.info(f"Order {level_id} filled, updating positions from database")
                
                # ВАЖНО для v2.10: Сохраняем executor_id для связи с TP
                if level_id.startswith("aroon_buy"):
                    self._entry_executor_map["aroon_buy"] = executor_id
                elif level_id.startswith("aroon_sell"):
                    self._entry_executor_map["aroon_sell"] = executor_id
                
                # добавляем небольшую задержку, чтобы база успела записать сделку
                import asyncio
                await asyncio.sleep(0.3)
                self._calculate_positions_from_database()
            
        except Exception as e:
            self.log.warning(f"Ошибка в on_executor_filled: {e}")

    def on_stop(self):
        """
        Переопределяем стандартное поведение при выключении контроллера:
        отменяем все активные ордера без рыночного закрытия позиций.
        """
        try:
            self.log.info("on_stop() — отмена активных ордеров без рыночного закрытия позиций")
            for executor in getattr(self, "executors_info", []):
                if getattr(executor, "is_active", False):
                    action = StopExecutorAction(
                        controller_id=self.config.id,
                        executor_id=executor.id,
                        keep_position=True  # <-- ключевой момент
                    )
                    self.strategy_action_queue.put_nowait(action)
        except Exception as e:
            self.log.warning(f"Ошибка при on_stop: {e}")

    def to_format_status(self) -> List[str]:
        ref_price = self.processed_data.get("reference_price", self._get_decimal("0"))
        aroon = self.processed_data.get("aroon", {})
        ask_spread = self.processed_data.get("adjusted_ask_spread", self._get_decimal("0"))
        bid_spread = self.processed_data.get("adjusted_bid_spread", self._get_decimal("0"))
        lines: List[str] = []
        lines.append(f"Контроллер: {self.config.controller_name}")
        lines.append(f"Пара: {self.config.trading_pair}")
        lines.append(f"Опорная цена: {ref_price:.8f}")
        lines.append("")
        lines.append("=== Индикаторы Aroon ===")
        lines.append(f"Aroon Up = {aroon.get('aroon_up', 0):.2f}")
        lines.append(f"Aroon Down = {aroon.get('aroon_down', 0):.2f}")
        lines.append(f"Aroon Osc = {aroon.get('aroon_osc', 0):.2f}")
        full_flag = (aroon.get("periods", 0) >= int(self.config.period_length))
        lines.append(f"Aroon Indicator Full = {full_flag}, Total periods {aroon.get('periods', 0)}")
        lines.append(f"Current Period (start: {aroon.get('period_start', 0)}, end: {aroon.get('period_end', 0)}, high: {aroon.get('high', 0):.6f}, low: {aroon.get('low', 0):.6f})")
        lines.append(f"Adjusted Ask Spread = {ask_spread * self._get_decimal('100'):.2f}%")
        lines.append(f"Adjusted Bid Spread = {bid_spread * self._get_decimal('100'):.2f}%")
        lines.append("")
        lines.append("=== Ограничения по цене ===")
        lines.append(f"price_ceiling: {self.config.price_ceiling}")
        lines.append(f"price_floor:  {self.config.price_floor}")
        lines.append("")
        exec_lines: List[str] = []
        exec_lines.append("=== Executors (активные у контроллера) ===")
        header = f"{'ID':<12} | {'Сторона':<6} | {'Цена':>12} | {'Кол-во':>12} | {'Спред':>9}"
        exec_lines.append(header)
        exec_lines.append("-" * len(header))
        active_count = 0
        for executor in self.executors_info:
            try:
                if getattr(executor, "controller_id", None) != self.config.id:
                    continue
                level_id = self._get_executor_level_id(executor)
                if not getattr(executor, "is_active", False):
                    continue
                state = getattr(executor, "state", None)
                if state is not None and state != "ACTIVE":
                    continue
                if level_id is not None:
                    valid = (level_id in {"aroon_buy", "aroon_sell", "tp_long", "tp_short"} or
                           (isinstance(level_id, str) and
                            (level_id.startswith("aroon_buy_") or level_id.startswith("aroon_sell_"))))
                    if not valid:
                        continue
                    cfg = getattr(executor, "config", None)
                    if cfg:
                        price = getattr(cfg, "price", None)
                        amount = getattr(cfg, "amount", None)
                        # универсальный side_name
                        try:
                            side_name = getattr(cfg.side, "name", "")
                        except:
                            side_name = getattr(cfg, "side", "")
                        spread = self._last_proposed_spreads.get(level_id)
                        spread_str = f"{(spread * self._get_decimal('100')):.2f}%" if spread else "-"
                        executor_id = getattr(executor, "id", "")
                        exec_lines.append(f"{str(executor_id)[:10]:<12} | {str(side_name):<6} | {price:>12} | {amount:>12} | {spread_str:>9}")
                        active_count += 1
                        if active_count >= 50:
                            exec_lines.append("... (показано только первые 50 активных исполнителей)")
                            break
            except:
                continue
        if len(exec_lines) > 3:
            lines.extend(exec_lines)
            lines.append("")
        lines.append("=== Сводка Производительности ===")
        try:
            db_path = self.config.database_path
            if not os.path.exists(db_path):
                lines.append(f"База данных не найдена: {db_path}")
                lines.append("")
                return lines

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Определяем активы из пары
            if '-' in self.config.trading_pair:
                base_asset, quote_asset = self.config.trading_pair.split('-')
            elif '/' in self.config.trading_pair:
                base_asset, quote_asset = self.config.trading_pair.split('/')
            else:
                base_asset = self.config.trading_pair[:-4]
                quote_asset = self.config.trading_pair[-4:]

            # Извлекаем сделки из базы
            query = """
                SELECT trade_type, price, amount, trade_fee_in_quote
                FROM TradeFill
                WHERE market = ? AND base_asset = ? AND quote_asset = ?
            """
            cursor.execute(query, (self.config.connector_name, base_asset, quote_asset))
            trades = cursor.fetchall()
            conn.close()

            if not trades:
                lines.append("Нет исполненных сделок для анализа.")
                lines.append("")
                return lines

            buy_volume_base = Decimal("0")
            buy_volume_quote = Decimal("0")
            sell_volume_base = Decimal("0")
            sell_volume_quote = Decimal("0")
            total_fees = Decimal("0")

            for trade_type, price_raw, amount_raw, fee_raw in trades:
                price = Decimal(str(price_raw)) / Decimal("1000000")
                amount = Decimal(str(amount_raw)) / Decimal("1000000")
                fee = Decimal(str(fee_raw or 0)) / Decimal("1000000")
                quote_value = price * amount

                if trade_type == "BUY":
                    buy_volume_base += amount
                    buy_volume_quote += quote_value
                elif trade_type == "SELL":
                    sell_volume_base += amount
                    sell_volume_quote += quote_value

                total_fees += fee

            avg_buy_price = (buy_volume_quote / buy_volume_base) if buy_volume_base > 0 else Decimal("0")
            avg_sell_price = (sell_volume_quote / sell_volume_base) if sell_volume_base > 0 else Decimal("0")

            net_position = buy_volume_base - sell_volume_base

            # Реализованный PnL
            realized_pnl = sell_volume_quote - (sell_volume_base * avg_buy_price if buy_volume_base > 0 else Decimal("0"))

            # Текущая цена (для нереализованной прибыли)
            current_price = self.processed_data.get("reference_price", Decimal("0"))
            unrealized_pnl = Decimal("0")
            if net_position != 0 and current_price > 0:
                if net_position > 0:
                    # Лонг позиция
                    unrealized_pnl = net_position * (current_price - avg_buy_price)
                else:
                    # Шорт позиция
                    unrealized_pnl = abs(net_position) * (avg_sell_price - current_price)

            # --- Конвертация всех расчетов в USDT ---
            quote_label = "USDT"
            conversion_rate = Decimal("1")

            # если котировка не USDT, пробуем получить курс quote → USDT
            if "USDT" not in quote_asset.upper():
                try:
                    conversion_rate_raw = self.market_data_provider.get_price_by_type(
                        self.config.connector_name,
                        f"{quote_asset}-USDT",
                        PriceType.MidPrice
                    )
                    if conversion_rate_raw and conversion_rate_raw > 0:
                        conversion_rate = Decimal(str(conversion_rate_raw))
                        self.log.info(f"Конвертация {quote_asset}->USDT по курсу {conversion_rate}")
                except Exception as e:
                    self.log.warning(f"Не удалось получить курс {quote_asset}->USDT: {e}")

            # все суммы пересчитываем в USDT
            realized_pnl_usdt = realized_pnl * conversion_rate
            unrealized_pnl_usdt = unrealized_pnl * conversion_rate
            total_fees_usdt = total_fees * conversion_rate
            current_portfolio_value = (current_price * net_position) * conversion_rate
            hold_portfolio_value = (current_portfolio_value + realized_pnl_usdt)
            total_pnl = realized_pnl_usdt - total_fees_usdt
            trade_pnl = realized_pnl_usdt
            return_pct = (total_pnl / hold_portfolio_value * Decimal("100")) if hold_portfolio_value != 0 else Decimal("0")

            # --- Форматированный вывод ---
            lines.append(f"Пара: {self.config.trading_pair}")
            lines.append(f"Объем BUY:  {buy_volume_base:.6f} @ {avg_buy_price:.6f}")
            lines.append(f"Объем SELL: {sell_volume_base:.6f} @ {avg_sell_price:.6f}")
            lines.append(f"Чистая позиция (net): {net_position:.6f} {base_asset}")
            lines.append(f"Текущая цена: {current_price:.6f} {quote_asset}")
            lines.append(f"Hold portfolio value: {hold_portfolio_value:.2f} USDT")
            lines.append(f"Current portfolio value: {current_portfolio_value:.2f} USDT")
            lines.append(f"Trade P&L: {trade_pnl:.2f} USDT")
            lines.append(f"Fees paid: {total_fees_usdt:.2f} USDT")
            lines.append(f"Total P&L: {total_pnl:.2f} USDT")
            lines.append(f"Return %: {return_pct:.2f}%")
            lines.append("")

        except Exception as e:
            lines.append(f"Ошибка при анализе производительности: {str(e)}")
            lines.append("")

        except Exception as e:
            import traceback
            lines.append(f"Ошибка при расчёте производительности: {e}")
            lines.append(traceback.format_exc())
            lines.append("")

        return lines

    def _extract_filled_from_executor(self, executor):
        """Вспомогательный метод для извлечения информации о заполнении из executor"""
        try:
            filled_amount = getattr(executor, "filled_amount", None)
            if filled_amount is not None:
                return {"filled": filled_amount}
            
            config = getattr(executor, "config", None)
            if config:
                amount = getattr(config, "amount", None)
                if amount is not None:
                    return {"filled": Decimal("0"), "total": amount}
            
            return None
        except:
            return None

