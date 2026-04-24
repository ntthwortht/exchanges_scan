import asyncio
import random
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from functools import wraps
from typing import Any

import aiohttp
from loguru import logger

# HTTP-коды, при которых имеет смысл повторить запрос (остальные 4xx/5xx — сразу наверх)
_RETRIABLE_STATUS = frozenset({429, 502, 503, 504})


@dataclass(frozen=True, slots=True)
class ExchangeConfig:
    """Статичные параметры запроса к публичному API."""

    name: str
    url: str
    params: dict[str, str | int | float]


def retry_fetch(
    max_attempts: int = 3, backoff: float = 0.5
) -> Callable[
    [Callable[..., Coroutine[Any, Any, Any]]], Callable[..., Coroutine[Any, Any, Any]]
]:
    """Повтор при исключениях. Использование: @retry_fetch() над async def fetch."""

    def decorator(
        func: Callable[..., Coroutine[Any, Any, Any]],
    ) -> Callable[..., Coroutine[Any, Any, Any]]:
        @wraps(func)
        async def wrapper(
            self: Any,
            session: aiohttp.ClientSession,
            *args: Any,
            **kwargs: Any,
        ) -> Any:
            for attempt in range(max_attempts):
                try:
                    return await func(self, session, *args, **kwargs)
                except (TimeoutError, aiohttp.ClientConnectionError) as e:
                    err = e
                except aiohttp.ClientResponseError as e:
                    if e.status not in _RETRIABLE_STATUS:
                        raise
                    err = e
                if attempt == max_attempts - 1:
                    raise err
                logger.error(
                    "exchange: {}, retry_fetch: {} (attempt {}/{})",
                    self.name,
                    err,
                    attempt + 1,
                    max_attempts,
                )
                await asyncio.sleep(backoff * (2**attempt))
            # при max_attempts == 0 сюда попадаем; иначе выход — return или raise выше
            raise RuntimeError("retry_fetch: max_attempts must be >= 1")

        return wrapper

    return decorator


class Exchange(ABC):
    """Биржа: знает URL/params и умеет вытащить цену из JSON."""

    def __init__(self, config: ExchangeConfig) -> None:
        self._config = config

    @property
    def name(self) -> str:
        return self._config.name

    @abstractmethod
    def parse(self, data: dict[str, Any]) -> int | float | None:
        """Разбор тела ответа при HTTP 200."""

    @retry_fetch()
    async def fetch(
        self,
        session: aiohttp.ClientSession,
        *,
        run_index: int = 0,
        semaphore: asyncio.Semaphore,
    ) -> tuple[str, int | float | None] | None:
        """
        Возвращает:
        - (name, price) при успешном разборе;
        - (name, None) при HTTP 200, но цену не вытащили;
        - None при ошибке транспорта / статуса != 200 / не-JSON.
        """

        async with semaphore:
            async with asyncio.timeout(5):
                try:
                    logger.info(f"{self.name} fetching price with id {run_index}")
                    async with session.get(
                        self._config.url,
                        params=self._config.params,
                    ) as response:
                        if response.status != 200:
                            logger.error(f"{self.name} status {response.status}")
                            if response.status in _RETRIABLE_STATUS:
                                response.raise_for_status()

                            return self.name, None

                        data = await response.json()
                        price = self.parse(data)
                        if price is not None:
                            return self.name, price
                        return self.name, None
                except TimeoutError:
                    logger.error("Timeout error, retrying...")
                    raise
                except aiohttp.ClientResponseError:
                    logger.error("Client error, retrying...")
                    raise
                except Exception as e:
                    logger.error(
                        "Error fetching price, exchange: {}, error: {}",
                        self.name,
                        e,
                    )
                    return None


class RateLimiter:
    def __init__(self, rate: int = 0, capacity: int = 0) -> None:
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.refill_time = time.monotonic()

    async def acquire(self) -> None:
        """Ждать в цикле, накопить токен и списать (один acquire = одно разрешение)."""
        if self.rate <= 0:  # Если rate = 0, то не нужно ждать
            return
        while (
            True
        ):  # Ждать в цикле, накопить токен и списать (один acquire = одно разрешение).
            now = time.monotonic()
            elapsed = now - self.refill_time
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.refill_time = now
            if self.tokens >= 1:
                self.tokens -= 1
                return
            wait = (1 - self.tokens) / self.rate
            await asyncio.sleep(wait)


class BybitExchange(Exchange, RateLimiter):
    def __init__(
        self, config: ExchangeConfig, rate: int = 20, capacity: int = 40
    ) -> None:
        Exchange.__init__(self, config)
        RateLimiter.__init__(self, rate, capacity)

    async def fetch(
        self,
        session: aiohttp.ClientSession,
        *,
        run_index: int = 0,
        semaphore: asyncio.Semaphore,
    ) -> tuple[str, int | float | None] | None:
        await self.acquire()
        return await super().fetch(session, run_index=run_index, semaphore=semaphore)

    def parse(self, data: dict[str, Any]) -> int | float | None:

        try:
            rows = data["result"]["list"]
            if not rows:
                return None
            first_row = rows[0]
            return first_row[4]
        except (KeyError, IndexError) as e:
            logger.error("Error getting price, exchange: {}, {}", self.name, e)
            return None
        except Exception as e:
            logger.error("Error getting price, exchange: {}, {}", self.name, e)
            return None


class OkxExchange(Exchange, RateLimiter):
    def __init__(
        self, config: ExchangeConfig, rate: int = 5, capacity: int = 5
    ) -> None:
        Exchange.__init__(self, config)
        RateLimiter.__init__(self, rate, capacity)

    async def fetch(
        self,
        session: aiohttp.ClientSession,
        *,
        run_index: int = 0,
        semaphore: asyncio.Semaphore,
    ) -> tuple[str, int | float | None] | None:
        await self.acquire()
        return await super().fetch(session, run_index=run_index, semaphore=semaphore)

    def parse(self, data: dict[str, Any]) -> int | float | None:
        try:
            items = data["data"]
            if not items:
                return None
            return items[0]["last"]
        except (KeyError, IndexError) as e:
            logger.error("Error getting price, exchange: {}, {}", self.name, e)
            return None
        except Exception as e:
            logger.error("Error getting price, exchange: {}, {}", self.name, e)
            return None


class MexcExchange(Exchange, RateLimiter):
    def __init__(
        self, config: ExchangeConfig, rate: int = 20, capacity: int = 30
    ) -> None:
        Exchange.__init__(self, config)
        RateLimiter.__init__(self, rate, capacity)

    async def fetch(
        self,
        session: aiohttp.ClientSession,
        *,
        run_index: int = 0,
        semaphore: asyncio.Semaphore,
    ) -> tuple[str, int | float | None] | None:
        await self.acquire()
        return await super().fetch(session, run_index=run_index, semaphore=semaphore)

    def parse(self, data: dict[str, Any]) -> int | float | None:
        try:
            if "lastPrice" in data:
                return data["lastPrice"]
            logger.error("Error getting price, exchange: {}, no lastPrice", self.name)
            return None
        except (KeyError, IndexError) as e:
            logger.error("Error getting price, exchange: {}, {}", self.name, e)
            return None
        except Exception as e:
            logger.error("Error getting price, exchange: {}, {}", self.name, e)
            return None


def build_exchanges() -> list[Exchange]:
    return [
        BybitExchange(
            ExchangeConfig(
                name="bybit",
                url="https://api.bybit.com/v5/market/kline",
                params={
                    "category": "spot",
                    "symbol": "BTCUSDT",
                    "interval": "1",
                    "limit": 10,
                },
            )
        ),
        OkxExchange(
            ExchangeConfig(
                name="okx",
                url="https://www.okx.com/api/v5/market/ticker",
                params={"instId": "BTC-USDT-SWAP"},
            )
        ),
        MexcExchange(
            ExchangeConfig(
                name="mexc",
                url="https://api.mexc.com/api/v3/ticker/24hr",
                params={"symbol": "BTCUSDT"},
            )
        ),
    ]


async def main() -> None:
    semaphore = asyncio.Semaphore(20)
    exchanges = build_exchanges()
    connector = aiohttp.TCPConnector(limit=30, limit_per_host=10)
    price_list: list[tuple[str, float]] = []

    async with aiohttp.ClientSession(connector=connector) as session:
        work: list[tuple[Exchange, int]] = [
            (ex, run_index) for ex in exchanges for run_index in range(100)
        ]
        random.shuffle(work)
        tasks = [
            asyncio.create_task(
                ex.fetch(session, run_index=run_index, semaphore=semaphore)
            )
            for ex, run_index in work
        ]
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, tuple) and result is not None:
                    exchange, price = result
                    if price is not None:
                        price_list.append((exchange, float(price)))
                    else:
                        logger.error(f"🔴 No price found, exchange: {exchange}")

                elif isinstance(result, BaseException):
                    logger.error(f"🔴 Error: {result}")
                else:
                    logger.error(f"🔴 Unexpected result: {result}")

        except Exception as e:
            logger.error(f"Error: {e}")

        sorted_price_list = sorted(price_list, key=lambda x: x[1])
        for exchange, price in sorted_price_list[:3]:
            print(f"{exchange} - {price}")


if __name__ == "__main__":
    asyncio.run(main())
