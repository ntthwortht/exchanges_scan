import asyncio
import contextlib
import signal
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import aiohttp
from loguru import logger


class Aggregator:
    def __init__(self, price_queue: asyncio.Queue, stop_event: asyncio.Event) -> None:
        self._price_queue = price_queue
        self._stop_event = stop_event
        self._prices = {}

    async def run(self) -> None:
        if self._stop_event.is_set():
            return

        while not self._stop_event.is_set():
            try:
                price = await asyncio.wait_for(self._price_queue.get(), timeout=1.5)
                if price is None:
                    continue
                self._prices[price[0]] = price[1]
                self._price_queue.task_done()
                if len(self._prices) >= 1:
                    name, p = min(self._prices.items(), key=lambda x: x[1])
                    logger.info(f"Best price: {name} {p}")
            except TimeoutError:
                if self._stop_event.is_set():
                    return
                logger.debug("Aggregator: no quote within timeout, waiting…")
            except asyncio.CancelledError:
                logger.info("Aggregator task cancelled (shutdown)")
                raise
            except Exception as e:
                logger.error(f"Aggregator error: {e}")
                continue


@dataclass(frozen=True, slots=True)
class ExchangeConfig:
    """Статичные параметры запроса к публичному API."""

    name: str
    url: str
    params: dict[str, Any]


class Exchange(ABC):
    """Биржа: знает URL/params и умеет вытащить цену из JSON."""

    def __init__(self, config: ExchangeConfig) -> None:
        self._config = config

    @property
    def name(self) -> str:
        return self._config.name

    @abstractmethod
    async def parse_websocket(
        self,
        session: aiohttp.ClientSession,
        stop_event: asyncio.Event,
        price_queue: asyncio.Queue,
    ) -> int | float | None:
        """Разбор тела ответа при HTTP 200."""


class BybitExchange(Exchange):
    async def parse_websocket(
        self,
        session: aiohttp.ClientSession,
        stop_event: asyncio.Event,
        price_queue: asyncio.Queue,
    ) -> int | float | None:
        async with session.ws_connect(self._config.url) as ws:
            logger.info("Trading bybit websocket")
            sub = self._config.params
            await ws.send_json(sub)
            message = await ws.receive()
            if message.type == aiohttp.WSMsgType.TEXT:
                data = message.json()
                if data.get("success", None):
                    logger.success(f"Bybit websocket subscription message: {data}")
                else:
                    logger.error(f"Bybit websocket subscription message: {data}")

            async for message in ws:
                try:
                    if stop_event.is_set():
                        break
                    if message.type == aiohttp.WSMsgType.TEXT:
                        data = message.json()
                        body = data.get("data") or {}
                        price = float(body.get("lastPrice", "0"))
                        await asyncio.wait_for(
                            price_queue.put((self.name, price)), timeout=1
                        )
                except TimeoutError:
                    logger.error("Bybit websocket queue is full")
                    return None
                if message.type == aiohttp.WSMsgType.CLOSED:
                    logger.warning("Bybit websocket closed")
                    return None
                if message.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"Bybit websocket error: {message.data}")
                    return None


class OkxExchange(Exchange):
    async def parse_websocket(
        self,
        session: aiohttp.ClientSession,
        stop_event: asyncio.Event,
        price_queue: asyncio.Queue,
    ) -> int | float | None:
        async with session.ws_connect(self._config.url) as ws:
            logger.info("Trading okx websocket")
            sub = self._config.params
            await ws.send_json(sub)
            message = await ws.receive()
            if message.type == aiohttp.WSMsgType.TEXT:
                data = message.json()
                if data.get("event", None) == "subscribe":
                    logger.success(f"Okx websocket subscription message: {data}")
                else:
                    logger.error(f"Okx websocket subscription message: {data}")

            async for message in ws:
                try:
                    if stop_event.is_set():
                        break
                    if message.type == aiohttp.WSMsgType.TEXT:
                        data = message.json()
                        body = data.get("data") or {}
                        price = float(body[0].get("idxPx", "0"))
                        await asyncio.wait_for(
                            price_queue.put((self.name, price)), timeout=1
                        )
                except TimeoutError:
                    logger.error("Okx websocket queue is full")
                    return None
                if message.type == aiohttp.WSMsgType.CLOSED:
                    logger.warning("Okx websocket closed")
                    return None
                if message.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"Okx websocket error: {message.data}")
                    return None


# class MexcExchange(Exchange):
#     async def parse_websocket(
#         self, session: aiohttp.ClientSession
#     ) -> int | float | None:
#         async with session.ws_connect(self._config.url) as ws:
#             sub = self._config.params
#             await ws.send_json(sub)


def build_exchanges_request() -> list[Exchange]:
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
        # MexcExchange(
        #     ExchangeConfig(
        #         name="mexc",
        #         url="https://api.mexc.com/api/v3/ticker/24hr",
        #         params={"symbol": "BTCUSDT"},
        #     )
        # ),
    ]


def build_exchanges_websocket() -> list[Exchange]:
    return [
        BybitExchange(
            ExchangeConfig(
                name="bybit",
                url="wss://stream.bybit.com/v5/public/spot",
                params={
                    "op": "subscribe",
                    "args": [
                        "tickers.BTCUSDT",
                    ],
                },
            )
        ),
        OkxExchange(
            ExchangeConfig(
                name="okx",
                url="wss://wspap.okx.com:8443/ws/v5/public",
                params={
                    "op": "subscribe",
                    "args": [
                        {
                            "channel": "index-tickers",
                            "instId": "BTC-USDT",
                        }
                    ],
                },
            )
        ),
        # MexcExchange(
        #     ExchangeConfig(
        #         name="mexc",
        #         url="wss://wbs-api.mexc.com/ws",
        #         params={
        #             "method": "SUBSCRIPTION",
        #             "params": [
        #                 "spot@public.tickers.v3.api@BTCUSDT",
        #             ],
        #         },
        #     )
        # ),
    ]


async def main() -> None:
    stop_event = asyncio.Event()
    price_queue = asyncio.Queue(maxsize=100)

    def signal_handler():
        logger.info("Signal received, stopping...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, signal_handler)
    loop.add_signal_handler(signal.SIGTERM, signal_handler)

    async with aiohttp.ClientSession() as session:
        exchanges = build_exchanges_websocket()
        agg_task = asyncio.create_task(Aggregator(price_queue, stop_event).run())
        await asyncio.gather(
            *[
                exchange.parse_websocket(session, stop_event, price_queue)
                for exchange in exchanges
            ]
        )
        stop_event.set()
        agg_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await agg_task


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Программа остановлена пользователем]")
