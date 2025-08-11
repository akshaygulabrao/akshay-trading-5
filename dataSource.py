import asyncio
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Optional


class DataSource(ABC):
    def __init__(self, name: str, broadcast_queue: Optional[asyncio.Queue]):
        self.name = name
        self.broadcast_queue = broadcast_queue
        self.queue = asyncio.Queue(maxsize=10_000)
        self.db_pool = ThreadPoolExecutor(max_workers=4)

    @abstractmethod
    async def start(self) -> None:
        """Start collecting data from this source"""
        pass

    @abstractmethod
    async def process_queue(self) -> None:
        """Process items from this source's queue"""
        pass
