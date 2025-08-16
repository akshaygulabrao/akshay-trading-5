import asyncio
import random
import time

_epoch = time.perf_counter()


def us() -> int:  # microseconds since epoch
    return int((time.perf_counter() - _epoch) * 1_000_000)


def fmt(t_us: int) -> str:  # HH:MM:SS.mmmmmm
    s, us_ = divmod(t_us, 1_000_000)
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}.{us_:06d}"


class Producer:
    def __init__(self, pid: int, total_items: int, queue: asyncio.Queue):
        self.pid = pid
        self.total_items = total_items
        self.queue = queue

        self.produce_time_total = 0  # µs
        self.produce_latencies = []  # µs per item

    async def run(self):
        for i in range(self.total_items):
            await asyncio.sleep(random.uniform(0.1, 0.3))  # simulate work

            item = f"item-{self.pid}-{i}"
            t0 = us()
            await self.queue.put(item)
            t1 = us()

            dt = t1 - t0
            self.produce_time_total += dt
            self.produce_latencies.append(dt)

        print(
            f"[{fmt(us())}] P{self.pid}: done producing " f"(total put() time {self.produce_time_total} µs)"
        )


async def consumer(queue: asyncio.Queue):
    while True:
        item = await queue.get()
        if item is None:  # shutdown sentinel
            print(f"[{fmt(us())}] C: received shutdown signal")
            queue.task_done()
            return

        t0 = us()
        print(f"[{fmt(t0)}] C: processing {item}")
        t1 = us()
        print(f"[{fmt(t1)}] C: finished {item} (took {t1 - t0} µs)")
        queue.task_done()


# ------------------------------------------------------------------
async def main():
    num_producers = 3
    items_per_producer = 10
    queue = asyncio.Queue(maxsize=10_000)

    producers = [Producer(pid, items_per_producer, queue) for pid in range(num_producers)]

    consumer_task = asyncio.create_task(consumer(queue))

    producer_tasks = [asyncio.create_task(p.run()) for p in producers]

    await asyncio.gather(*producer_tasks)

    await queue.put(None)

    await queue.join()

    await consumer_task

    for p in producers:
        print(f"\n[SUMMARY] P{p.pid}")
        print(f"  items produced      = {p.total_items}")
        print(f"  total put() time    = {p.produce_time_total} µs")
        if p.produce_latencies:
            print(f"  min put() latency   = {min(p.produce_latencies)} µs")
            print(f"  max put() latency   = {max(p.produce_latencies)} µs")
            avg = sum(p.produce_latencies) // len(p.produce_latencies)
            print(f"  avg put() latency   = {avg} µs")


if __name__ == "__main__":
    asyncio.run(main())
