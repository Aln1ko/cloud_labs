import asyncio
import time
import httpx

async def post_test(table: str, primary_key: str):
    post_array = []
    i = 1
    async with httpx.AsyncClient(timeout=10.0) as client:
        start_test = time.time()
        while time.time() - start_test < 10.0:
            start = time.time()
            response = await client.post(
                f"http://127.0.0.1:8000/tables/{table}/",
                json={primary_key: f"user_{i}"}
            )
            finish = time.time()
            i += 1
            latency = (finish - start) * 1000
            post_array.append(latency)

    avg_latency = sum(post_array) / len(post_array)
    write_throughput = len(post_array) / 10.0
    print(f"WRITE AVG LATENCY (ms): {avg_latency:.2f}")
    print(f"WRITE Throughput (ops/sec): {write_throughput:.2f}")

async def get_test(table: str, primary_key: str):
    get_array = []
    async with httpx.AsyncClient(timeout=10.0) as client:
        start_test = time.time()
        while time.time() - start_test < 10.0:
            start = time.time()
            response = await client.get(f"http://127.0.0.1:8000/tables/{table}/user_1")
            finish = time.time()
            latency = (finish - start) * 1000
            get_array.append(latency)

    avg_latency = sum(get_array) / len(get_array)
    read_throughput = len(get_array) / 10.0
    print(f"READ AVG LATENCY (ms): {avg_latency:.2f}")
    print(f"READ Throughput (ops/sec): {read_throughput:.2f}")

async def main_test():
    table = "orders"
    primary_key = "email"
    async with httpx.AsyncClient(timeout=10.0) as client:
        data = {"name": table, "primary_key": primary_key}
        await client.post("http://127.0.0.1:8000/tables/", json=data)

    await post_test(table, primary_key)
    await get_test(table, primary_key)

if __name__ == "__main__":
    asyncio.run(main_test())