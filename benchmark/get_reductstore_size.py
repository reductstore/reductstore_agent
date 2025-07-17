import asyncio
from reduct import Client

async def get_total_data_size_kb():
    async with Client("http://localhost:8383") as client:
        buckets = await client.list()
        total_size = sum(bucket.size for bucket in buckets)
        return total_size / 1024  # Kb

if __name__ == "__main__":
    size = asyncio.run(get_total_data_size_kb())
    print(f"{size:.2f}")