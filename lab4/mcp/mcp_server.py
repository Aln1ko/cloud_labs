from mcp.server.mcpserver import MCPServer
import httpx

mcp = MCPServer("Dynamo-DB-MCP")
COORDINATOR_URL = "http://localhost:8000"


@mcp.tool()
async def get_tables():
    """
    Повертає масив таблиці, які наявні в базі даних,
    а точніше ім'я таблиці, ії primary_key та sort_key
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{COORDINATOR_URL}/tables/")
        return response.json()

@mcp.tool()
async def table_exists(table_name: str):
    """
    Повертає bool значення чи існує в базі даних таблиця з ім'ям table_name
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{COORDINATOR_URL}/tables/{table_name}/exists/")
        return response.json()

@mcp.tool()
async def create_table(table_name: str, primary_key: str = "id", sort_key: str = "timestamp"):
    """
    Створює таблицю з ім'ям table_name, primary_key та sort_key.
    primary_key та sort_key передавати не обов'яково, вони автоматично створяться на сервері
    primary_key = "id" та sort_key = "timestamp"
    """
    table = {"name": table_name, "primary_key":primary_key, "sort_key":sort_key } 
    async with httpx.AsyncClient() as client:
        response = await client.post(f"{COORDINATOR_URL}/tables/", json=table)
        return response.json()
    
@mcp.tool()
async def delete_table(table_name: str):
    """
    Видаляє таблицю з бази даних по ім'ю table_name
    """
    async with httpx.AsyncClient() as client:
        response = await client.delete(f"{COORDINATOR_URL}/tables/{table_name}")
        return response.json()

@mcp.tool()
async def create_record(table_name: str, record:dict):
    """
    Створює запис record у таблиці table_name. 
    record не забов'язан містити primary_key та sort_key,
    якщо їх немає їх створить сервер
    """
    async with httpx.AsyncClient() as client:
        response = await client.post(f"{COORDINATOR_URL}/tables/{table_name}/", json = record)
        return response.json()

@mcp.tool()
async def record_exists(table_name: str, record_id: str,sort_val:str):
    """
    Повертає bool значення чи існує в базі даних у таблиці table_name запис з 
    primary_key = record_id і sort_key = sort_val
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{COORDINATOR_URL}/tables/{table_name}/exists/{record_id}/{sort_val}")
        return response.json()

@mcp.tool()
async def read_records(table_name: str):
    """
    Повертає всі записи в таблиці table_name
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{COORDINATOR_URL}/tables/{table_name}/")
        return response.json()

@mcp.tool()
async def read_records_by_id(table_name: str, record_id: str):
    """
    Повертає всі записи в таблиці table_name з primary_key = record_id
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{COORDINATOR_URL}/tables/{table_name}/{record_id}")
        return response.json()

@mcp.tool()
async def read_record(table_name:str, record_id:str, sort_val:str):
    """
    Повертає запис в таблиці table_name з primary_key = record_id та sort_key = sort_val
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{COORDINATOR_URL}/tables/{table_name}/{record_id}/{sort_val}")
        return response.json()

@mcp.tool()
async def delete_record_by_id(table_name: str, record_id: str):
    """
    Видаляє всі записи в базі даних в таблиці з ім'ям table_name та primary_key = record_id
    """
    async with httpx.AsyncClient() as client:
        response = await client.delete(f"{COORDINATOR_URL}/tables/{table_name}/{record_id}")
        return response.json()

@mcp.tool()
async def delete_record(table_name: str, record_id: str,sort_val:str):
    """
    Видаляє всі записи в базі даних в таблиці з ім'ям table_name та primary_key = record_id та sort_key = sort_val
    """
    async with httpx.AsyncClient() as client:
        response = await client.delete(f"{COORDINATOR_URL}/tables/{table_name}/{record_id}/{sort_val}")
        return response.json()

@mcp.tool()
async def update_record(table_name: str, record_id: str,sort_val:str, updated_record: dict):
    """
    Оновлює запис в таблиці table_name, де запис primary_key = record_id та sort_key = sort_val на updated_record
    """
    async with httpx.AsyncClient() as client:
        response = await client.put(f"{COORDINATOR_URL}/tables/{table_name}/{record_id}/{sort_val}", json=updated_record)
        return response.json()

    
if __name__ == "__main__":
    mcp.run()