from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
import httpx
from pydantic import BaseModel
import uuid
import json
from typing import Any, Optional
from pathlib import Path
import hashlib
from datetime import datetime
import time
import asyncio


class Table(BaseModel):
    name: str
    primary_key: str = "id"
    sort_key: str = "timestamp"

class ShardRegistration(BaseModel):
    # shard_id: str
    url: str

# Формат: {"shard1": "http://shard1:8000", "shard2": "http://shard2:8000"}
registered_shards: dict[str, str] = {}
tables_meta: dict[str, dict[str, str]] = {} 
hash_ring:list[tuple[int, str]] = []  # Список кортежів (hash, shard_url)   

def get_hash(key: str) -> int:
    """Повертає хеш-значення для ключа"""
    rec = hashlib.md5(key.encode('utf-8'))
    hex_rec = rec.hexdigest()   
    return int(hex_rec,16)

def get_shard_url(record_id: str) -> str:
    if not registered_shards:
        raise HTTPException(status_code=404, detail=f"Shards not found")
    
    hash_ring.sort(key=lambda x: x[0])
    hashed_key = get_hash(record_id)
    for vnode_hash,shard_url in hash_ring:
        if vnode_hash >= hashed_key:
            return shard_url

    return hash_ring[0][1]  # Повертаємо URL обраного шарду

app = FastAPI()

token_capacity = 4000.0
tokens = 40000.0
last_refill = time.time()
refill_rate = token_capacity/60
bucket_lock = asyncio.Lock()

async def token_bucket():
    global tokens, last_refill
    while True:
        async with bucket_lock:
            now = time.time()
            elapsed = now - last_refill
            tokens = min(token_capacity, tokens + elapsed* refill_rate)
            last_refill = now

            if tokens >= 1.0:
                tokens -= 1.0
                return 
           
            needed = 1.0 - tokens
            wait_time = needed/refill_rate

            await asyncio.sleep(wait_time)
            
@app.middleware("http")
async def rate_limit_middleware(requests:Request,call_next):
    # Пропускаємо службові запити без лімітів!
    if requests.url.path in ["/register","/metadata","/docs","/shards", "/test/overload"]:
        return await call_next(requests)

    await token_bucket()

    response = await call_next(requests)
    return response

@app.post("/register")
def register_shard(shard: ShardRegistration):
    assigned_id = f"shard_{len(registered_shards) + 1}"
    for i in range(0,50):
        key = f"{assigned_id}_vnode{i}"
        hashed_key = get_hash(key)
        hash_ring.append((hashed_key, shard.url))

    hash_ring.sort(key=lambda x: x[0])  # Сортуємо хеш-кільце за хеш-значенням
    registered_shards[assigned_id] = shard.url

    print(f"[Coordinator] Зареєстровано шард: {assigned_id} за адресою {shard.url}")
    return {"status": "ok", "shard_id": assigned_id, "message": f"Shard {assigned_id} registered"}

@app.post("/metadata")
def register_table_metadata(table: Table):
    if table.name in tables_meta and table.primary_key != tables_meta[table.name]["primary_key"]:
        raise HTTPException(status_code=400, detail=f"Table '{table.name}' already exists with a different primary key")
    tables_meta[table.name] = {
        "primary_key": table.primary_key,
        "sort_key": table.sort_key
    }
   
    print(f"[Coordinator] Зареєстровано метадані таблиці: {table.name} з первинним ключем {table.primary_key} та ключем сортування: {table.sort_key}")
    return {"status": "ok", "message": f"Metadata for table {table.name} registered"}

@app.get("/shards")
def get_shards():
    """Допоміжний ендпоінт, щоб подивитися, хто зареєстрований"""
    return registered_shards

@app.get("/")
def read_root():
    return {"message": "Coordinator is running. Shards registered: " + str(len(registered_shards))}


@app.post("/test/seed/{table_name}")
async def seed_table(table_name:str):
    if table_name not in tables_meta:
        await create_table(Table(name=table_name, primary_key="id"))
    shard1_num = 0
    shard2_num = 0
    for i in range(100):
        record = {"id": str(uuid.uuid4()), "name": f"Record {i}"}
        record_shard_url = get_shard_url(record["id"])
        if record_shard_url == registered_shards["shard_1"]:
            shard1_num += 1
        elif record_shard_url == registered_shards["shard_2"]:
            shard2_num += 1
        await create_record(table_name, record)
    
    return {"shard1_records": shard1_num, "shard2_records": shard2_num}

@app.post("/test/rebalance")
async def rebalance_data():
    async with httpx.AsyncClient() as client:
        for shard_id, shard_url in registered_shards.items():
            response = await client.get(f"{shard_url}/tables/")
            if response.status_code == 200:
                tables = response.json()

                shard_table_names = {table["name"] for table in tables}
                expected_table_names = set(tables_meta.keys())
                missing_tables = expected_table_names - shard_table_names

                for missing_table in missing_tables:
                    table = Table(name=missing_table, primary_key=tables_meta[missing_table]["primary_key"], sort_key= tables_meta[missing_table]["sort_key"])
                    response = await client.post(f"{shard_url}/tables/", json=table.model_dump())
                    if response.status_code != 200:
                        raise HTTPException(status_code=400, detail=f"Failed to create table '{missing_table}' on shard '{shard_id}': {response.text}")
            else:
                raise HTTPException(status_code=400, detail=f"Failed to read records from shard '{shard_id}': {response.text}")
    
        total_records = 0 
        moved_records = 0
        seen_records = set()
        for table_name, meta in tables_meta.items():
            pk = meta["primary_key"]
            sk = meta["sort_key"]
            for shard_id, shard_url in registered_shards.items():
                current_data = {}
                current_data[shard_url] = (await client.get(f"{shard_url}/tables/{table_name}/")).json()
                
                for shard_url,records in current_data.items():
                    for record in records:
                        if (record[pk], record[sk]) in seen_records:
                            continue
                        seen_records.add((record[pk], record[sk]))

                        total_records += 1
                        record_id = str(record[pk])
                        correct_shard_url = get_shard_url(record_id)

                        if correct_shard_url != shard_url:
                            moved_records += 1
                            # total_records -= 1 
                            response = await client.post(f"{correct_shard_url}/tables/{table_name}/", json=record)
                            if response.status_code != 200:
                                raise HTTPException(status_code=400, detail=f"Failed to move record '{record[pk]}' to shard '{correct_shard_url}': {response.text}")
                            response = await client.delete(f"{shard_url}/tables/{table_name}/{record[pk]}/{record[sk]}")
                            if response.status_code != 200:
                                raise HTTPException(status_code=400, detail=f"Failed to delete record '{record[pk]}' from shard '{shard_url}': {response.text}")
    
    return {
    "total_records": total_records,
    "moved_records": moved_records,
    "stayed_records": total_records - moved_records
    }


@app.post("/test/overload")
async def overload():
    shard_url = list(registered_shards.values())[0]
    async with httpx.AsyncClient() as client:
        for i in range(10):
            response = await client.get(f"{shard_url}/tables/")
            if response.status_code != 200:
                return JSONResponse(status_code = response.status_code, content=response.json())
    return JSONResponse(status_code=response.status_code, content=response.json())

@app.get("/tables/")
def get_tables():
    return [
        Table(name = name, primary_key=meta["primary_key"],sort_key= meta["sort_key"]) 
        for name, meta in tables_meta.items()]

@app.get("/tables/{table_name}/exists/")
def table_exists(table_name: str):
    exists = table_name in tables_meta
    return {"exists": exists}

@app.post("/tables/")
async def create_table(table: Table):
    try:
        if table.name in tables_meta:
            raise HTTPException(status_code=400, detail=f"Table '{table.name}' already exists")
        if table.primary_key is None:
            table.primary_key = "id"
        if table.sort_key is None:
            table.sort_key = "timestamp"
        async with httpx.AsyncClient() as client:
            for shard_id, shard_url in registered_shards.items():
                response = await client.post(f"{shard_url}/tables/", json=table.model_dump())
                if response.status_code != 200:
                    raise HTTPException(status_code=400, detail=f"Failed to create table '{table.name}' on shard '{shard_id}': {response.text}")
        tables_meta[table.name] = {
            "primary_key": table.primary_key,
            "sort_key": table.sort_key
        }
        return table
    except Exception as e:
       raise HTTPException(status_code=400, detail=str(e))

@app.delete("/tables/{table_name}")
async def delete_table(table_name: str):
    if table_name not in tables_meta:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    try:
        async with httpx.AsyncClient() as client:
            for shard_id, shard_url in registered_shards.items():
                responce = await client.delete(f"{shard_url}/tables/{table_name}")
                if responce.status_code == 200:
                    success = True
                else:
                    raise HTTPException(status_code=400, detail=f"Failed to delete table '{table_name}' on shard '{shard_id}': {responce.text}")
        tables_meta.pop(table_name, None)
        return {f"Table '{table_name}' deleted": success}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/tables/{table_name}/")   
async def create_record(table_name: str, record:dict):
    table = tables_meta.get(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    try:
        async with httpx.AsyncClient() as client:
            primary_key = tables_meta[table_name]["primary_key"]
            sort_key = tables_meta[table_name]["sort_key"]
            if primary_key not in record:
                record[primary_key] = str(uuid.uuid4())
            if sort_key not in record:
                record[sort_key] = str(datetime.now().isoformat())
            shard_url = get_shard_url(str(record[primary_key]))
            responce = await client.post(f"{shard_url}/tables/{table_name}/", json=record)
            if responce.status_code != 200:
                raise HTTPException(status_code=400, detail=f"Failed to create record in table '{table_name}' on shard '{shard_url}': {responce.text}")
        return record
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/tables/{table_name}/exists/{record_id}/{sort_val}")
async def record_exists(table_name: str, record_id: str,sort_val:str):
    table = tables_meta.get(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    async with httpx.AsyncClient() as client:
        shard_url = get_shard_url(record_id)
        response = await client.get(f"{shard_url}/tables/{table_name}/exists/{record_id}/{sort_val}")
        if response.status_code != 200:
            raise HTTPException(status_code=400, detail=f"Failed to check record existence in table '{table_name}' on shard '{shard_url}': {response.text}")
    exists = response.json().get("exists", False)
    return {"exists": exists}

@app.get("/tables/{table_name}/")
async def read_records(table_name: str):
    table = tables_meta.get(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    async with httpx.AsyncClient() as client:
        records = []
        for shard_id, shard_url in registered_shards.items():
            response = await client.get(f"{shard_url}/tables/{table_name}/")
            if response.status_code == 200:
                records.extend(response.json())
            else:
                raise HTTPException(status_code=400, detail=f"Failed to read records from table '{table_name}' on shard '{shard_id}': {response.text}")
    return records

@app.get("/tables/{table_name}/{record_id}")
async def read_records_by_id(table_name: str, record_id: str):
    table = tables_meta.get(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    async with httpx.AsyncClient() as client:
        shard_url = get_shard_url(record_id)
        response = await client.get(f"{shard_url}/tables/{table_name}/{record_id}")
        if response.status_code != 200:
            raise HTTPException(status_code=404, detail=f"Record '{record_id}' not found in table '{table_name}'")
    return response.json()

@app.get("/tables/{table_name}/{record_id}/{sort_val}")
async def read_record(table_name:str, record_id:str, sort_val:str):
    table = tables_meta.get(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    async with httpx.AsyncClient() as client:
        shard_url = get_shard_url(record_id)
        response = await client.get(f"{shard_url}/tables/{table_name}/{record_id}/{sort_val}")
        if response.status_code != 200:
            raise HTTPException(status_code=404, detail=f"Record '{record_id}' not found in table '{table_name}'")
    return response.json()    

@app.delete("/tables/{table_name}/{record_id}")
async def delete_record_by_id(table_name: str, record_id: str):
    table = tables_meta.get(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    async with httpx.AsyncClient() as client:
        shard_url = get_shard_url(record_id)
        response = await client.delete(f"{shard_url}/tables/{table_name}/{record_id}")
        if response.status_code != 200:
            raise HTTPException(status_code=404, detail=f"Record '{record_id}' not found in table '{table_name}'")
    return response.json()

@app.delete("/tables/{table_name}/{record_id}/{sort_val}")
async def delete_record(table_name: str, record_id: str,sort_val:str):
    table = tables_meta.get(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    async with httpx.AsyncClient() as client:
        shard_url = get_shard_url(record_id)
        response = await client.delete(f"{shard_url}/tables/{table_name}/{record_id}/{sort_val}")
        if response.status_code != 200:
            raise HTTPException(status_code=404, detail=f"Record '{record_id}' not found in table '{table_name}'")
    return response.json()

@app.put("/tables/{table_name}/{record_id}/{sort_val}")
async def update_record(table_name: str, record_id: str,sort_val:str, updated_record: dict):
    pk = tables_meta[table_name]["primary_key"]
    if not pk:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    if pk in updated_record and updated_record[pk] != record_id:
        raise HTTPException(status_code=400, detail=f"Cannot change the primary key '{pk}' of the record")
    async with httpx.AsyncClient() as client:
        shard_url = get_shard_url(record_id)
        response = await client.put(f"{shard_url}/tables/{table_name}/{record_id}/{sort_val}", json=updated_record)
        if response.status_code != 200:
            raise HTTPException(status_code=404, detail=f"Record '{record_id}' not found in table '{table_name}'")
    return response.json()


