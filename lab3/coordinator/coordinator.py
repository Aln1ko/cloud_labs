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
import random
from contextlib import asynccontextmanager

class Table(BaseModel):
    name: str
    primary_key: str = "id"
    sort_key: str = "timestamp"

class ShardRegistration(BaseModel):
    # shard_id: str
    url: str

# Формат: {"shard1": {
# "leader" : "http://shard1:8000"
# replicas: [
# "http://shard1:8000",
# "http://shard2:8000"
# "http://shard3:8000"] 
# }, "shard2": "http://shard2:8000".......}
registered_shards: dict[dict[str, str]] = {}
dead_shards: dict[str, list[str]] = {} # shard_1 : []
url_to_replica_info: dict[str, tuple[str, str]] = {}

num_of_replica = 3

tables_meta: dict[str, dict[str, str]] = {} 
hash_ring:list[tuple[int, str]] = []  # Список кортежів (hash, shard_url)  
W = 2
R = 2
N = 3 

cache: dict[str,dict] = {}
TTL = 10.0

def get_hash(key: str) -> int:
    """Повертає хеш-значення для ключа"""
    rec = hashlib.md5(key.encode('utf-8'))
    hex_rec = rec.hexdigest()   
    return int(hex_rec,16)

def get_shard_id(record_id: str) -> str:
    if not registered_shards:
        raise HTTPException(status_code=404, detail=f"Shards not found")
    
    hash_ring.sort(key=lambda x: x[0])
    hashed_key = get_hash(record_id)
    for vnode_hash,shard_id in hash_ring:
        if vnode_hash >= hashed_key:
            return shard_id

    return hash_ring[0][1]  # Повертаємо id обраного шарду

def get_leader_url(record_id:str):
    global W
    id = get_shard_id(record_id)
    if len(registered_shards[id]["replicas"]) < W:
        raise HTTPException(status_code=503, detail=f"Write Quorum not met (alive < {W})!")
    return registered_shards[id]["leader"]

def get_read_url(record_id:str):
    id = get_shard_id(record_id)
    return get_read_url_for_shard_id(id)

def get_read_url_for_shard_id(shard_id:str):
    replicas = registered_shards[shard_id]["replicas"]
    idx = registered_shards[shard_id]["read_index"] % len(replicas)
    registered_shards[shard_id]["read_index"] = (idx + 1) % len(replicas)
    return replicas[idx]


async def monitor_shards():
    global registered_shards, dead_shards
    while True:
        await asyncio.sleep(5.0)
        async with httpx.AsyncClient() as client:
            for shard_id,shard_info in registered_shards.items():
                for replica in list(shard_info["replicas"]):
                    try:
                        responce = await client.get(f"{replica}/health", timeout = 1.0)
                    except httpx.RequestError:
                        shard_info["replicas"].remove(replica)
                        dead_shards[shard_id].append(replica)
                        print(f"Репліка {replica} не відповідає")

                        if replica == shard_info["leader"]:
                            if len(shard_info["replicas"]) > 0:
                                new_leader = shard_info["replicas"][0]
                                shard_info["leader"] = new_leader
                                await client.post(f"{new_leader}/role", json = {"role":"leader"})
                                print(f"Новим лідером шарда {shard_id} призначено {new_leader}")
            for dead_shard_id, dead_shards_urls in dead_shards.items():
                for dead_url in list(dead_shards_urls):
                    try:
                        responce = await client.get(f"{dead_url}/health", timeout = 1.0)
                        if responce.status_code == 200:
                            dead_shards[dead_shard_id].remove(dead_url)
                            registered_shards[dead_shard_id]["replicas"].append(dead_url)
                            print(f"Репліка {dead_url} вийшла на зв'язок")
                            await client.post(f"{dead_url}/role", json = {"role":"follower"})

                    except httpx.RequestError:
                        pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(monitor_shards())
    yield                   

app = FastAPI(lifespan=lifespan)

token_capacity = 54000.0
tokens = 54000.0
last_refill = time.time()
refill_rate = token_capacity/60
bucket_lock = asyncio.Lock()

async def wait_write_quorum(shard_id:str,table_name: str = None, record_id: str = None):
    global W,cache
    for k in list(cache.keys()):
        if record_id and table_name:
            # Видаляємо тільки кеш цього конкретного record_id І загальний список всієї таблиці
            if (k == f"{shard_id}:tables/{table_name}/" or 
                k.startswith(f"{shard_id}:tables/{table_name}/{record_id}") or 
                k.startswith(f"{shard_id}:tables/{table_name}/exists/{record_id}")):
                del cache[k]
        elif table_name:
            # Якщо видалили/створили саму таблицю — видаляємо тільки кеш цієї таблиці
            if k.startswith(f"{shard_id}:tables/{table_name}/"):
                del cache[k]
        else:
            # Запасний варіант на рівень шарда
            if k.startswith(f"{shard_id}:"):
                del cache[k]

    if len(registered_shards[shard_id]["replicas"]) >= W:
        async with httpx.AsyncClient() as client:
            replica_url = None
            while True:
                replica_url = get_read_url_for_shard_id(shard_id)
                leader_url = registered_shards[shard_id]["leader"]
                if replica_url != leader_url:
                    version = (await client.get(f"{leader_url}/offset")).json()
                    offset = (await client.get(f"{replica_url}/offset")).json()
                    if version > offset:
                         await asyncio.sleep(0.2)
                    else: return f"ok"
                else: 
                    continue        
    else:
        raise HTTPException(status_code=503, detail=f"Write Quorum not met (alive < {W})!")

async def read_quorum_url(shard_id:str,path:str):
    global R, TTL
    cache_key = f"{shard_id}:{path}"
    now = time.time()
    if cache_key in cache:
        if now - cache[cache_key]["timestamp"] < TTL:
            print(f"[Cache HIT] Відповідь для {cache_key} взято з кешу!")
            return cache[cache_key]["response"]
        else:
            del cache[cache_key]  # TTL минув, видаляємо прострочений запис
    
    if len(registered_shards[shard_id]["replicas"]) < R:
        raise HTTPException(status_code=503,detail=f"Write Quorum not met (alive < {R})!")
    responses = []
    for i in range(R):
        shard_url = get_read_url_for_shard_id(shard_id)
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{shard_url}/{path}")
            offset = (await client.get(f"{shard_url}/offset")).json()
            responses.append((offset,response))
    newest_response = max(responses, key = lambda x: x[0])[1]
    if newest_response.status_code == 200:
        cache[cache_key] = {"response": newest_response, "timestamp": time.time()}
    return newest_response

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
    global registered_shards,dead_shards, url_to_replica_info
    #  1. ЯКЩО ЦЯ РЕПЛІКА ВЖЕ КОЛИСЬ РЕЄСТРУВАЛАСЯ (вона перезапустилася):
    if shard.url in url_to_replica_info:
        s_id, replica_id = url_to_replica_info[shard.url]  # Дістаємо її рідні імена з пам'ятки!
        
        # Прибираємо зі списку мертвих (якщо вона там була)
        if shard.url in dead_shards[s_id]:
            dead_shards[s_id].remove(shard.url)
            
        # Якщо в шарді взагалі не залишилося живих реплік — робимо її лідером, інакше фолловером
        if len(registered_shards[s_id]["replicas"]) == 0:
            role = "leader"
            registered_shards[s_id]["leader"] = shard.url
        else:
            role = "follower"
            
        # Додаємо назад у список живих реплік
        if shard.url not in registered_shards[s_id]["replicas"]:
            registered_shards[s_id]["replicas"].append(shard.url)
            
        print(f"[Coordinator] Репліка {replica_id} ({role}) перезапустилася і повернулася в стрій!")
        return {
            "status": "ok",
            "shard_id": s_id,
            "replica_id": replica_id,
            "role": role,
            "message": f"Re-registered {replica_id} as {role}"
        }
    
    if len(registered_shards) == 0:
        assigned_id = f"shard_{len(registered_shards) + 1}"
    else:
        num_of_shard = len(registered_shards)
        if len(registered_shards[f"shard_{num_of_shard}"]["replicas"]) == num_of_replica:
            assigned_id = f"shard_{len(registered_shards) + 1}"
        else:
            assigned_id = f"shard_{len(registered_shards)}"

    if assigned_id not in registered_shards:
        registered_shards[assigned_id]  = {"leader": shard.url,"replicas": [shard.url], "read_index":0}
        dead_shards[assigned_id] = []
        for i in range(0,50):
            key = f"{assigned_id}_vnode{i}"
            hashed_key = get_hash(key)
            hash_ring.append((hashed_key, assigned_id))

        hash_ring.sort(key=lambda x: x[0])  # Сортуємо хеш-кільце за хеш-значенням
        replica_id = assigned_id + f"_replica_{len(registered_shards[assigned_id]['replicas'])}"
        role = "leader"
    else:
        registered_shards[assigned_id]["replicas"].append(shard.url)
        replica_id = assigned_id + f"_replica_{len(registered_shards[assigned_id]['replicas'])}"
        role = "follower"

    url_to_replica_info[shard.url] = (assigned_id, replica_id)
    print(f"[Coordinator] Зареєстровано шард: {replica_id} з ролю {role} за адресою {shard.url}")
    return {
    "status": "ok",
    "shard_id": assigned_id,      # наприклад, "shard_1"
    "replica_id": replica_id,     # наприклад, "shard_1_replica_1"
    "role": role,                 # "leader" або "follower"
    "message": f"Registered {replica_id} as {role}"
    }

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
        shard_id = get_shard_id(record["id"])
        if shard_id == "shard_1":
            shard1_num += 1
        elif shard_id == "shard_2":
            shard2_num += 1
        await create_record(table_name, record)
    
    return {"shard1_records": shard1_num, "shard2_records": shard2_num}

@app.post("/test/rebalance")
async def rebalance_data():
    async with httpx.AsyncClient() as client:
        for shard_id, shard_info in registered_shards.items():
            shard_url = get_read_url_for_shard_id(shard_id)
            response = await client.get(f"{shard_url}/tables/")
            if response.status_code == 200:
                tables = response.json()

                shard_table_names = {table["name"] for table in tables}
                expected_table_names = set(tables_meta.keys())
                missing_tables = expected_table_names - shard_table_names

                for missing_table in missing_tables:
                    table = Table(name=missing_table, primary_key=tables_meta[missing_table]["primary_key"], sort_key= tables_meta[missing_table]["sort_key"])
                    write_shard_url = shard_info["leader"]
                    response = await client.post(f"{write_shard_url}/tables/", json=table.model_dump())
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
            for shard_id, shard_info in registered_shards.items():
                shard_url = shard_info["leader"]
                current_data = {}
                current_data[shard_url] = (await client.get(f"{shard_url}/tables/{table_name}/")).json()
                
                for shard_url,records in current_data.items():
                    for record in records:
                        if (record[pk], record[sk]) in seen_records:
                            continue
                        seen_records.add((record[pk], record[sk]))

                        total_records += 1
                        record_id = str(record[pk])
                        correct_shard_id = get_shard_id(record_id)
                        correct_shard_url = registered_shards[correct_shard_id]["leader"]

                        if correct_shard_id != shard_id:
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
    shard_url = list(registered_shards.values())[0]["leader"]
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

        for shard_id, shard_info in registered_shards.items():
            if len(shard_info["replicas"]) < W:
                raise HTTPException(status_code=503, detail=f"Write Quorum not met (alive < {W})!")
            
        async with httpx.AsyncClient() as client:
            for shard_id, shard_info in registered_shards.items():
                if len(shard_info["replicas"]) < W:
                    raise HTTPException(status_code=503, detail=f"Write Quorum not met (alive < {W})!")
                shard_url = shard_info["leader"]
                response = await client.post(f"{shard_url}/tables/", json=table.model_dump())
                if response.status_code != 200:
                    raise HTTPException(status_code=400, detail=f"Failed to create table '{table.name}' on shard '{shard_id}': {response.text}")
                await wait_write_quorum(shard_id,table.name)
        tables_meta[table.name] = {
            "primary_key": table.primary_key,
            "sort_key": table.sort_key
        }
        return table
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/tables/{table_name}")
async def delete_table(table_name: str):
    if table_name not in tables_meta:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    try:
        for shard_id, shard_info in registered_shards.items():
            if len(shard_info["replicas"]) < W:
                raise HTTPException(status_code=503, detail=f"Write Quorum not met (alive < {W})!")
        async with httpx.AsyncClient() as client:
            for shard_id, shard_info in registered_shards.items():
                if len(shard_info["replicas"]) < W:
                    raise HTTPException(status_code=503, detail=f"Write Quorum not met (alive < {W})!")
                shard_url = shard_info["leader"]
                responce = await client.delete(f"{shard_url}/tables/{table_name}")
                if responce.status_code == 200:
                    success = True
                    await wait_write_quorum(shard_id,table_name)
                else:
                    raise HTTPException(status_code=400, detail=f"Failed to delete table '{table_name}' on shard '{shard_id}': {responce.text}")
        tables_meta.pop(table_name, None)       
        return {f"Table '{table_name}' deleted": success}
    except HTTPException:
        raise
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

            shard_url = get_leader_url(str(record[primary_key]))
            shard_id = get_shard_id(str(record[primary_key]))
            responce = await client.post(f"{shard_url}/tables/{table_name}/", json=record)
            
            if responce.status_code != 200:
                raise HTTPException(status_code=400, detail=f"Failed to create record in table '{table_name}' on shard '{shard_url}': {responce.text}")
            await wait_write_quorum(shard_id,table_name, str(record[primary_key]))
        return record
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/tables/{table_name}/exists/{record_id}/{sort_val}")
async def record_exists(table_name: str, record_id: str,sort_val:str):
    table = tables_meta.get(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    async with httpx.AsyncClient() as client:
        shard_id = get_shard_id(record_id)
        response = await read_quorum_url(shard_id,f"tables/{table_name}/exists/{record_id}/{sort_val}" )
        if response.status_code != 200:
            raise HTTPException(status_code=400, detail=f"Failed to check record existence in table '{table_name}' on shard '{shard_id}': {response.text}")
    exists = response.json().get("exists", False)
    return {"exists": exists}

@app.get("/tables/{table_name}/")
async def read_records(table_name: str):
    table = tables_meta.get(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    async with httpx.AsyncClient() as client:
        records = []
        for shard_id in registered_shards.keys():
            response = await read_quorum_url(shard_id,f"tables/{table_name}/" )
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
        shard_id = get_shard_id(record_id)
        response = await read_quorum_url(shard_id,f"tables/{table_name}/{record_id}" )
        if response.status_code != 200:
            raise HTTPException(status_code=404, detail=f"Record '{record_id}' not found in table '{table_name}'")
    return response.json()

@app.get("/tables/{table_name}/{record_id}/{sort_val}")
async def read_record(table_name:str, record_id:str, sort_val:str):
    table = tables_meta.get(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    async with httpx.AsyncClient() as client:
        shard_id = get_shard_id(record_id)
        response = await read_quorum_url(shard_id,f"tables/{table_name}/{record_id}/{sort_val}" )    
        if response.status_code != 200:
            raise HTTPException(status_code=404, detail=f"Record '{record_id}' not found in table '{table_name}'")
    return response.json()    

@app.delete("/tables/{table_name}/{record_id}")
async def delete_record_by_id(table_name: str, record_id: str):
    table = tables_meta.get(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    async with httpx.AsyncClient() as client:
        shard_url = get_leader_url(record_id)
        response = await client.delete(f"{shard_url}/tables/{table_name}/{record_id}")
        
        if response.status_code != 200:
            raise HTTPException(status_code=404, detail=f"Record '{record_id}' not found in table '{table_name}'")
    shard_id = get_shard_id(record_id)
    await wait_write_quorum(shard_id,table_name, record_id)
    return response.json()

@app.delete("/tables/{table_name}/{record_id}/{sort_val}")
async def delete_record(table_name: str, record_id: str,sort_val:str):
    table = tables_meta.get(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    async with httpx.AsyncClient() as client:
        shard_url = get_leader_url(record_id)
        response = await client.delete(f"{shard_url}/tables/{table_name}/{record_id}/{sort_val}")
        if response.status_code != 200:
            raise HTTPException(status_code=404, detail=f"Record '{record_id}' not found in table '{table_name}'")
    shard_id = get_shard_id(record_id)
    await wait_write_quorum(shard_id, table_name, record_id)
    return response.json()

@app.put("/tables/{table_name}/{record_id}/{sort_val}")
async def update_record(table_name: str, record_id: str,sort_val:str, updated_record: dict):
    pk = tables_meta[table_name]["primary_key"]
    if not pk:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    if pk in updated_record and updated_record[pk] != record_id:
        raise HTTPException(status_code=400, detail=f"Cannot change the primary key '{pk}' of the record")
    async with httpx.AsyncClient() as client:
        shard_url = get_leader_url(record_id)
        response = await client.put(f"{shard_url}/tables/{table_name}/{record_id}/{sort_val}", json=updated_record)
        if response.status_code != 200:
            raise HTTPException(status_code=404, detail=f"Record '{record_id}' not found in table '{table_name}'")
    shard_id = get_shard_id(record_id)
    await wait_write_quorum(shard_id, table_name, record_id)
    return response.json()


