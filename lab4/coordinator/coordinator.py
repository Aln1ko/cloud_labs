from fastapi import FastAPI, HTTPException, Request,Response
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
from monitoring import prom_histogram, prom_counter, cluster_alive_replicas, cluster_dead_replicas, shard_replication_lag
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from contextvars import ContextVar
import os
from urllib.parse import quote

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
trace_id_var = ContextVar("trace_id", default="system")

APP_VERSION = os.getenv("APP_VERSION", "4.0.0")

rebalance_lock = asyncio.Lock()
rebalance_pending = False

rebalance_state: dict[str, Any] = {
    "status": "idle",        # "idle" | "running" | "completed" | "failed"
    "reason": None,
    "started_at": None,
    "finished_at": None,
    "total_records": 0,
    "moved_records": 0,
    "stayed_records": 0,
    "distribution": {},
    "error": None,
    "runs_count": 0
}

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
            # Перевіряємо чи всі репліки живі, якщо ні записуємо їх як мертві
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
                cluster_alive_replicas.labels(shard_id = shard_id).set(len(shard_info["replicas"]))
           
            # Перевіряємо чи не воскресла репліка, якщо так пишемо що вона жива
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
                cluster_dead_replicas.labels(shard_id = dead_shard_id).set(len(dead_shards_urls))

            # Перевіряємо наскільки репліки відстають від лідера
            for shard_id,shard_info in registered_shards.items():
                for replica in list(shard_info["replicas"]):
                    try:
                        leader_url = registered_shards[shard_id]["leader"]
                        if replica != leader_url:
                            version = (await client.get(f"{leader_url}/offset")).json()
                            offset = (await client.get(f"{replica}/offset")).json()

                            if version > offset:
                                lag = max(0, version  - offset)
                                shard_replication_lag.labels(shard_id=shard_id, replica_url=replica).set(lag)
                            else: shard_replication_lag.labels(shard_id=shard_id, replica_url=replica).set(0)
                    except httpx.RequestError:
                        pass

def log_info(service: str, message: str, **extra):
    log_data = {
        "timestamp": datetime.now().isoformat(),
        "service": service,
        "trace_id": trace_id_var.get(),
        "message": message
    }
    log_data.update(extra)
    print(json.dumps(log_data, ensure_ascii=False), flush=True)

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
        async with httpx.AsyncClient(headers={"X-Trace-Id": trace_id_var.get()}) as client:
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
        async with httpx.AsyncClient(headers={"X-Trace-Id": trace_id_var.get()}) as client:
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

async def wait_for_shard_ready(client: httpx.AsyncClient, shard_url: str, retries: int = 10, delay: float = 0.5) -> bool:
    """Чекає, поки новий шард завершить ініціалізацію БД після /register."""
    for _ in range(retries):
        try:
            res = await client.get(f"{shard_url}/health", timeout=1.5)
            if res.status_code == 200 and res.json().get("shard_id") is not None:
                return True
        except httpx.RequestError:
            pass
        await asyncio.sleep(delay)
    return False

async def run_rebalance_task(reason: str = "manual"):
    global rebalance_pending

    # Якщо ребалансування вже виконується — ставимо в чергу повторний запуск
    if rebalance_lock.locked():
        rebalance_pending = True
        log_info("coordinator", "Rebalance already running, queued next run", reason=reason)
        return

    async with rebalance_lock:
        while True:
            rebalance_pending = False
            rebalance_state.update({
                "status": "running",
                "reason": reason,
                "started_at": datetime.now().isoformat(),
                "finished_at": None,
                "total_records": 0,
                "moved_records": 0,
                "stayed_records": 0,
                "distribution": {},
                "error": None
            })

            log_info("coordinator", "Background rebalance started", reason=reason)
            try:
                # Пауза, щоб новий шард встиг зберегти свою конфігурацію та завантажити БД
                await asyncio.sleep(1.0)
                async with httpx.AsyncClient(headers={"X-Trace-Id": trace_id_var.get()}, timeout=10.0) as client:
                    shards_snapshot = list(registered_shards.items())
                    tables_snapshot = list(tables_meta.items())

                    # 1. Створюємо відсутні таблиці на лідерах усіх шардів
                    for shard_id, shard_info in shards_snapshot:
                        leader_url = shard_info["leader"]
                        ready = await wait_for_shard_ready(client, leader_url)
                        if not ready:
                            raise RuntimeError(f"Leader of {shard_id} ({leader_url}) is not ready")

                        response = await client.get(f"{leader_url}/tables/")
                        if response.status_code != 200:
                            raise RuntimeError(f"Failed to read tables from shard '{shard_id}': {response.text}")

                        shard_table_names = {t["name"] for t in response.json()}
                        missing_tables = set(tables_meta.keys()) - shard_table_names

                        for missing_table in missing_tables:
                            table = Table(
                                name=missing_table,
                                primary_key=tables_meta[missing_table]["primary_key"],
                                sort_key=tables_meta[missing_table]["sort_key"]
                            )
                            create_res = await client.post(f"{leader_url}/tables/", json=table.model_dump())
                            if create_res.status_code != 200:
                                raise RuntimeError(
                                    f"Failed to create table '{missing_table}' on '{shard_id}': {create_res.text}"
                                )

                    # 2. Переносимо ключі, які за consistent hashing тепер належать іншому шарду
                    total_records = 0
                    moved_records = 0
                    seen_records = set()
                    distribution = {sid: 0 for sid, _ in shards_snapshot}

                    for table_name, meta in tables_snapshot:
                        pk = meta["primary_key"]
                        sk = meta["sort_key"]

                        for shard_id, shard_info in shards_snapshot:
                            shard_url = shard_info["leader"]
                            res = await client.get(f"{shard_url}/tables/{table_name}/")
                            if res.status_code != 200:
                                continue

                            for record in res.json():
                                rec_key = (table_name, str(record[pk]), str(record[sk]))
                                if rec_key in seen_records:
                                    continue
                                seen_records.add(rec_key)

                                total_records += 1
                                record_id = str(record[pk])
                                correct_shard_id = get_shard_id(record_id)
                                correct_shard_url = registered_shards[correct_shard_id]["leader"]
                                distribution[correct_shard_id] = distribution.get(correct_shard_id, 0) + 1

                                if correct_shard_id != shard_id:
                                    # Спочатку записуємо на новий шард, потім видаляємо зі старого
                                    post_res = await client.post(
                                        f"{correct_shard_url}/tables/{table_name}/",
                                        json=record
                                    )
                                    if post_res.status_code != 200:
                                        raise RuntimeError(
                                            f"Failed to move record '{record[pk]}' to '{correct_shard_id}': {post_res.text}"
                                        )

                                    del_res = await client.delete(
                                        f"{shard_url}/tables/{table_name}/{quote(str(record[pk]), safe='')}/{quote(str(record[sk]), safe='')}"
                                    )
                                    if del_res.status_code != 200:
                                        raise RuntimeError(
                                            f"Failed to delete record '{record[pk]}' from '{shard_id}': {del_res.text}"
                                        )

                                    moved_records += 1
                                    rebalance_state["moved_records"] = moved_records
                                    rebalance_state["total_records"] = total_records

                                # Не блокуємо event loop для звичайних клієнтських запитів
                                await asyncio.sleep(0)

                # 3. Очищаємо кеш читання, щоб не віддавати старі дані
                cache.clear()

                rebalance_state.update({
                    "status": "completed",
                    "finished_at": datetime.now().isoformat(),
                    "total_records": total_records,
                    "moved_records": moved_records,
                    "stayed_records": total_records - moved_records,
                    "distribution": distribution,
                    "runs_count": rebalance_state["runs_count"] + 1
                })
                log_info(
                    "coordinator",
                    "Background rebalance completed",
                    reason=reason,
                    total_records=total_records,
                    moved_records=moved_records,
                    distribution=distribution
                )

            except Exception as e:
                rebalance_state.update({
                    "status": "failed",
                    "finished_at": datetime.now().isoformat(),
                    "error": str(e)
                })
                log_info("coordinator", "Background rebalance failed", reason=reason, error=str(e))

            if not rebalance_pending:
                break
            reason = "queued_new_shard"

@app.middleware("http")
async def rate_limit_middleware(requests:Request,call_next):
    # Пропускаємо службові запити без лімітів!
    if requests.url.path in ["/register","/metadata","/docs","/shards", "/test/overload","/metrics",
                             "/test/replication_lag","/health","/version","/cluster/health","/rebalance/status", "/test/rebalance" ]:
        return await call_next(requests)

    await token_bucket()
    # trace id
    trace_id = requests.headers.get("X-Trace-Id") or str(uuid.uuid4())
    trace_id_var.set(trace_id)

    #monitoring data
    start_time = time.perf_counter()
    response = await call_next(requests)
    duration =  time.perf_counter() - start_time

    if requests.scope.get("route"):
        endpoint = requests.scope.get("route").path
    else: endpoint = requests.url.path

    log_info("coordinator",
             "Request processed on coordinator",
             method = requests.method, 
             endpoint =endpoint, 
             status_code = response.status_code)

    response.headers["X-Trace-Id"] = trace_id
    
    prom_histogram.labels(service = "coordinator", method = requests.method, endpoint = endpoint  ).observe(duration)
    prom_counter.labels(service = "coordinator", method = requests.method, endpoint =endpoint, status_code = response.status_code).inc()

    return response


@app.get("/metrics")
def get_metrics():
    metrics = generate_latest()
    return Response(content= metrics,media_type=CONTENT_TYPE_LATEST )

@app.post("/register")
async def register_shard(shard: ShardRegistration):
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
    is_new_shard = False
    if assigned_id not in registered_shards:
        is_new_shard = True
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
    cluster_alive_replicas.labels(shard_id = assigned_id).set(len(registered_shards[assigned_id]["replicas"]))
    cluster_dead_replicas.labels(shard_id = assigned_id).set(0)

    rebalance_triggered = False
    if is_new_shard and len(registered_shards) > 1 and len(tables_meta) > 0:
        asyncio.create_task(run_rebalance_task(reason=f"new_shard:{assigned_id}"))
        rebalance_triggered = True
        print(f"[Coordinator] Запущено фонове ребалансування через додавання {assigned_id}")

    return {
        "status": "ok",
        "shard_id": assigned_id,
        "replica_id": replica_id,
        "role": role,
        "rebalance_triggered": rebalance_triggered,
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

@app.get("/health")
def get_health():
    shards_summary = {}
    for shard_id, shard_info in registered_shards.items():
        shards_summary[shard_id] = {
            "leader": shard_info["leader"],
            "alive": shard_info["replicas"],
            "dead": dead_shards.get(shard_id, [])
        }
    return {
        "status": "ok",
        "shards_count": len(registered_shards),
        "shards": shards_summary
    }

@app.get("/version")
def get_app_version():
    return {"service": "coordinator", "version": APP_VERSION}

async def fetch_replica_status(client: httpx.AsyncClient, url: str) -> dict:
    """Паралельно опитує /health та /version однієї репліки."""
    try:
        health_res, version_res = await asyncio.gather(
            client.get(f"{url}/health", timeout=1.5),
            client.get(f"{url}/version", timeout=1.5)
        )
        return {
            "url": url,
            "reachable": True,
            "health": health_res.json() if health_res.status_code == 200 else {"status_code": health_res.status_code},
            "version": version_res.json() if version_res.status_code == 200 else {"status_code": version_res.status_code}
        }
    except Exception as e:
        return {
            "url": url,
            "reachable": False,
            "health": {"status": "unreachable"},
            "version": None,
            "error": str(e)
        }


@app.get("/cluster/health")
async def get_cluster_health():
    """Опитує /health і /version на всіх відомих репліках усіх шардів."""
    cluster_shards = {}
    any_dead = False
    quorum_lost = False

    async with httpx.AsyncClient() as client:
        for shard_id, shard_info in registered_shards.items():
            # Збираємо і живі, і мертві репліки (без дублікатів)
            all_urls = list(dict.fromkeys(shard_info["replicas"] + dead_shards.get(shard_id, [])))

            # Опитуємо всі репліки шарда паралельно
            replicas_statuses = await asyncio.gather(
                *(fetch_replica_status(client, url) for url in all_urls)
            )

            alive_count = sum(1 for r in replicas_statuses if r["reachable"])
            dead_count = len(replicas_statuses) - alive_count

            if dead_count > 0:
                any_dead = True
            if alive_count < W:
                quorum_lost = True

            cluster_shards[shard_id] = {
                "leader": shard_info["leader"],
                "alive_count": alive_count,
                "dead_count": dead_count,
                "quorum_met": alive_count >= W,
                "replicas": replicas_statuses
            }

    if not registered_shards or quorum_lost:
        overall_status = "unhealthy"
    elif any_dead:
        overall_status = "degraded"
    else:
        overall_status = "ok"

    return {
        "cluster_status": overall_status,
        "coordinator": {
            "status": "ok",
            "version": APP_VERSION
        },
        "shards_count": len(registered_shards),
        "shards": cluster_shards
    }

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
async def trigger_rebalance():
    asyncio.create_task(run_rebalance_task(reason="manual"))
    return {
        "status": "started",
        "message": "Rebalancing started in background. Check GET /rebalance/status for progress."
    }
@app.get("/rebalance/status")
def get_rebalance_status():
    return rebalance_state

@app.post("/test/overload")
async def overload():
    shard_url = list(registered_shards.values())[0]["leader"]
    async with httpx.AsyncClient(headers={"X-Trace-Id": trace_id_var.get()}) as client:
        for i in range(10):
            response = await client.get(f"{shard_url}/tables/")
            if response.status_code != 200:
                return JSONResponse(status_code = response.status_code, content=response.json())
    return JSONResponse(status_code=response.status_code, content=response.json())


@app.post("/test/replication_lag")
async def pause_replication(paused: bool = True):
   shard1_leader = registered_shards["shard_1"]["leader"]
   for replica_url in registered_shards["shard_1"]["replicas"]:
       if shard1_leader != replica_url:
           async with httpx.AsyncClient() as client:
                response = await client.post(f"{replica_url}/test/pause_sync", json = {"paused": paused})
                return response.json()

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
            
        async with httpx.AsyncClient(headers={"X-Trace-Id": trace_id_var.get()}) as client:
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
        async with httpx.AsyncClient(headers={"X-Trace-Id": trace_id_var.get()}) as client:
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
        async with httpx.AsyncClient(headers={"X-Trace-Id": trace_id_var.get()}) as client:
            primary_key = tables_meta[table_name]["primary_key"]
            sort_key = tables_meta[table_name]["sort_key"]
            if primary_key not in record:
                record[primary_key] = str(uuid.uuid4())
            if sort_key not in record:
                record[sort_key] = str(datetime.now().isoformat())
            if "version_ts" not in record:
                record["version_ts"] = time.time()

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
    async with httpx.AsyncClient(headers={"X-Trace-Id": trace_id_var.get()}) as client:
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
    async with httpx.AsyncClient(headers={"X-Trace-Id": trace_id_var.get()}) as client:
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
    async with httpx.AsyncClient(headers={"X-Trace-Id": trace_id_var.get()}) as client:
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
    async with httpx.AsyncClient(headers={"X-Trace-Id": trace_id_var.get()}) as client:
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
    async with httpx.AsyncClient(headers={"X-Trace-Id": trace_id_var.get()}) as client:
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
    async with httpx.AsyncClient(headers={"X-Trace-Id": trace_id_var.get()}) as client:
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
    async with httpx.AsyncClient(headers={"X-Trace-Id": trace_id_var.get()}) as client:
        if "version_ts" not in updated_record:
            updated_record["version_ts"] = time.time()
        shard_url = get_leader_url(record_id)
        response = await client.put(f"{shard_url}/tables/{table_name}/{record_id}/{sort_val}", json=updated_record)
        if response.status_code != 200:
            raise HTTPException(status_code=404, detail=f"Record '{record_id}' not found in table '{table_name}'")
    shard_id = get_shard_id(record_id)
    await wait_write_quorum(shard_id, table_name, record_id)
    return response.json()


