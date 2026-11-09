from fastapi import FastAPI, HTTPException,Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uuid
import json
from typing import Any, Optional
from pathlib import Path
import os
import asyncio
import httpx
from contextlib import asynccontextmanager
import socket
from datetime import datetime
import time


# Отримуємо налаштування з Docker
SHARD_ID = socket.gethostname() 
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://coordinator:8000")

DB_PATH = Path(f"/app/data/{SHARD_ID}_data.json")


async def send_metadata_to_coordinator():
    url = f"{COORDINATOR_URL}/metadata"
    for table in db.list_tables():
        payload = {"name": table.name, "primary_key": table.primary_key, "sort_key": table.sort_key}
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, json=payload,timeout = 2)
                if response.status_code == 200:
                    print(f"[Shard {SHARD_ID}] Successfully sent metadata for table '{table.name}' to coordinator.")
                else:
                    print(f"[Shard {SHARD_ID}] Failed to send metadata for table '{table.name}' to coordinator. Status code: {response.status_code}")
        except Exception as e:
            print(f"[Shard {SHARD_ID}] Error sending metadata for table '{table.name}' to coordinator: {e}")

async def register_with_coordinator():
    global db, SHARD_ID
    url = f"{COORDINATOR_URL}/register"
    payload = {"url": f"http://{SHARD_ID}:8000"}

    for attempt in range(10):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, json=payload,timeout = 2)
                
                if response.status_code == 200:
                    assigned_id = response.json()["shard_id"]
                    SHARD_ID = assigned_id  # оновлюємо ім'я шарда на красиве "shard_1"

                    db_path = Path(f"/app/data/{assigned_id}_data.json")
                    db = JsonDatabase(db_path).load()

                    print(f"[Shard {assigned_id}] Successfully registered with coordinator.")
                    await send_metadata_to_coordinator() 
                    return
        except Exception as e:
            print(f"[Shard {SHARD_ID}] Attempt {attempt + 1}: Failed to register with coordinator. Error: {e}")
            await asyncio.sleep(2)  # Wait before retrying
    print(f"[Shard {SHARD_ID}] Failed to register with coordinator after 10 attempts. Exiting.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"[Shard {SHARD_ID}] Starting up and attempting to register with coordinator...")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True) # Створюємо папку, якщо треба
    asyncio.create_task(register_with_coordinator())
    yield
    print(f"[Shard {SHARD_ID}] Shutting down.")

class Table(BaseModel):
    name: str
    primary_key: str = "id"
    sort_key: str = "timestamp"

class JsonTable:
    def __init__(self,name:str, records = None, primary_key:str = "id", sort_key: str = "timestamp"):
        self.name = name
        self.primary_key = primary_key
        self.sort_key = sort_key
        self.records = list(records) if records is not None else []
        self._sort_records()

    def _sort_records(self):
        self.records.sort(key = lambda x: (str(x.get(self.primary_key, "")), str(x.get(self.sort_key, ""))))

    #CRUD operations
    def add(self,record:dict) -> dict:
        if self.primary_key not in record:
            record[self.primary_key] = str(uuid.uuid4())
        if self.sort_key not in record:
            record[self.sort_key] = datetime.now().isoformat()
        self.records.append(record)
        self._sort_records()
        return record

    def get_by_partition(self, record_id: str) -> list[dict]:
        records = []
        for record in self.records:
            if record.get(self.primary_key) == record_id:
                records.append(record)
        if records:
            records.sort(key = lambda x: str(x.get(self.sort_key,"")))
            return records
        else: return None

    def get(self, record_id:str, sort_key:str) -> Optional[dict]:
        for record in self.records:
            if record.get(self.primary_key) == record_id and record.get(self.sort_key) == sort_key:
                return record
        return None

    def get_all(self) -> list:
        return list(self.records)

    def delete(self, record_id:str, sort_key:str) -> bool:
        for i, record in enumerate(self.records):
            if record.get(self.primary_key) == record_id and record.get(self.sort_key) == sort_key:
                self.records.pop(i)
                return True
        return False

    def delete_by_partition(self, record_id:str) -> bool:
        old_len = len(self.records)
        self.records = [record for record in self.records if record.get(self.primary_key) != record_id]
        return len(self.records) < old_len

    def update(self,record_id:str, sort_key:str, updated_record:dict)->dict:
        for i,record in enumerate(self.records):
            if record.get(self.primary_key) == record_id and record.get(self.sort_key) == sort_key:
                self.records[i].update(updated_record)
                self._sort_records()
                return self.records[i]
        return None

    # def update_by_partition(self,record_id:str, updated_record:dict)->dict:
    #     for i,record in enumerate(self.records):
    #         if record.get(self.primary_key) == record_id:
    #             self.records[i].update(updated_record)
    #             self._sort_records()
    #             return self.records[i]
    #     return None

    def exists(self, record_id:str, sort_key:str) -> bool:
        return any(record.get(self.primary_key) == record_id and record.get(self.sort_key) == sort_key for record in self.records)

    def records_to_list(self) -> list[dict]:
        return self.records

    @classmethod
    def records_from_list(cls, name: str, data: list[dict], primary_key: str = "id", sort_key: str = "timestamp"):
        return cls(name, data, primary_key, sort_key)

class JsonDatabase:
    def __init__(self,path:str):
        self.path = Path(path)
        self.tables:dict[str, JsonTable] = {}

    def create_table(self, name:str, primary_key:str = "id", sort_key:str = "timestamp") -> JsonTable:
        if name not in self.tables:
            self.tables[name] = JsonTable(name, primary_key=primary_key, sort_key=sort_key)
        return self.tables[name]

    def get_tables(self) -> list[str]:
        return list(self.tables.keys())

    def get_table(self, name:str) -> Optional[JsonTable]:
        return self.tables.get(name)

    def delete_table(self,name:str)->bool:
        if name in self.tables:
            del self.tables[name]
            return True
        return False

    def list_tables(self) -> list[str]:
        return [Table(name = name, primary_key=table.primary_key, sort_key=table.sort_key) for name, table in self.tables.items()]

    def load(self) -> "JsonDatabase":
        if not self.path.exists():
            return self
        with open(self.path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
            for name,data in raw_data.items():
                pk = data.get("primary_key", "id")
                sk = data.get("sort_key", "timestamp")
                records = data.get("records", [])
                self.tables[name] = JsonTable.records_from_list(name, records, primary_key=pk, sort_key=sk)
        return self

    def save(self) -> None:
        raw = {
            name: {
                "primary_key": table.primary_key, 
                "sort_key": table.sort_key,
                "records": table.records_to_list()
             } for name, table in self.tables.items()
            }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(raw, f, ensure_ascii=False, indent=2)

    def exists(self,table_name:str)->bool:
        return table_name in self.tables

db: Optional[JsonDatabase] = None

app = FastAPI(lifespan=lifespan)

rate_limit = 5
requests = []
siding_window_lock = asyncio.Lock()

async def siding_window():
    global requests
    async with siding_window_lock:
        now = time.time()
        requests = [rec for rec in requests if now - 60.0 < rec]
        if len(requests) < rate_limit:
            requests.append(now)
            return True
        else: return False

@app.middleware("http")
async def rate_limit_middleware(requests:Request,call_next):
    can_we_go = await siding_window()
    if not can_we_go:
        return JSONResponse(
            status_code = 429,
            content = {"detail": f"Too Many Requests (limit: {rate_limit}/min)"}
        )
    response = await call_next(requests)
    return response   

@app.get("/tables/")
def get_tables():
    return db.list_tables()

@app.get("/tables/{table_name}/exists/")
def table_exists(table_name: str):
    exists = db.exists(table_name)
    return {"exists": exists}

@app.post("/tables/")
def create_table(table: Table):
    try:
        if db.exists(table.name):
            raise HTTPException(status_code=400, detail=f"Table '{table.name}' already exists")
        if table.primary_key is None:
            table.primary_key = "id"
        db.create_table(table.name, primary_key=table.primary_key, sort_key=table.sort_key)
        db.save()
        return table
    except Exception as e:
       raise HTTPException(status_code=400, detail=str(e))

@app.delete("/tables/{table_name}")
def delete_table(table_name: str):
    if db.exists(table_name) is False:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    try:
        success = db.delete_table(table_name)
        db.save()
        return {f"Table '{table_name}' deleted": success}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/tables/{table_name}/")   
def create_record(table_name: str, record:dict):
    table = db.get_table(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    try:
        table.add(record=record)
        db.save()
        return record
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/tables/{table_name}/exists/{record_id}/{sort_val}")
def record_exists(table_name: str, record_id: str, sort_val: str):
    table = db.get_table(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    exists = table.exists(record_id, sort_val)
    return {"exists": exists}

@app.get("/tables/{table_name}/")
def read_records(table_name: str):
    table = db.get_table(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    return table.get_all()

@app.get("/tables/{table_name}/{record_id}")
def read_records_by_id(table_name: str, record_id: str):
    table = db.get_table(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    record = table.get_by_partition(record_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Record '{record_id}' not found in table '{table_name}'")
    return record 

@app.get("/tables/{table_name}/{record_id}/{sort_val}")
def read_record(table_name: str, record_id: str, sort_val: str):
    table = db.get_table(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    record = table.get(record_id, sort_val)
    if not record:
        raise HTTPException(status_code=404, detail=f"Record with ID '{record_id}'  and sort value '{sort_val}' not found in table '{table_name}'")
    return record 

@app.delete("/tables/{table_name}/{record_id}")
def delete_records_by_id(table_name: str, record_id: str):
    table = db.get_table(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    try:
        sucess = table.delete_by_partition(record_id)
        db.save()
        return {f"Records with ID '{record_id}' deleted from table '{table_name}'": sucess}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/tables/{table_name}/{record_id}/{sort_val}")
def delete_records_by_id(table_name: str, record_id: str, sort_val: str):
    table = db.get_table(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    try:
        sucess = table.delete(record_id, sort_val)
        db.save()
        return {f"Records with ID '{record_id}' and sort value '{sort_val}' deleted from table '{table_name}'": sucess}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
       

@app.put("/tables/{table_name}/{record_id}/{sort_val}")
def update_record(table_name: str, record_id: str, sort_val: str, updated_record: dict):
    table = db.get_table(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    if table.primary_key in updated_record and updated_record[table.primary_key] != record_id:
        raise HTTPException(status_code=400, detail=f"Cannot change the primary key '{table.primary_key}' of the record")
    record = table.update(record_id, sort_val, updated_record=updated_record)
    if not record:
        raise HTTPException(status_code=404, detail=f"Record '{record_id}' with sort value '{sort_val}' not found in table '{table_name}'")

    db.save()
    return record

