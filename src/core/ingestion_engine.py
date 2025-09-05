import time
import math
import json
import random
import urllib.parse
from io import StringIO
from datetime import timezone, datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

import asyncio
import aiohttp
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.data_format import DataFormat
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties, IngestionMappingKind, ReportLevel


class Ingestor:
    def __init__(self, bootstrap: Dict[str, Any], max_concurrent_tasks: int = 3, chunk_size: int = 25000, max_thread_workers: int = 8):
        self.bootstrap = bootstrap
        self.chunk_size = chunk_size
        self.max_concurrent_tasks = max_concurrent_tasks
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)
        self.max_thread_workers = max_thread_workers
        self.thread_pool = ThreadPoolExecutor(max_workers=self.max_thread_workers)

        self.connection_config = {
            'ingest_uri': self.bootstrap["adx_ingest_uri"],
            'cluster_uri': self.bootstrap["adx_cluster_uri"],
            'client_id': self.bootstrap["clientId"],
            'client_secret': self.bootstrap["clientSecret"],
            'tenant_id': self.bootstrap["tenantId"]
        }

        self.ingest_client = self._create_adx_ingest_client()
        self.data_client = self._create_adx_data_client()

        self.defender_token_cache = {'token': None, 'expires': None}
        self.adx_token_cache = {'token': None, 'expires': None}
        self.token_lock = asyncio.Lock()

    def _create_adx_ingest_client(self):
        kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
            connection_string=self.connection_config['ingest_uri'],
            aad_app_id=self.connection_config['client_id'],
            app_key=self.connection_config['client_secret'],
            authority_id=self.connection_config['tenant_id']
        )
        return QueuedIngestClient(kcsb)
    
    def _create_adx_data_client(self):
        kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
            connection_string=self.connection_config['cluster_uri'],
            aad_app_id=self.connection_config['client_id'],
            app_key=self.connection_config['client_secret'],
            authority_id=self.connection_config['tenant_id']
        )
        return KustoClient(kcsb)
    
    async def get_defender_token(self, session: aiohttp.ClientSession) -> str:
        async with self.token_lock:
            if (self.defender_token_cache['token'] and 
                self.defender_token_cache['expires'] and
                datetime.now() < self.defender_token_cache['expires']):
                return self.defender_token_cache['token']
            
            try:
                aad_token_url = f"https://login.microsoftonline.com/{self.bootstrap['tenantId']}/oauth2/token"
                body = {
                    'resource': self.bootstrap["defender_resource_uri"],
                    'client_id': self.bootstrap["clientId"],
                    'client_secret': self.bootstrap["clientSecret"],
                    'grant_type': 'client_credentials'
                }
                
                async with session.post(
                    aad_token_url,
                    data=urllib.parse.urlencode(body),
                    headers={'Content-Type': 'application/x-www-form-urlencoded'}
                ) as response:
                    if response.status == 200:
                        token_data = await response.json()
                        token = token_data["access_token"]
                        expires_in = int(token_data.get("expires_in", 3600))
                        
                        self.defender_token_cache = {
                            'token': token,
                            'expires': datetime.now() + timedelta(seconds=expires_in - 300)
                        }
                        print("[INFO] --> Defender Token acquired")
                        return token
                    else:
                        error_text = await response.text()
                        raise Exception(f"Failed to get Defender token: {response.status} - {error_text}")
                        
            except Exception as e:
                self.defender_token_cache = {'token': None, 'expires': None}
                raise Exception(f"Defender token acquisition failed: {str(e)}")

    async def get_adx_token(self, session: aiohttp.ClientSession) -> str:
        """Get ADX API token with improved caching and error isolation"""
        async with self.token_lock:
            if (self.adx_token_cache['token'] and 
                self.adx_token_cache['expires'] and
                datetime.now() < self.adx_token_cache['expires']):
                return self.adx_token_cache['token']
            
            try:
                aad_token_url = f"https://login.microsoftonline.com/{self.bootstrap['tenantId']}/oauth2/token"
                body = {
                    "grant_type": "client_credentials",
                    "client_id": self.bootstrap["clientId"],
                    "client_secret": self.bootstrap["clientSecret"],
                    "resource": self.bootstrap["adx_cluster_uri"]
                }
                
                async with session.post(
                    aad_token_url,
                    data=body,
                ) as response:
                    if response.status == 200:
                        token_data = await response.json()
                        token = token_data["access_token"]
                        expires_in = int(token_data.get("expires_in", 3600))
                        
                        self.adx_token_cache = {
                            'token': token,
                            'expires': datetime.now() + timedelta(seconds=expires_in - 300)
                        }
                        print("[INFO] --> ADX Token acquired")
                        return token
                    else:
                        error_text = await response.text()
                        raise Exception(f"Failed to get ADX token: {response.status} - {error_text}")
                        
            except Exception as e:
                self.adx_token_cache = {'token': None, 'expires': None}
                raise Exception(f"ADX token acquisition failed: {str(e)}")
    
    def build_base_kql_query(self, source_tbl: str, load_type: str, watermark_column: str, high_watermark: datetime) -> str:        
        if load_type == "Full" or not high_watermark:
            if watermark_column == "Watermark_IngestionTime":
                return f"{source_tbl} | extend {watermark_column} = ingestion_time()"
            else:
                return source_tbl
        else:
            if watermark_column == "Watermark_IngestionTime":
                return f"{source_tbl} | extend {watermark_column} = ingestion_time() | where {watermark_column} > datetime('{high_watermark}')"
            else:
                return f"{source_tbl} | where {watermark_column} > datetime('{high_watermark}')"
    
    def build_chunked_kql_query(self, base_query: str, watermark_column: str, chunk_index: int, chunk_size: int) -> str:
        start_rownum = (chunk_index-1) * chunk_size
        end_rownum = start_rownum + chunk_size
        return f"{base_query} | sort by {watermark_column} asc | extend rownum = row_number() | where rownum between ({start_rownum+1} .. {end_rownum}) | project-away rownum"

    async def get_record_count(self, session: aiohttp.ClientSession, base_query: str) -> int:
        count_query = f"{base_query} | count"
        
        try:
            defender_token = await self.get_defender_token(session)
            headers = {
                "Authorization": f"Bearer {defender_token}",
                "Content-Type": "application/json"
            }
            
            async with session.post(
                self.bootstrap['defender_hunting_api_url'],
                headers=headers,
                json={"Query": count_query}
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    records = result.get("Results", [])
                    if records and len(records) > 0:
                        return records[0].get("Count", 0)
                    return 0
                else:
                    error_text = await response.text()
                    raise Exception(f"Failed to get record count: {response.status} - {error_text}")
                    
        except Exception as e:
            print(f"[ERROR] --> Error getting record count: {str(e)}")
            raise

    async def calculate_chunks(self, session: aiohttp.ClientSession, base_query: str) -> Tuple[int, int]:
        try:
            total_records = await self.get_record_count(session, base_query)
            if total_records == 0:
                return 0, 0
            
            num_chunks = math.ceil(total_records / self.chunk_size)
            # print(f"[INFO] --> Total records: {total_records:,}, Chunk size: {self.chunk_size:,}, Number of chunks: {num_chunks}")
            return total_records, num_chunks
        except Exception as e:
            print(f"[ERROR] --> Error calculating chunks: {e}")
            return 0, 0

    def ensure_table_exists(self, destination_folder: str, destination_tbl: str, watermark_column: str) -> None:
        try:
            create_table_cmd = f".create-merge table {destination_tbl} ({watermark_column}: datetime, RawData: dynamic) with (folder = '{destination_folder}')"
            self.data_client.execute(self.bootstrap["adx_database"], create_table_cmd)
            print(f"[INFO] --> Table {destination_tbl} created/verified")

            mapping = [
                {"column": watermark_column, "path": f"$.{watermark_column}", "datatype": "datetime"},
                {"column": "RawData", "path": "$", "datatype": "dynamic"},
            ]
            create_mapping_cmd = (
                f".create-or-alter table {destination_tbl} ingestion json mapping 'RawDataMap' "
                f"'{json.dumps(mapping)}'"
            )            
            self.data_client.execute(self.bootstrap["adx_database"], create_mapping_cmd)
            print(f"[INFO] --> Ingestion JSON mapping created for {destination_tbl}")
            
        except Exception as e:
            print(f"[ERROR] --> Error creating table {destination_tbl}: {str(e)}")
            raise

    def meta_insert_configs(self, ingestion_results: Dict[str, Any], table_configs: List[Dict[str, Any]]) -> None:
        table_lookup = {item["DestinationTable"]: item for item in table_configs}

        for r in ingestion_results:
            if r.get("chunk_results"):
                try:
                    max_high_watermark = r["chunk_results"][-1]["high_watermark"] if r["chunk_results"][-1]["high_watermark"] else 'null'

                    source_table = table_lookup.get(r["table"])["SourceTable"]
                    destination_folder = table_lookup.get(r["table"])["DestinationFolder"]
                    watermark_column = table_lookup.get(r["table"])["WatermarkColumn"]
                    load_type = table_lookup.get(r["table"])["LoadType"]
                    
                    if r["success"]:
                        append_command = f"""
                            .set-or-append {self.bootstrap['config_table']} <|
                            datatable (
                                SourceTable: string,
                                DestinationFolder: string,
                                DestinationTable: string,
                                WatermarkColumn: string,
                                LastRefreshedTime: datetime,
                                HighWatermark: datetime,
                                LoadType: string,
                                IsActive: bool
                            )
                            [
                                '{source_table}',
                                '{destination_folder}',
                                '{r["table"]}',
                                '{watermark_column}',
                                '{self.bootstrap["ingestion_start_time"]}',
                                datetime('{max_high_watermark}'),
                                '{load_type}',
                                true
                            ]
                        """
                    else:
                        append_command = f"""
                            .set-or-append {self.bootstrap['config_table']} <|
                            datatable (
                                SourceTable: string,
                                DestinationFolder: string,
                                DestinationTable: string,
                                WatermarkColumn: string,
                                LastRefreshedTime: datetime,
                                HighWatermark: datetime,
                                LoadType: string,
                                IsActive: bool
                            )
                            [
                                '{source_table}',
                                '{destination_folder}',
                                '{r["table"]}',
                                '{watermark_column}',
                                '{self.bootstrap["ingestion_start_time"]}',
                                datetime('{max_high_watermark}'),
                                '{load_type}',
                                false
                            ]
                        """

                    self.data_client.execute_mgmt(self.bootstrap["adx_database"], append_command)

                    print(f"[INFO] --> Updated config for {r['table']} to {max_high_watermark}")
                except Exception as e:
                    print(f"[ERROR] --> Error updating config for {r['table']}: {str(e)}")
                    raise

    def meta_insert_audits(self, ingestion_id: str, ingestion_start_time: str, ingestion_results: Dict[str, Any]) -> None:
        insert_values = []
        for data in ingestion_results:
            error_val = '""' if data["error"] is None else str(data["error"]).replace('"', "")
            if data.get('chunk_results'):
                chunk_results = json.dumps(data['chunk_results']).replace('"', '\\"')
                chunk_results = f'dynamic({json.dumps(data["chunk_results"])})'
            else:
                chunk_results = 'dynamic([])'
            insert_values.append(
                f'"{ingestion_id}", datetime({ingestion_start_time}), "{data["folder"]}", "{data["table"]}", {str(data["success"]).lower()}, '
                f'{data["records_processed"]}, {data["chunked"]}, {data["chunks_processed"]}, {data["chunks_failed"]}, '
                f'{chunk_results}, "{error_val}"'
            )
            insert_values_str = ",\n    ".join(insert_values)

        kql_command = f"""
        .set-or-append {self.bootstrap["audit_table"]} <|
        datatable(ingestion_id:string, ingestion_timestamp: datetime, folder: string, table: string, success: bool, records_processed: long, chunked: bool, chunks_processed: int, chunks_failed: int, chunk_results: dynamic, error: string)
        [
        {insert_values_str}
        ]"""

        try:
            self.data_client.execute_mgmt(self.bootstrap["adx_database"], kql_command)
            print("[INFO] --> Inserted audit records")
        except Exception as e:
            print(f"Error inserting audit records: {e}")

    def meta_insert_chunk_failures(self, ingestion_id: str, ingestion_start_time: str, ingestion_results: Dict[str, Any]) -> None:
        insert_values = []
        for data in ingestion_results:
            if data.get("chunk_results"):
                for r in data["chunk_results"]:
                    if not r["success"]:
                        low_watermark = r["low_watermark"] if r["low_watermark"] else 'null'
                        high_watermark = r["high_watermark"] if r["high_watermark"] else 'null'
                        error_val = '""' if r["error"] is None else str(r["error"]).replace('"', "")
                        insert_values.append(
                            f'"{ingestion_id}", datetime({ingestion_start_time}), '
                            f'"{r["folder"]}", "{r["table"]}", {r["chunk_id"]}, {str(r["success"]).lower()}, '
                            f'{r["records_count"]}, {r["records_processed"]}, '
                            f'datetime({low_watermark}), datetime({high_watermark}), "{error_val}", "false"'
                        )
                insert_values_str = ",\n    ".join(insert_values)
        
        if insert_values != []:
            kql_command = f"""
            .set-or-append {self.bootstrap["chunk_audit_table"]} <|
            datatable(ingestion_id:string, ingestion_time:datetime, folder:string, table:string, chunk_id:int, success:bool, records_count:int, records_processed:int, low_watermark:datetime, high_watermark:datetime, error:string, reprocess_success:bool)
            [
            {insert_values_str}
            ]"""

            try:
                self.data_client.execute_mgmt(self.bootstrap["adx_database"], kql_command)
                print("[INFO] --> Inserted chunk failure records")
            except Exception as e:
                print(f"Error inserting chunk failure records: {e}")
                raise


    def analyze_results(self, table_configs: List[Dict[str, Any]], ingestion_results: List[Dict[str, Any]], execution_time: float) -> Dict[str, Any]:
        successful_tables = 0
        failed_tables = 0
        total_records_processed = 0
        total_chunks_processed = 0
        total_chunks_failed = 0
        exceptions = []
        detailed_results = []
        
        for i, result in enumerate(ingestion_results):
            table_name = table_configs[i]["SourceTable"]
            
            if isinstance(result, Exception):
                print(f"[ERROR] --> Exception in {table_name}: {str(result)}")
                exceptions.append(f"{table_name}: {str(result)}")
                failed_tables += 1
                detailed_results.append({
                    "table": table_name,
                    "success": False,
                    "error": str(result)
                })
            elif isinstance(result, dict):
                detailed_results.append(result)
                if result.get("success"):
                    successful_tables += 1
                    print(f"[SUCCESS] --> {table_name}: {result.get('records_processed', 0)} records")
                else:
                    failed_tables += 1
                    print(f"[ERROR] --> {table_name}: {result.get('error', 'Unknown error')}")
                
                total_records_processed += result.get("records_processed", 0)
                total_chunks_processed += result.get("chunks_processed", 0)
                total_chunks_failed += result.get("chunks_failed", 0)
                
                if result.get("error"):
                    exceptions.extend([f"{table_name}: {e}" for e in result["error"]])
                
        summary = {
            "total_tables": len(table_configs),
            "successful_tables": successful_tables,
            "failed_tables": failed_tables,
            "total_records_processed": total_records_processed,
            "total_chunks_processed": total_chunks_processed,
            "total_chunks_failed": total_chunks_failed,
            "execution_time_seconds": execution_time,
            "exceptions": exceptions,
            "detailed_results": detailed_results,
            "chunking_enabled": True,
            "chunk_size": self.chunk_size
        }
        
        print(f"\n" + f"="*100)
        print(f"PROCESSING COMPLETED")
        print(f"="*100)
        print(f"Execution time: {execution_time:.2f} seconds")
        print(f"Tables - Successful: {successful_tables}, Failed: {failed_tables}")
        print(f"Records processed: {total_records_processed:,}")
        print(f"Chunks - Successful: {total_chunks_processed}, Failed: {total_chunks_failed}")
        
        if exceptions:
            print(f"\nErrors encountered:")
            for error in exceptions[:5]:  # Show first 5 errors
                print(f"  - {error}")
            if len(exceptions) > 5:
                print(f"  ... and {len(exceptions) - 5} more errors")

        return summary

    def _sync_ingest_data(self, records: List[Dict], chunk_index: int, destination_folder: str, destination_tbl: str, low_watermark: str, high_watermark: str) -> None:
        """Synchronous data ingestion - runs in thread pool"""
        max_retries = 5
        retry_attempts = 0
        backoff_factor = 2
        max_backoff = 60

        result = {
            "chunk_id": chunk_index,
            "folder": destination_folder,
            "table": destination_tbl,
            "success": False,
            "records_count": len(records),
            "records_processed": 0,
            "low_watermark": low_watermark,
            "high_watermark": high_watermark,
            "error": None
        }

        while retry_attempts < max_retries:
            try:
                data_as_str = "\n".join(json.dumps(r) for r in records)
                data_stream = StringIO(data_as_str)
                
                ingestion_props = IngestionProperties(
                    database=self.bootstrap["adx_database"],
                    table=destination_tbl,
                    data_format=DataFormat.JSON,
                    ingestion_mapping_kind=IngestionMappingKind.JSON,
                    ingestion_mapping_reference="RawDataMap",
                )

                self.ingest_client.ingest_from_stream(data_stream, ingestion_props)

                result["success"] = True
                result["records_processed"] = result["records_count"]
                result["error"] = None
                print(f"[INFO - {threading.current_thread().name}] --> Successfully ingested {len(records)} records to {destination_tbl}")
                
                return result
            
            except Exception as e:
                error_message = str(e).lower()
                
                if (
                    type(e).__name__ == "KustoBlobError" 
                    and ("timeout" in error_message or "timed out" in error_message)
                ) or (
                    "timeout" in error_message 
                    or "timed out" in error_message
                ):
                    retry_attempts += 1
                    if retry_attempts < max_retries:
                        base_wait_time = min(backoff_factor ** retry_attempts, max_backoff)
                        jitter = random.uniform(0, 0.5) * base_wait_time
                        wait_time = max(0.1, base_wait_time + jitter)
                        thread_offset = (chunk_index % 10) * 0.1 
                        wait_time += thread_offset
                        print(f"[WARNING] --> Timeout error during ingestion of chunk {chunk_index} to {destination_tbl}, attempt {retry_attempts}/{max_retries}: {str(e)}. Retrying in {wait_time:.2f} seconds...")
                        time.sleep(wait_time)
                        continue
                    else:
                        result["success"] = False
                        result["error"] = f"Max retries reached: {str(e)[:500]}"
                        print(f"[ERROR] --> Ingestion failed for {destination_tbl} after {max_retries} attempts: {str(e)}")
                        return result
                else:
                    result["success"] = False
                    result["error"] = str(e)[:500]
                    print(f"[ERROR] --> Ingestion failed for {destination_tbl}: {str(e)}")
                    return result

    async def ingest_to_adx(self, records: List[Dict], chunk_index: int, destination_folder: str, destination_tbl: str, watermark_column: str) -> None:
        print("[FUNCTION] --> ingest_to_adx")

        try:
            low_watermark = min(item[watermark_column] for item in records)
            high_watermark = max(item[watermark_column] for item in records)

            loop = asyncio.get_event_loop()
            chunk_result = await loop.run_in_executor(
                self.thread_pool,
                self._sync_ingest_data,
                records,
                chunk_index,
                destination_folder,
                destination_tbl,
                low_watermark,
                high_watermark
            )

            # if not chunk_result["success"]:
            #     print(f"[ERROR] --> Ingestion failed for {destination_tbl} chunk {chunk_index}: {chunk_result['error']}")
            # else:
            #     print(f"[INFO] --> Successfully ingested to {destination_tbl}")
            
            return chunk_result
            
        except Exception as e:
            # print(f"[ERROR] --> Ingestion failed for {destination_tbl}: {str(e)}")
            
            return {
                "chunk_id": chunk_index,
                "folder": destination_folder,
                "table": destination_tbl,
                "success": False,
                "records_count": len(records),
                "records_processed": 0,
                "low_watermark": low_watermark,
                "high_watermark": high_watermark,
                "error": f"Async wrapper error: {str(e)[:500]}"
            }

    async def process_single_chunk(
        self, 
        session: aiohttp.ClientSession, 
        table_config: Dict[str, Any], 
        base_query: str, 
        chunk_index: int, 
        total_chunks: int,
        disable_chunking: bool = False
    ) -> Dict[str, Any]:
        """Process a single chunk of data with better error isolation"""
        print("-"*80)
        print("[FUNCTION] --> process_single_chunk")
        source_tbl = table_config["SourceTable"]
        destination_folder = table_config["DestinationFolder"]
        destination_tbl = table_config["DestinationTable"]
        watermark_column = table_config["WatermarkColumn"]

        max_retries = 5
        retry_attempts = 0
        # backoff_factor = 2
        # max_backoff = 60
        
        try:
            if disable_chunking:
                chunked_query = base_query
            else:
                chunked_query = self.build_chunked_kql_query(base_query, watermark_column, chunk_index, self.chunk_size)
            
            print(f"[INFO] --> Processing {source_tbl} chunk {chunk_index}/{total_chunks}")
            
            defender_token = await self.get_defender_token(session)
            headers = {
                "Authorization": f"Bearer {defender_token}",
                "Content-Type": "application/json"
            }
            
            while retry_attempts < max_retries:
                async with session.post(
                    self.bootstrap['defender_hunting_api_url'],
                    headers=headers,
                    json={"Query": chunked_query},
                    timeout=aiohttp.ClientTimeout(total=300)
                ) as response:
                    if response.status == 429:
                        retry_attempts += 1
                        if retry_attempts < max_retries:
                            retry_after = int(response.headers.get("Retry-After", "60"))
                            jitter = random.uniform(0, 0.2) * retry_after
                            wait_time = retry_after + jitter
                            print(f"[WARNING] --> Rate limited by Defender API. Retrying chunk {chunk_index} after {wait_time:.2f} seconds...")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            error_text = await response.text()
                            return {
                                "folder": destination_folder,
                                "table": destination_tbl,
                                "success": False,
                                "chunk_id": chunk_index,
                                "records_count": 0,
                                "records_processed": 0,
                                "low_watermark": None,
                                "high_watermark": None,
                                "error": f"Rate limited by Defender API: {response.status} - {error_text}",
                            }
                    elif response.status != 200:
                        error_text = await response.text()
                        return {
                            "folder": destination_folder,
                            "table": destination_tbl,
                            "success": False,
                            "chunk_id": chunk_index,
                            "records_count": 0,
                            "records_processed": 0,
                            "low_watermark": None,
                            "high_watermark": None,
                            "error": f"API call failed: {response.status} - {error_text}",
                        }
                
                    apijson = await response.json()
                    records = apijson.get("Results", [])
                    
                    if not records:
                        return {
                            "folder": destination_folder,
                            "table": destination_tbl,
                            "success": True,
                            "chunk_id": chunk_index,
                            "records_count": 0,
                            "records_processed": 0,
                            "low_watermark": None,
                            "high_watermark": None,
                            "error": None
                        }
                    
                    chunk_result = await self.ingest_to_adx(records, chunk_index, destination_folder, destination_tbl, watermark_column)
                    
                    if not chunk_result["success"]:
                        print(f"[ERROR] --> Chunk ingestion failed for {source_tbl} chunk {chunk_index}/{total_chunks}: {chunk_result['error']}")
                    else:
                        print(f"[INFO] --> Successfully processed {source_tbl} chunk {chunk_index}/{total_chunks} - {len(records):,} records")

                    return chunk_result
                
        except Exception as e:
            print(f"[ERROR] --> Error processing chunk for {source_tbl} (chunk {chunk_index}): {str(e)}")
            
            return {
                "success": False,
                "chunk_id": chunk_index,
                "error": str(e),
                "records_processed": 0
            }
        
    async def process_multiple_chunks_parallel(
        self, 
        session: aiohttp.ClientSession, 
        table_config: Dict[str, Any], 
        base_query: str, 
        num_chunks: int
    ) -> List[Dict[str, Any]]:
        # Group chunks into batches for thread pool processing
        batch_size = min(self.max_thread_workers, num_chunks)
        chunk_batches = [
            list(range(i, min(i + batch_size, num_chunks+1))) 
            for i in range(1, num_chunks+1, batch_size)
        ]

        processed_results = []
        
        for batch_indices in chunk_batches:
            # Process batch of chunks concurrently
            batch_tasks = [
                self.process_single_chunk(session, table_config, base_query, chunk_index, num_chunks)
                for chunk_index in batch_indices
            ]
            
            chunk_results  = await asyncio.gather(*batch_tasks, return_exceptions=True)
            processed_results.extend(chunk_results)
            
            # Small delay between batches to prevent overwhelming the system
            if len(chunk_batches) > 1:
                await asyncio.sleep(1)
        
        return processed_results

    
    async def process_single_table(self, session: aiohttp.ClientSession, table_config: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single table with chunking support and better error isolation"""
        async with self.semaphore:
            source_tbl = table_config["SourceTable"]
            destination_folder = table_config["DestinationFolder"]
            destination_tbl = table_config["DestinationTable"]
            load_type = table_config['LoadType']
            watermark_column = table_config["WatermarkColumn"]
            high_watermark = table_config["HighWatermark"]
            
            try:
                print("-"*80)
                print("[FUNCTION] --> process_single_table")
                print(f"[INFO] --> Starting processing for table: {source_tbl}")
                
                # Build base query
                base_query = self.build_base_kql_query(
                    source_tbl, 
                    load_type, 
                    watermark_column,
                    high_watermark
                )
                
                print(f"[INFO] --> Base query for {source_tbl}: {base_query}")
                
                total_records, num_chunks = await self.calculate_chunks(session, base_query)
                
                if total_records == 0:
                    print(f"[INFO] --> No records found for {source_tbl}")
                    return {
                        "folder": destination_folder,
                        "table": destination_tbl,
                        "success": True,
                        "records_count": 0, 
                        "records_processed": 0,
                        "chunks_processed": 0,
                        "chunks_failed": 0,
                        "chunked": False,
                        "error": None
                    }

                needs_chunking = total_records > self.chunk_size
                
                if not needs_chunking:
                    print(f"[INFO] --> {source_tbl} has {total_records:,} records - processing without chunking")
                    
                    result = await self.process_single_chunk(session, table_config, base_query, 1, num_chunks, True)
                    
                    return {
                        "folder": destination_folder,
                        "table": result["table"],
                        "success": result["success"],
                        "records_processed": result["records_processed"],
                        "chunks_processed": 1 if result["success"] else 0,
                        "chunks_failed": 0 if result["success"] else 1,
                        "chunked": False,
                        "chunk_results": [result],
                        "error": result.get("error", None)
                    }
                else:
                    print(f"[INFO] --> {source_tbl} has {total_records:,} records - processing with {num_chunks} chunks")
                    
                    chunk_results = await self.process_multiple_chunks_parallel(
                        session, table_config, base_query, num_chunks
                    )
                    
                    successful_chunks = sum(1 for r in chunk_results if isinstance(r, dict) and r.get("success"))
                    failed_chunks = num_chunks - successful_chunks
                    total_records_processed = sum(
                        r.get("records_processed", 0) 
                        for r in chunk_results 
                        if isinstance(r, dict)
                    )
                    
                    # Collect errors
                    errors = [
                        r.get("error", str(r)) 
                        for r in chunk_results 
                        if isinstance(r, dict) and not r.get("success") or isinstance(r, Exception)
                    ]
                    
                    return {
                        "folder": destination_folder,
                        "table": destination_tbl,
                        "success": failed_chunks == 0,
                        "records_processed": total_records_processed,
                        "chunks_processed": successful_chunks,
                        "chunks_failed": failed_chunks,
                        "chunked": True,
                        "chunk_results": chunk_results,
                        "error": errors if errors else None
                    }
                    
            except Exception as e:
                print(f"[ERROR] --> Error processing table {source_tbl}: {str(e)}")
                return {
                    "folder": destination_folder,
                    "table": destination_tbl,
                    "success": False,
                    "records_processed": 0,
                    "chunks_processed": 0,
                    "chunks_failed": 0,
                    "chunked": False,
                    "error": str(e)
                }
                
    async def process_all_tables(self, table_configs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process all tables concurrently"""
        
        print("-"*80)
        print("[FUNCTION] --> process_all_tables")
        print(f"[INFO] --> Chunk size: {self.chunk_size:,} records")
        print(f"[INFO] --> Max concurrent tasks: {self.max_concurrent_tasks}")
        print(f"[INFO] --> Max thread workers: {self.max_thread_workers}")

        start_time = time.time()

        for config in table_configs:
            if not config["HighWatermark"]:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    self.thread_pool,
                    self.ensure_table_exists,
                    config["DestinationFolder"],
                    config["DestinationTable"],
                    config["WatermarkColumn"]
                )
        
        timeout = aiohttp.ClientTimeout(total=900)  # 15 minutes timeout
        connector = aiohttp.TCPConnector(limit=50, limit_per_host=10)  # Connection pooling
        
        print(f"[INFO] --> Starting concurrent processing of {len(table_configs)} tables")

        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            tasks = [self.process_single_table(session, config) for config in table_configs]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        end_time = time.time()
        execution_time = end_time - start_time

        self.meta_insert_configs(results, table_configs)

        self.meta_insert_audits(
            self.bootstrap["ingestion_id"],
            self.bootstrap["ingestion_start_time"],
            results
        )

        self.meta_insert_chunk_failures(
            self.bootstrap["ingestion_id"],
            self.bootstrap["ingestion_start_time"],
            results
        )

        summary = self.analyze_results(table_configs, results, execution_time)
        
        return summary