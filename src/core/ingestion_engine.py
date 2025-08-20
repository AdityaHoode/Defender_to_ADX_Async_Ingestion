import time
import math
import json
import urllib.parse
from io import StringIO
from datetime import timezone, datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

import asyncio
import aiohttp

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.data_format import DataFormat
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties, IngestionMappingKind, ReportLevel
from azure.kusto.ingest.status import KustoIngestStatusQueues

from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


class ConcurrentDefenderIngestionWithChunking:
    def __init__(self, bootstrap: Dict[str, Any], max_concurrent_tasks: int = 3, chunk_size: int = 25000):
        self.bootstrap = bootstrap
        self.max_concurrent_tasks = max_concurrent_tasks
        self.chunk_size = chunk_size
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)

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

        self.update_lock = asyncio.Lock()

        self.max_thread_workers = 8
        self.thread_pool = ThreadPoolExecutor(max_workers=self.max_thread_workers)

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
            # Check if cached token is still valid
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
                        expires_in = int(token_data.get("expires_in", 3600))  # Default 1 hour
                        
                        # Cache with expiration (subtract 5 minutes for safety)
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
                # Clear cache on error to force retry
                self.defender_token_cache = {'token': None, 'expires': None}
                raise Exception(f"Defender token acquisition failed: {str(e)}")

    async def get_adx_token(self, session: aiohttp.ClientSession) -> str:
        """Get ADX API token with improved caching and error isolation"""
        async with self.token_lock:
            # Check if cached token is still valid
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
                        
                        # Cache with expiration
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
                # Clear cache on error
                self.adx_token_cache = {'token': None, 'expires': None}
                raise Exception(f"ADX token acquisition failed: {str(e)}")

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
    
    def build_base_kql_query(self, source_tbl: str, load_type: str, watermark_column: str, high_watermark: datetime) -> str:
        print("[FUNCTION] --> build_base_kql_query")
        
        if load_type == "Full" or not high_watermark:
            return source_tbl
        else:
            formatted_ts = high_watermark.isoformat()
            return f"{source_tbl} | where {watermark_column} > datetime('{formatted_ts}')"
    
    def build_chunked_kql_query(self, base_query: str, watermark_column: str, chunk_index: int, chunk_size: int) -> str:
        start_rownum = chunk_index * chunk_size
        end_rownum = start_rownum + chunk_size
        return f"{base_query} | sort by {watermark_column} asc | extend RowNum = row_number() | where RowNum between ({start_rownum+1} .. {end_rownum})"
    
    async def calculate_chunks(self, session: aiohttp.ClientSession, base_query: str) -> Tuple[int, int]:
        print("[FUNCTION] --> calculate_chunks")
        try:
            total_records = await self.get_record_count(session, base_query)
            if total_records == 0:
                return 0, 0
            
            num_chunks = math.ceil(total_records / self.chunk_size)
            print(f"[INFO] --> Total records: {total_records:,}, Chunk size: {self.chunk_size:,}, Number of chunks: {num_chunks}")
            return total_records, num_chunks
        except Exception as e:
            print(f"[ERROR] --> Error calculating chunks: {e}")
            return 0, 0

    async def ensure_table_exists(self, destination_tbl: str, watermark_column: str) -> None:
        try:
            # Create fresh data client for table operations
            # data_client = self._create_adx_data_client()
            
            create_table_cmd = f".create-merge table {destination_tbl} ({watermark_column}: datetime, RawData: dynamic)"
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

    def _sync_ingest_data(self, records: List[Dict], destination_tbl: str) -> None:
        """Synchronous data ingestion - runs in thread pool"""
        try:
            
            # CPU-intensive JSON serialization
            data_as_str = "\n".join(json.dumps(r) for r in records)
            data_stream = StringIO(data_as_str)
            
            ingestion_props = IngestionProperties(
                database=self.bootstrap["adx_database"],
                table=destination_tbl,
                data_format=DataFormat.JSON,
                ingestion_mapping_kind=IngestionMappingKind.JSON,
                ingestion_mapping_reference="RawDataMap",
            )
            
            # Blocking I/O operation
            self.ingest_client.ingest_from_stream(data_stream, ingestion_props)
            print(f"[INFO] --> Successfully ingested {len(records)} records to {destination_tbl}")
            
        except Exception as e:
            print(f"[ERROR] --> Ingestion failed for {destination_tbl}: {str(e)}")
            raise

    async def ingest_to_adx(self, session: aiohttp.ClientSession, table_config: Dict[str, Any], records: List[Dict], destination_tbl: str, watermark_column: str) -> None:
        print("[FUNCTION] --> ingest_to_adx")

        # if not table_config["HighWatermark"] and not table_config["LastRefreshedTime"]:
        #     await self.ensure_table_exists(destination_tbl, watermark_column)

        try:
            # ingest_client = self._create_adx_ingest_client()
            
            # data_as_str = "\n".join(json.dumps(r) for r in records)
            # data_stream = StringIO(data_as_str)

            # print(f"[INFO] --> Ingesting {len(records)} records to {destination_tbl}...")
            
            # ingestion_props = IngestionProperties(
            #     database=self.bootstrap["adx_database"],
            #     table=destination_tbl,
            #     data_format=DataFormat.JSON,
            #     ingestion_mapping_kind=IngestionMappingKind.JSON,
            #     ingestion_mapping_reference="RawDataMap",
            # )
            
            # self.ingest_client.ingest_from_stream(data_stream, ingestion_props)

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                self.thread_pool,
                self._sync_ingest_data,
                records,
                destination_tbl
            )

            print(f"[INFO] --> Successfully ingested to {destination_tbl}")

            # max_timestamp = max(item[watermark_column] for item in records)

            # await self.update_high_watermark(session, table_config, max_timestamp)
            
        except Exception as e:
            print(f"[ERROR] --> Ingestion failed for {destination_tbl}: {str(e)}")
            raise

    async def update_high_watermark(self, session: aiohttp.ClientSession, table_config: Dict[str, Any], max_timestamp: str) -> None:
        async with self.update_lock:
            source_tbl = table_config["SourceTable"]
            destination_tbl = table_config["DestinationTable"]
            watermark_column = table_config["WatermarkColumn"]
            
            try:
                # Use fresh data client for watermark operations
                # data_client = self._create_adx_data_client()
                
                # max_cmd = f"{destination_tbl} | summarize max({watermark_column}) | project max_timestamp = max_{watermark_column}"
                # max_timestamp = self.data_client.execute(self.bootstrap["adx_database"], max_cmd).primary_results[0][0]["max_timestamp"]
                print(f"[INFO] --> Retrieved high watermark for {destination_tbl}: {max_timestamp}")

                update_cmd_1 = f"""
                    .update table {self.bootstrap["config_table"]} delete D append A <|
                        let D = {self.bootstrap["config_table"]}
                        | where DestinationTable=='{destination_tbl}' and SourceTable=='{source_tbl}';
                        let A = {self.bootstrap["config_table"]}
                        | where DestinationTable=='{destination_tbl}' and SourceTable=='{source_tbl}'
                        | extend HighWatermark=datetime('{max_timestamp}');
                """
                update_cmd_2 = f"""
                    .update table {self.bootstrap["config_table"]} delete D append A <|
                        let D = {self.bootstrap["config_table"]}
                        | where DestinationTable=='{destination_tbl}' and SourceTable=='{source_tbl}';
                        let A = {self.bootstrap["config_table"]}
                        | where DestinationTable=='{destination_tbl}' and SourceTable=='{source_tbl}'
                        | extend LastRefreshedTime=datetime('{self.bootstrap["ingestion_start_time"]}');
                """

                adx_token = await self.get_adx_token(session)
                adx_ingest_uri = f"{self.bootstrap['adx_cluster_uri']}/v1/rest/mgmt"
                adx_headers = {"Authorization": f"Bearer {adx_token}", "Content-Type": "application/json"}

                for cmd in [update_cmd_1, update_cmd_2]:
                    audit_update_payload = {"db": self.bootstrap["adx_database"], "csl": cmd.strip()}
                    async with session.post(
                        adx_ingest_uri, 
                        headers=adx_headers, 
                        json=audit_update_payload
                    ) as response:
                        if response.status != 200:
                            text = await response.text()
                            raise Exception(f"Config table update failed: {response.status}, {text}")
                        print(f"[INFO] --> Config table updated for {destination_tbl}")
                        
            except Exception as e:
                print(f"[ERROR] --> Error updating watermark for {destination_tbl}: {str(e)}")
                raise

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
        destination_tbl = table_config["DestinationTable"]
        watermark_column = table_config["WatermarkColumn"]
        
        try:
            # Build chunked query
            if disable_chunking:
                chunked_query = base_query
            else:
                chunked_query = self.build_chunked_kql_query(base_query, watermark_column, chunk_index, self.chunk_size)
            
            print(f"[INFO] --> Processing {source_tbl} chunk {chunk_index+1}/{total_chunks}")
            
            # Get fresh token for this request
            defender_token = await self.get_defender_token(session)
            headers = {
                "Authorization": f"Bearer {defender_token}",
                "Content-Type": "application/json"
            }
            
            # Make API call with timeout
            async with session.post(
                self.bootstrap['defender_hunting_api_url'],
                headers=headers,
                json={"Query": chunked_query},
                timeout=aiohttp.ClientTimeout(total=300)  # 5 minute timeout per chunk
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    return {
                        "success": False,
                        "chunk_index": chunk_index,
                        "error": f"API call failed: {response.status} - {error_text}",
                        "records_processed": 0
                    }
                
                apijson = await response.json()
                records = apijson.get("Results", [])
                
                if not records:
                    return {
                        "success": True,
                        "chunk_index": chunk_index,
                        "records_processed": 0
                    }
                
                # Ingest chunk to ADX with isolation
                await self.ingest_to_adx(session, table_config, records, destination_tbl, watermark_column)
                
                print(f"[INFO] --> Successfully processed {source_tbl} chunk {chunk_index + 1}/{total_chunks} - {len(records):,} records")

                return {
                    "success": True,
                    "chunk_index": chunk_index,
                    "records_processed": len(records)
                }
                
        except Exception as e:
            print(f"[ERROR] --> Error processing chunk for {source_tbl} (chunk {chunk_index}): {str(e)}")
            return {
                "success": False,
                "chunk_index": chunk_index,
                "error": str(e),
                "records_processed": 0
            }
        
    async def process_multiple_chunks_parallel(self, session: aiohttp.ClientSession, 
                                             table_config: Dict[str, Any], 
                                             base_query: str, 
                                             num_chunks: int) -> List[Dict[str, Any]]:
        """Process multiple chunks with a hybrid approach"""
        
        # Group chunks into batches for thread pool processing
        batch_size = min(self.max_thread_workers, num_chunks)
        chunk_batches = [
            list(range(i, min(i + batch_size, num_chunks))) 
            for i in range(0, num_chunks, batch_size)
        ]
        
        all_results = []
        
        for batch_indices in chunk_batches:
            # Process batch of chunks concurrently
            batch_tasks = [
                self.process_single_chunk(session, table_config, base_query, chunk_idx, num_chunks)
                for chunk_idx in batch_indices
            ]
            
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            all_results.extend(batch_results)
            
            # Small delay between batches to prevent overwhelming the system
            if len(chunk_batches) > 1:
                await asyncio.sleep(1)
        
        return all_results

    
    async def process_single_table(self, session: aiohttp.ClientSession, table_config: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single table with chunking support and better error isolation"""
        async with self.semaphore:
            source_tbl = table_config["SourceTable"]
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
                
                # Calculate chunks needed
                total_records, num_chunks = await self.calculate_chunks(session, base_query)
                
                if total_records == 0:
                    print(f"[INFO] --> No records found for {source_tbl}")
                    return {
                        "table": source_tbl,
                        "success": True,
                        "total_records": 0,
                        "chunks_processed": 0,
                        "chunks_failed": 0,
                        "chunked": False
                    }

                needs_chunking = total_records > self.chunk_size
                
                if not needs_chunking:
                    print(f"[INFO] --> {source_tbl} has {total_records:,} records - processing without chunking")
                    
                    result = await self.process_single_chunk(session, table_config, base_query, 0, num_chunks, True)
                    
                    # # Wait for ingestion to complete
                    # await asyncio.sleep(180)

                    # # Update watermark
                    # await self.update_high_watermark(session, table_config)
                    
                    return {
                        "table": source_tbl,
                        "success": result["success"],
                        "total_records": result["records_processed"],
                        "chunks_processed": 1 if result["success"] else 0,
                        "chunks_failed": 0 if result["success"] else 1,
                        "chunked": False,
                        "error": result.get("error")
                    }
                else:
                    print(f"[INFO] --> {source_tbl} has {total_records:,} records - processing with {num_chunks} chunks")
                    
                    # # Process all chunks for this table
                    # chunk_tasks = [
                    #     self.process_single_chunk(session, table_config, base_query, i, num_chunks)
                    #     for i in range(num_chunks)
                    # ]
                    # chunk_results = await asyncio.gather(*chunk_tasks, return_exceptions=True)

                    # chunk_results = []
                    # for i in range(num_chunks):
                    #     result = await self.process_single_chunk(session, table_config, base_query, i, num_chunks)
                    #     chunk_results.append(result)

                    # Wait for ingestion to complete
                    # await asyncio.sleep(180)

                    # # Update watermark
                    # await self.update_high_watermark(session, table_config)

                    chunk_results = await self.process_multiple_chunks_parallel(
                        session, table_config, base_query, num_chunks
                    )
                    
                    # Analyze chunk results
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
                        "table": source_tbl,
                        "success": failed_chunks == 0,
                        "total_records": total_records_processed,
                        "chunks_processed": successful_chunks,
                        "chunks_failed": failed_chunks,
                        "chunked": True,
                        "errors": errors if errors else None
                    }
                    
            except Exception as e:
                print(f"[ERROR] --> Error processing table {source_tbl}: {str(e)}")
                return {
                    "table": source_tbl,
                    "success": False,
                    "total_records": 0,
                    "chunks_processed": 0,
                    "chunks_failed": 0,
                    "chunked": False,
                    "error": str(e)
                }
    
    async def process_all_tables(self, table_configs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process all tables concurrently with improved error isolation"""
        start_time = time.time()
        
        print("-"*80)
        print("[FUNCTION] --> process_all_tables")
        print(f"[INFO] --> Starting concurrent processing of {len(table_configs)} tables with improved isolation...")
        print(f"[INFO] --> Chunk size: {self.chunk_size:,} records")
        print(f"[INFO] --> Max concurrent tasks: {self.max_concurrent_tasks}")
        
        # Create aiohttp session with longer timeout for better resilience
        timeout = aiohttp.ClientTimeout(total=900)  # 15 minutes timeout
        connector = aiohttp.TCPConnector(limit=50, limit_per_host=10)  # Connection pooling
        
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            # Create tasks for all tables with better exception handling
            tasks = [self.process_single_table(session, config) for config in table_configs]
            
            # Execute all tasks concurrently with return_exceptions=True
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Analyze results
        successful_tables = 0
        failed_tables = 0
        total_records_processed = 0
        total_chunks_processed = 0
        total_chunks_failed = 0
        exceptions = []
        detailed_results = []
        
        for i, result in enumerate(results):
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
                    print(f"[SUCCESS] --> {table_name}: {result.get('total_records', 0)} records")
                else:
                    failed_tables += 1
                    print(f"[ERROR] --> {table_name}: {result.get('error', 'Unknown error')}")
                
                total_records_processed += result.get("total_records", 0)
                total_chunks_processed += result.get("chunks_processed", 0)
                total_chunks_failed += result.get("chunks_failed", 0)
                
                if result.get("errors"):
                    exceptions.extend([f"{table_name}: {e}" for e in result["errors"]])
        
        end_time = time.time()
        execution_time = end_time - start_time
        
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