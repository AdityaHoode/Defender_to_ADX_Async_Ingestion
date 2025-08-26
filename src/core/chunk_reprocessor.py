import time
import math
import json
import asyncio
import aiohttp
from datetime import timezone, datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple, Union
from concurrent.futures import ThreadPoolExecutor

from core.ingestion_engine import ConcurrentDefenderIngestionWithChunking

class DefenderIngestionReprocessor(ConcurrentDefenderIngestionWithChunking):
    
    def __init__(self, bootstrap: Dict[str, Any], max_concurrent_tasks: int = 3, chunk_size: int = 25000, max_thread_workers: int = 8):
        
        super().__init__(bootstrap, max_concurrent_tasks, chunk_size, max_thread_workers)
        
    def get_failed_chunks(self) -> List[Dict[str, Any]]:

        print("[FUNCTION] --> get_failed_chunks")
        
        base_query = f"""
            {self.bootstrap["chunk_audit_table"]}
            | where reprocess_success == false and isnotnull(low_watermark) and isnotnull(high_watermark)
        """
        
        try:
            response = self.data_client.execute(self.bootstrap["adx_database"], base_query)
            failed_chunks = []
            
            for row in response.primary_results[0]:
                failed_chunks.append({
                    "ingestion_id": row["ingestion_id"],
                    "table": row["table"],
                    "chunk_id": row["chunk_id"],
                    "low_watermark": row["low_watermark"],
                    "high_watermark": row["high_watermark"],
                    "records_count": row["records_count"],
                })
            
            print(f"[INFO] --> Found {len(failed_chunks)} failed chunks to reprocess")
            return failed_chunks
            
        except Exception as e:
            print(f"[ERROR] --> Error retrieving failed chunks: {str(e)}")
            raise

    def get_table_config_for_chunk(self, table_name: str) -> Dict[str, Any]:

        query = f"""
            {self.bootstrap["config_table"]}
            | where DestinationTable == '{table_name}'
            | project SourceTable, DestinationTable, WatermarkColumn
        """
        
        try:
            response = self.data_client.execute(self.bootstrap["adx_database"], query)
            
            if response.primary_results[0].rows_count == 0:
                raise Exception(f"No configuration found for table: {table_name}")
            
            row = response.primary_results[0][0]
            return {
                "SourceTable": row["SourceTable"],
                "DestinationTable": row["DestinationTable"],
                "WatermarkColumn": row["WatermarkColumn"]
            }
            
        except Exception as e:
            print(f"[ERROR] --> Error retrieving table config for {table_name}: {str(e)}")
            raise

    def build_watermark_based_query(
        self, source_table: str, 
        watermark_column: str, 
        low_watermark: datetime, 
        high_watermark: datetime
    ) -> str:
        
        low_watermark_str = low_watermark.isoformat()
        high_watermark_str = high_watermark.isoformat()
        
        query = (
            f"{source_table} "
            f"| where {watermark_column} >= datetime('{low_watermark_str}') "
            f"and {watermark_column} <= datetime('{high_watermark_str}') "
            f"| sort by {watermark_column} asc"
        )
        
        return query
    
    def meta_update_chunk_failures(
        self, 
        reprocess_results: List[Dict[str, Any]]
    ) -> None:
        
        print("[FUNCTION] --> meta_update_chunk_failures")

        for result in reprocess_results:
            if result.get("success"):
                update_cmd = f"""
                    .update table {self.bootstrap["chunk_audit_table"]} delete D append A <|
                        let D = {self.bootstrap["chunk_audit_table"]}
                        | where ingestion_id=='{result["ingestion_id"]}' and table=='{result["table"]}' and chunk_id=={result["chunk_id"]} and success==false;
                        let A = {self.bootstrap["chunk_audit_table"]}
                        | where ingestion_id=='{result["ingestion_id"]}' and table=='{result["table"]}' and chunk_id=={result["chunk_id"]} and success==false
                        | extend reprocess_success=true;
                """   

                try:
                    self.data_client.execute_mgmt(self.bootstrap["adx_database"], update_cmd)
                    print("[INFO] --> Inserted reprocess audit records")
                except Exception as e:
                    print(f"[ERROR] --> Error inserting reprocess audit records: {e}")
                    raise

    async def reprocess_single_chunk(
        self, 
        session: aiohttp.ClientSession,
        failed_chunk: Dict[str, Any],
        source_table: str,
        watermark_column: str,
    ) -> Dict[str, Any]:

        print("-"*60)
        print(f"[FUNCTION] --> reprocess_single_chunk")
        
        table_name = failed_chunk["table"]
        chunk_id = failed_chunk["chunk_id"]
        low_watermark = failed_chunk["low_watermark"]
        high_watermark = failed_chunk["high_watermark"]
        
        try:
            print(f"[INFO] --> Reprocessing {table_name} chunk {chunk_id}")
            print(f"[INFO] --> Watermark range: {low_watermark} to {high_watermark}")
            
            watermark_query = self.build_watermark_based_query(
                source_table, watermark_column, low_watermark, high_watermark
            )
            
            print(f"[INFO] --> Watermark query: {watermark_query}")
            
            defender_token = await self.get_defender_token(session)
            headers = {
                "Authorization": f"Bearer {defender_token}",
                "Content-Type": "application/json"
            }
            
            async with session.post(
                self.bootstrap['defender_hunting_api_url'],
                headers=headers,
                json={"Query": watermark_query},
                timeout=aiohttp.ClientTimeout(total=300)  # 5 minute timeout
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    return {
                        "success": False,
                        "chunk_id": chunk_id,
                        "table": table_name,
                        "error": f"API call failed: {response.status} - {error_text}",
                        "records_processed": 0,
                        "low_watermark": low_watermark.isoformat(),
                        "high_watermark": high_watermark.isoformat(),
                    }
                
                apijson = await response.json()
                records = apijson.get("Results", [])
                
                if not records:
                    print(f"[WARNING] --> No records found for {table_name} chunk {chunk_id}")
                    return {
                        "success": True,
                        "chunk_id": chunk_id,
                        "table": table_name,
                        "records_processed": 0,
                        "low_watermark": low_watermark.isoformat(),
                        "high_watermark": high_watermark.isoformat(),
                        "error": "No records found in range"
                    }
                
                ingest_result = await self.ingest_to_adx(records, chunk_id, table_name, watermark_column)
                
                result = {
                    "ingestion_id": failed_chunk["ingestion_id"],
                    "success": ingest_result["success"],
                    "chunk_id": chunk_id,
                    "table": table_name,
                    "records_processed": ingest_result["records_processed"],
                    "low_watermark": low_watermark.isoformat(),
                    "high_watermark": high_watermark.isoformat(),
                    "error": ingest_result.get("error", None)
                }
                
                if ingest_result["success"]:
                    print(f"[SUCCESS] --> Reprocessed {table_name} chunk {chunk_id} - {len(records):,} records")
                else:
                    print(f"[ERROR] --> Failed to reprocess {table_name} chunk {chunk_id}: {ingest_result['error']}")
                
                return result
                
        except Exception as e:
            print(f"[ERROR] --> Error reprocessing chunk {chunk_id} for {table_name}: {str(e)}")
            
            return {
                "success": False,
                "chunk_id": chunk_id,
                "table": table_name,
                "error": str(e)[:500],
                "records_processed": 0,
                "low_watermark": low_watermark.isoformat(),
                "high_watermark": high_watermark.isoformat(),
            }

    async def reprocess_failed_chunks(self) -> Dict[str, Any]:

        print("="*100)
        print("STARTING CHUNK REPROCESSING")
        print("="*100)

        start_time = time.time()
        
        try:
            failed_chunks = self.get_failed_chunks()
            
            if not failed_chunks:
                print("[INFO] --> No failed chunks found to reprocess")
                return {
                    "total_chunks": 0,
                    "successful_chunks": 0,
                    "failed_chunks": 0,
                    "detailed_results": []
                }
            
            table_configs = {}
            for chunk in failed_chunks:
                table_name = chunk["table"]
                if table_name not in table_configs:
                    table_configs[table_name] = self.get_table_config_for_chunk(table_name)
            
            print(f"[INFO] --> Reprocessing {len(failed_chunks)} chunks across {len(table_configs)} tables")
            
            timeout = aiohttp.ClientTimeout(total=900)  # 15 minutes timeout
            connector = aiohttp.TCPConnector(limit=50, limit_per_host=10)
            
            reprocess_results = []
            
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                semaphore = asyncio.Semaphore(self.max_concurrent_tasks)
                
                async def process_chunk_with_semaphore(chunk):
                    async with semaphore:
                        table_config = table_configs[chunk["table"]]
                        return await self.reprocess_single_chunk(
                            session,
                            chunk,
                            table_config["SourceTable"],
                            table_config["WatermarkColumn"],
                        )
                
                tasks = [process_chunk_with_semaphore(chunk) for chunk in failed_chunks]
                reprocess_results = await asyncio.gather(*tasks, return_exceptions=True)

            print(f"[INFO] --> reprocess_results: {reprocess_results}")
            
            successful_chunks = sum(1 for r in reprocess_results if isinstance(r, dict) and r.get("success"))
            failed_chunks_count = len(failed_chunks) - successful_chunks
            total_records_processed = sum(
                r.get("records_processed", 0) 
                for r in reprocess_results 
                if isinstance(r, dict)
            )
            
            valid_results = [r for r in reprocess_results if isinstance(r, dict)]

            print(f"[INFO] --> valid_results: {valid_results}")

            if valid_results:
                self.meta_update_chunk_failures(valid_results)
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            summary = {
                "total_chunks": len(failed_chunks),
                "successful_chunks": successful_chunks,
                "failed_chunks": failed_chunks_count,
                "total_records_processed": total_records_processed,
                "execution_time_seconds": execution_time,
                "detailed_results": reprocess_results
            }
            
            print(f"\n" + "="*100)
            print(f"REPROCESSING COMPLETED")
            print("="*100)
            print(f"Execution time: {execution_time:.2f} seconds")
            print(f"Chunks - Successful: {successful_chunks}, Failed: {failed_chunks_count}")
            print(f"Records reprocessed: {total_records_processed:,}")
            
            return summary
            
        except Exception as e:
            print(f"[ERROR] --> Error during reprocessing: {str(e)}")
            raise