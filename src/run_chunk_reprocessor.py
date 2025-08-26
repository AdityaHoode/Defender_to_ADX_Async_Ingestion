import os
import pprint
from dotenv import load_dotenv

import asyncio

from core.chunk_reprocessor import Reprocessor

load_dotenv()

bootstrap = {
    "adx_cluster_uri": "https://kvc-t4efc4mq1p8d6sdfm5.southcentralus.kusto.windows.net",
    "adx_ingest_uri": "https://ingest-kvc-t4efc4mq1p8d6sdfm5.southcentralus.kusto.windows.net",
    "adx_database": "db1",
    "defender_resource_uri":"https://api.security.microsoft.com",
    "defender_hunting_api_url": "https://api.security.microsoft.com/api/advancedhunting/run",
    "config_table": "meta_MigrationConfiguration",
    "audit_table": "meta_MigrationAudit",
    "chunk_audit_table": "meta_ChunkIngestionFailures",
    "clientId": os.getenv("AZURE_CLIENT_ID"),
    "clientSecret": os.getenv("AZURE_CLIENT_SECRET"),
    "tenantId": os.getenv("AZURE_TENANT_ID"),
}

async def main():
        
    reprocess_handler = Reprocessor(
        bootstrap=bootstrap,
        max_concurrent_tasks=5,
        chunk_size=25000
    )

    try:
        summary = await reprocess_handler.reprocess_failed_chunks()
        pprint.pprint(summary)
    except Exception as e:
        print(f"[ERROR] --> Exception during ingestion: {e}")
    finally:
        reprocess_handler.thread_pool.shutdown(wait=True)

if __name__ == "__main__":
    asyncio.run(main())
