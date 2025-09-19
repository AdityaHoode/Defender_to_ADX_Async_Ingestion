# Export MS Defender TVM to ADX

This migration/export is the process of extracting Microsoft Defender data (via Advanced Hunting API) and loading it into Microsfot Fabric Real-Time Intellingence Eventhouse or Azure Data Explorer (ADX) tables for analytics, retention, and integration with other security data.

The script is designed to facilitate data migration and error recovery. The script performs two main operations:

1. **Reprocessing of failed ingestion chunks**
2. **Main ingestion of data based on configuration stored in ADX**

The script is built using Python with Azure SDKs and asynchronous/parallel programming to efficiently handle large datasets. The script is run on Azure Functions.

---

## 🪪 Step 1
### Access to ADX for service principal

To grant Azure Data Explorer (ADX) access to a service principal, you need to assign the service principal appropriate security roles.

These roles determine what actions the service principal can perform within the ADX cluster and its databases.

We can provide **admins**, **viewers**, **ingestors** and/or **users** roles in the ADX DB as needed.

```
.add database DBName viewers ('aadapp=clientId;tenantId') //required for app to read config table & audit log table, can be granular per table.

.add database DBName ingestors ('aadapp=clientId;tenantId') //required for app to ingest to audit log and tables., can be granular per table if desired.

.add database DBName users ('aadapp=clientId;tenantId') //not required if ingestor & viewer already granted.
```

Optionally, we can add AAD users as following:

`.add database DBName viewers ('aaduser=xxxxx@domain.com;tenantId') 'xxxxx@domain.com'`


## 💡 Step 2
### Providing access to service principal for defender API

Granting service principal access to Microsoft Defender APIs.

To automate interactions with Microsoft Defender APIs (e.g., Microsoft Defender for Endpoint, Microsoft Defender for Cloud Apps, or Microsoft Defender XDR), you need to grant a Service Principal in Azure Active Directory (Azure AD) the necessary permissions. This involves registering an application, assigning the appropriate API permissions, and granting admin consent. 

Please follow these steps:
1. Register an application in Azure AD
   - Sign in to the Azure portal.
   - Navigate to Azure Active Directory > App registrations > New registration.
   - Choose a descriptive name for your application (e.g., "DefenderAPIIntegration") and select Register. 

2. Grant API permissions.
   - On your newly created application's page, select API permissions > Add a permission.
   - Select the relevant Defender API based on your needs (e.g., WindowsDefenderATP for Microsoft Defender for Endpoint, Microsoft Cloud App Security for Microsoft Defender for Cloud Apps, or Microsoft Threat Protection for Microsoft Defender XDR).
   - Choose Delegated & Application permissions to grant read access to TVM tables in Defender XDR, as the permission types (as this export process intends to run as a background service or daemon without a signed-in user).
   - Select the specific permissions required by your application. Examples include:**
     - Alert.Read.All: Read all alerts.
     - Machine.Read.All: Read all machine information.
     - Software.Read.All: Read all software information.
     - User.Read.All: Read user information.
     - Vulnerability.Read.All: Read all vulnerability information.
     - Incident.Read.All: Read Incident related information
     - CustomDetections.ReadWrite.All: allows an application to read and write custom detection rules on behalf of the signed-in user
     - AdvancedHunting.Read.All: allows applications to access advanced hunting data in Microsoft Defender for Endpoint
   - Click Add permissions.

3. Grant admin consent.
   - After adding the necessary permissions, select Grant admin consent for to grant consent for your organization.
   - Confirm the action by clicking Yes. 

4. Obtain application credentials.
   - On your application's overview page, locate and copy the Application (client) ID and the Directory (tenant) ID.
   - Go to Certificates & secrets and create a New client secret.
   - Provide a description for the secret and specify an expiry period.
   - Important: Copy the generated client secret value immediately, as it will only be displayed once.

5. Using the credentials in your application.
   - In your application, you will use the Application (client) ID, Directory (tenant) ID, and the client secret to authenticate with Azure AD and obtain an access token.
   - This access token can then be used in the Authorization header (as "Bearer {token}") when making requests to the Defender APIs. 
  
By following steps above, you have successfully provided your app registration with the necessary access to interact with Microsoft Defender APIs. 

You can find more specific examples for different Defender APIs and permission scopes in the official Microsoft documentation. 

---

## 📋 Step 3
### Setup the environment for the script 

### 1️⃣ Install Requirements

- Python 3.10
- Install dependencies:
```bash
pip install -r requirements.txt
```

---

### 2️⃣ Set Environment Variables

Create a `.env` file at the root of the project with the following keys:

```bash
AZURE_CLIENT_ID=<your-client-id>
AZURE_CLIENT_SECRET=<your-client-secret>
AZURE_TENANT_ID=<your-tenant-id>
```

---

### 3️⃣ Setup Metadata Tables and Functions in ADX
Execute the script in kql/metadata_stores.kql on your desired ADX database

- meta_MigrationConfiguration
<img width="940" height="346" alt="image" src="https://github.com/AdityaHoode/FnApp_Defender_to_ADX_Async_Ingestion/blob/main/assets/meta_MigrationConfiguration.png?raw=true" />

- meta_MigrationAudit
<img width="940" height="346" alt="image" src="https://github.com/AdityaHoode/FnApp_Defender_to_ADX_Async_Ingestion/blob/main/assets/meta_MigrationAudit.png?raw=true" />

- vw_meta_LatestMigrationConfiguration
<img width="940" height="346" alt="image" src="https://github.com/AdityaHoode/FnApp_Defender_to_ADX_Async_Ingestion/blob/main/assets/vw_meta_LatestMigrationConfiguration.png?raw=true" />


---

### 4️⃣ Define Bootstrap Configuration
A dictionary containing ADX cluster and ingestion URIs, database and table names, Defender API URLs, concurrency and chunk settings, and Azure authentication credentials loaded from environment variables.
- **adx_cluster_uri**: The URI of the Azure Data Explorer (ADX) cluster.
- **adx_ingest_uri**: The ingestion endpoint URI for ADX.
- **adx_database**: The name of the ADX database used for migration.
- **defender_resource_uri**: The base URI for Microsoft Defender API.
- **defender_hunting_api_url**: The endpoint URL for Microsoft Defender’s advanced hunting API.
- **config_table**: The name of the table storing migration configuration data.
- **config_view**: The view used to fetch the latest migration configuration.
- **audit_table**: The table that logs migration audit information.
- **chunk_audit_table**: The table that tracks failed chunk ingestion attempts.
- **chunk_audit_view**: The view that provides the latest chunk ingestion failures.
- **max_concurrent_tasks**: The maximum number of concurrent tasks allowed during processing.
- **max_thread_workers**: The maximum number of threads in the pool for parallel processing.
- **chunk_size**: The size of data chunks to process in each batch.
- **clientId**: The Azure service principal’s client ID, loaded from environment variables.
- **clientSecret**: The Azure service principal’s client secret, loaded from environment variables.
- **tenantId**: The Azure tenant ID, loaded from environment variables.

---

## 🏃‍♂️ Step 5
### Run the script locally as a Function App

### 1️⃣ Install Dependencies

1. Install **Azure Functions Core Tools**.  
2. Install the **Azurite** extension in Visual Studio Code.

---

### 2️⃣ Start the Emulator

1. In Visual Studio Code, press `F1` to open the Command Palette.  
2. Search for and select **Azurite: Start** to launch the storage emulator.  
3. Check the bottom bar in VS Code to verify that Azurite emulation services are running. Once running, you can proceed to run your function locally.

---

### 3️⃣ Run the Function Locally

1. Press `F5` or click the **Run and Debug** icon in the Activity bar on the left.  
2. The Terminal panel will display the output from Azure Functions Core Tools.  
3. Your function app starts locally, and you can see the URL endpoint of your HTTP-triggered function running.
4. Start the Orchestration
   - Send a GET request to the **adxingestor** URL to start the orchestration.
5. Check Status or Terminate the Orchestration
   - To get the status of the orchestration run, send a GET request to the get_status URL. Replace {instanceId} with the actual orchestration ID.
   - To terminate the orchestration run, send a POST request to the terminate_orchestration URL, using the same {instanceId} replacement.
6. With the Terminal panel focused, press Ctrl + C to stop Core Tools and disconnect the debugger.

---

# End-to-End Workflow

1. **Start Ingestion Script**
2. **Load Bootstrap Configuration**
3. **Create ADX Ingest & Data Clients**
4. **Check & Create Tables if Missing**
5. **Start Async Session for API Calls**
6. **For Each Table → Process Table**
   - Check if High Watermark Exists
     - ✅ **No** → Create Table & Mapping
6. **Build Base KQL Query**
7. **Calculate Chunks**
8. Check if Records are Found
   - ✅ **No** → Mark Table as Processed (0 records)
   - ✅ **Yes** → Check if Chunking is Required
     - ✅ **No** → Process Entire Table in Single Chunk
     - ✅ **Yes** → Build Chunked Queries → Process Chunks in Parallel
9. **Ingest Data to ADX with Retry Logic for Failures & Timeouts**
11. **Collect Chunk Results**
12. **Update Config, Audit & Chunk Failure Tables**
13. **Analyze All Results**
14. **Display Summary & Errors**
15. **End of Script**
 
## 🪜 FlowChart

[![](https://mermaid.ink/img/pako:eNp1VNty2jAQ_ZUddSZPkEIwDXg66YCBhAAJtyRtDQ-KLYMmwqKSPIlDeO0H9BP7JZVlG5zOxA8G7Z49e3a16x3yuE-QjQLGn701FgrmnUUI-mm5M5Wc--GKSEV5CDNP0K1aQrl8AW13yLEPbc6VVAJvweFhQFeRwAl0mVK0DdRxHUGwItDqfM_Y4AQ6WGFwGCWhkhncMfCO66yJ96QhWdgcPzIigQYwolLScJXBOwbezWS2ZBx6MCMaoaUGXEBr3AcHM5bTdw2-5_a0r4u9dUoMf3__gbHgno5MLRk8ffdM0OUu1aQ1XNHVGh60LrHB4gm6L1QquU_Blwn47Ya_wZVbFK9rGeHt9ig9Bf4g8g36bjuiTHcSSwKDyRAmERFxhrsy2ftFQX1junZ1ZV7EkhTOOgqf8iKvjXtw1DslHhe-BCwI9HgU-vsi2-CgeOiOknpSvVjmLSH-10fx-aICIuVZFuNMAaNjLqNEVwlU6ry_IirI-3SjQ7obN-95N1QalyY2uaieNE3CssqWxVCT8TZrmXET33SMklzarenA-MCftgc06xgLPQ6EvbvgGwOfuNlgmrFU3MzqM1Vro2hKlIhhyFfUM5PVw5RFQs_kCczphvDoMMLjlK2YYGJMU9fhOrenUj26PTJih7BhiimGTY1p5t5tfXPJZr1K0Ip8qtfHyEqZMjHZmmSEMxM93434wZNUlfXk27tLmR872ytakmu6c1shZvGrXl7G_hOdvu9Mpnu3Q-WW4Rhm0UYvRqxb0xWCixx8b2APbjf0gQf5p6RIJFWsr7wFAWXM_kSqQT0gRc9D7mkEddIoei4zTxAENVIpegYfekYfeuYfeia5xyMW8VAJrQT1ka1EREpok3wQkiPaJTELpNZkQxbI1n99vVkLtAj3OmaLw5-cb_IwwaPVGtkBZlKfInPZHYpXAm8OVkFCnwhHL69C9tlZ3ZAge4dekF2unFaSp2lVG5Vmw7JqNetc_5RQjOzaqWU1z2sNq2o1z5qVav0LKZ_vS-jVCAgjxvb_AIZl20Q?type=png)](https://mermaid.live/edit#pako:eNp1VNty2jAQ_ZUddSZPkEIwDXg66YCBhAAJtyRtDQ-KLYMmwqKSPIlDeO0H9BP7JZVlG5zOxA8G7Z49e3a16x3yuE-QjQLGn701FgrmnUUI-mm5M5Wc--GKSEV5CDNP0K1aQrl8AW13yLEPbc6VVAJvweFhQFeRwAl0mVK0DdRxHUGwItDqfM_Y4AQ6WGFwGCWhkhncMfCO66yJ96QhWdgcPzIigQYwolLScJXBOwbezWS2ZBx6MCMaoaUGXEBr3AcHM5bTdw2-5_a0r4u9dUoMf3__gbHgno5MLRk8ffdM0OUu1aQ1XNHVGh60LrHB4gm6L1QquU_Blwn47Ya_wZVbFK9rGeHt9ig9Bf4g8g36bjuiTHcSSwKDyRAmERFxhrsy2ftFQX1junZ1ZV7EkhTOOgqf8iKvjXtw1DslHhe-BCwI9HgU-vsi2-CgeOiOknpSvVjmLSH-10fx-aICIuVZFuNMAaNjLqNEVwlU6ry_IirI-3SjQ7obN-95N1QalyY2uaieNE3CssqWxVCT8TZrmXET33SMklzarenA-MCftgc06xgLPQ6EvbvgGwOfuNlgmrFU3MzqM1Vro2hKlIhhyFfUM5PVw5RFQs_kCczphvDoMMLjlK2YYGJMU9fhOrenUj26PTJih7BhiimGTY1p5t5tfXPJZr1K0Ip8qtfHyEqZMjHZmmSEMxM93434wZNUlfXk27tLmR872ytakmu6c1shZvGrXl7G_hOdvu9Mpnu3Q-WW4Rhm0UYvRqxb0xWCixx8b2APbjf0gQf5p6RIJFWsr7wFAWXM_kSqQT0gRc9D7mkEddIoei4zTxAENVIpegYfekYfeuYfeia5xyMW8VAJrQT1ka1EREpok3wQkiPaJTELpNZkQxbI1n99vVkLtAj3OmaLw5-cb_IwwaPVGtkBZlKfInPZHYpXAm8OVkFCnwhHL69C9tlZ3ZAge4dekF2unFaSp2lVG5Vmw7JqNetc_5RQjOzaqWU1z2sNq2o1z5qVav0LKZ_vS-jVCAgjxvb_AIZl20Q)
