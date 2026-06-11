#                                               IMPORTS
import logging
import os
from datetime import datetime
import boto3
import requests
import json
import time


#                                               CONFIG

BASE_URL    = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
BATCH_SIZE  = 1000
MAX_RECORDS = 10000
MAX_RETRIES = 3
RETRY_DELAY = 5

# S3 (config-driven — set these in .env)
S3_BUCKET   = os.environ.get("S3_BUCKET", "")
S3_PREFIX   = os.environ.get("S3_PREFIX", "source")

if not S3_BUCKET:
    raise RuntimeError(
        "S3_BUCKET is not set. Copy .env.example to .env and set S3_BUCKET to your bucket name."
    )




#                                               LOGGING SETUP

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_filename = f"{LOG_DIR}/nyc311_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger=logging.getLogger(__name__)





#                                               FETCH SINGLE BATCH

def fetch_batch(session: requests.Session, offset: int) -> list:
    
    params = {
        "$limit": BATCH_SIZE,
        "$offset": offset,
        "$order": "created_date DESC"
    }
    
    for attempt in range(1, MAX_RETRIES +1):
        try:
            logger.info(f"[STARTING] Fetching batch... | offset = {offset}")
            response = session.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            records = response.json()
            logger.info(f"[SUCCESS] Fetched {len(records)} records from batch in JSON | offset = {offset}")
            return records
        
        except requests.exceptions.Timeout:
            logger.warning(f"[TIMEOUT] attempt={attempt}/{MAX_RETRIES}")

        except requests.exceptions.ConnectionError:
            logger.warning(f"[CONNECTION FAILED] attempt={attempt}/{MAX_RETRIES} | offset={offset}")

        except requests.exceptions.HTTPError as e:
            logger.error(f"[HTTP ERROR] {response.status_code} | offset={offset} | detail={e}")
            raise    

        except json.JSONDecodeError:
            logger.error(f"[INVALID JSON] offset={offset} | raw={response.text[:200]}")
            raise 

        if attempt < MAX_RETRIES:
            logger.info(f"[RETRYING] waiting {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)

    logger.error(f"[FAILED] All {MAX_RETRIES} attempts failed | offset = {offset}")
    raise Exception (f"Failed after {MAX_RETRIES} retries | offset = {offset}")






#                                                  DATA QUALITY

def check_data_quality(records:list, offset:int) -> dict:
    
    # ── STEP 1: define issues tracker ────────────────────
    
    issues = {
        "missing_unique_key":     0,
        "missing_created_date":   0,
        "missing_complaint_type": 0,
        "missing_borough":        0,
        "empty_strings":          0,
        "duplicate_keys":         set()
    }

    seen_keys = set()

    # ── STEP 2: loop through records ─────────────────────

    for record in records:
        unique_key=record.get("unique_key")

        if not unique_key:
            issues["missing_unique_key"] +=1
        
        else:
            if unique_key in seen_keys:
                issues["duplicate_keys"].add(unique_key)
            seen_keys.add(unique_key)

        if not record.get("created_date"):
            issues["missing_created_date"] += 1

        if not record.get("complaint_type"):
            issues["missing_complaint_type"] += 1
        
        if not record.get("borough"):
            issues["missing_borough"] += 1

        empty = sum(1 for v in record.values() if v == "")
        if empty > 0:
            issues["empty_strings"] += empty


    # ── STEP 3: log the results ──────────────────────────

    issues["duplicate_keys"] = len(issues["duplicate_keys"])
    logger.info(
        f"[QUALITY CHECK] offset={offset} | "
        f"missing_key={issues['missing_unique_key']} | "
        f"missing_date={issues['missing_created_date']} | "
        f"missing_borough={issues['missing_borough']} | "
        f"duplicates={issues['duplicate_keys']} | "
        f"empty_strings={issues['empty_strings']}"
    )

    if issues["duplicate_keys"] > 0:
        logger.warning(f"[DUPLICATES DETECTED] count={issues['duplicate_keys']} | offset={offset}")

    return issues





#                                                 SAVE BATCH TO S3

def save_to_s3(records:list, offset:int)->str:

    # ── STEP 1: build the s3 key (file path) ─────────────
   
    date_partition = datetime.now().strftime("%Y-%m-%d")
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    s3_key = f"{S3_PREFIX}/{date_partition}/batch{offset}_{timestamp}.json"


    # ── STEP 2: convert records to json string ────────────

    json_data = json.dumps(records, indent=3)

    # ── STEP 3: upload to s3 ─────────────────────────────
    
    try:
        s3_client = boto3.client("s3")
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json_data,
            ContentType="application/json"
        )
        logger.info(f"[S3 UPLOAD SUCCESS] s3://{S3_BUCKET}/{s3_key}")
        return s3_key

    except Exception as e:
        logger.error(f"[S3 UPLOAD FAILED] {e}")
        raise
    




#                                                ORCHESTRATOR

def run_extraction():

    # ── STEP 1: initialise tracking variables ─────────────
    
    start_time    = datetime.now()
    total_records = 0
    total_files   = []
    total_issues  = {}
    offset        = 0

    logger.info("=" * 60) #------------------------

    logger.info("NYC 311 Extraction Started")
    logger.info(f"Target records : {MAX_RECORDS}")
    logger.info(f"Batch size     : {BATCH_SIZE}")
    logger.info(f"S3 bucket      : {S3_BUCKET}")

    logger.info("=" * 60) #------------------------


    # ── STEP 2: main loop ─────────────────────────────────
    
    with requests.Session() as session:
        session.headers.update({"Accept": "application/json"})

        while total_records < MAX_RECORDS:
            try:
                records = fetch_batch(session, offset)

                if not records:
                    logger.info("[DONE] No more records available")
                    break

                issues = check_data_quality(records, offset)
                s3_key = save_to_s3(records, offset)

                for key, val in issues.items():
                    total_issues[key] = total_issues.get(key, 0) + val

                total_files.append(s3_key)
                total_records += len(records)
                offset        += BATCH_SIZE

                logger.info(f"[PROGRESS] total_records={total_records}/{MAX_RECORDS}")
                time.sleep(0.5)

            except KeyboardInterrupt:
                logger.warning("[INTERRUPTED] Pipeline stopped by user")
                break

            except Exception as e:
                logger.error(f"[ERROR] offset={offset} | error={e}")
                offset += BATCH_SIZE
                continue

    # ── STEP 3: final summary ─────────────────────────────

    duration = datetime.now() - start_time

    logger.info("=" * 60)
    logger.info("Extraction Complete")
    logger.info(f"Total records  : {total_records}")
    logger.info(f"Total files    : {len(total_files)}")
    logger.info(f"Duration       : {duration}")
    logger.info(f"Quality issues : {total_issues}")
    logger.info("=" * 60)

    return {
        "total_records": total_records,
        "files_saved":   total_files,
        "duration":      str(duration),
        "issues":        total_issues
    }





#                                                   ENTRY POINT

if __name__ == "__main__":
    try:
        run_extraction() 
    
    except Exception as e:
        logger.critical(f"[PIPELINE CRASHED] {e}")

    