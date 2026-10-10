import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    source_url: str = os.getenv("DATA_SOURCE_URL", "http://host.docker.internal:8000").rstrip("/")
    raw_dir: str = os.getenv("RAW_DIR", "raw/olist")
    state_file: str = os.getenv("STATE_FILE", "raw/_metadata/crawler_state.json")
    manifest_dir: str = os.getenv("MANIFEST_DIR", "raw/_metadata/manifests")
    start_date: str = os.getenv("START_DATE", "2016-09-04")
    end_date: str = os.getenv("END_DATE", "2018-10-17")
    review_start_date: str = os.getenv("REVIEW_START_DATE", "2016-09-04")
    review_end_date: str = os.getenv("REVIEW_END_DATE", "2018-10-29")
    crawl_unanswered_reviews: bool = os.getenv("CRAWL_UNANSWERED_REVIEWS", "true").lower() == "true"
    bootstrap_master: bool = os.getenv("BOOTSTRAP_MASTER", "true").lower() == "true"
    run_transactions: bool = os.getenv("RUN_TRANSACTIONS", "true").lower() == "true"
    run_master: bool = os.getenv("RUN_MASTER", "true").lower() == "true"
    max_retries: int = int(os.getenv("MAX_RETRIES", "3"))
    timeout: int = int(os.getenv("HTTP_TIMEOUT", "120"))
    retry_backoff: float = float(os.getenv("RETRY_BACKOFF", "2"))
    page_size: int = int(os.getenv("MASTER_PAGE_SIZE", "50000"))
    verify_order_count: bool = os.getenv("VERIFY_ORDER_COUNT", "true").lower() == "true"

SETTINGS = Settings()
