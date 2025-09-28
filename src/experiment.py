import random
import string
import time
from datetime import datetime, timedelta

def generate_experiment_id(prefix: str = "SYNC", length: int = 8) -> str:
    timestamp = int(time.time())
    random_str = "".join(random.choices(string.ascii_uppercase + string.digits, k=length))
    return f"{prefix}-{timestamp}-{random_str}"

def get_two_days_records() -> str:
    return (datetime.now() - timedelta(days=2)).date().isoformat()
