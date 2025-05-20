
import logging
import time
from datetime import datetime, timedelta
from collections import defaultdict

logger = logging.getLogger(__name__)

class RateLimitMonitor:
    def __init__(self, max_retries=3, initial_backoff=2):
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.request_counts = defaultdict(int)
        self.last_reset = datetime.now()
        self.error_counts = defaultdict(int)
        self.reset_interval = timedelta(minutes=15)

    def reset_counts(self):
        if datetime.now() - self.last_reset > self.reset_interval:
            self.request_counts.clear()
            self.error_counts.clear()
            self.last_reset = datetime.now()

    def calculate_backoff(self, attempt, endpoint):
        """Calculate backoff time with jitter"""
        base_delay = min(300, self.initial_backoff * (2 ** attempt))  # Cap at 5 minutes
        jitter = base_delay * 0.1  # 10% jitter
        return base_delay + ((-jitter/2) + (time.time() % jitter))

    def should_retry(self, endpoint, status_code):
        self.reset_counts()
        
        # Increment error count
        if status_code in (429, 403):
            self.error_counts[endpoint] += 1
        
        # Check if we should stop retrying
        if self.error_counts[endpoint] > self.max_retries:
            logger.error(f"Maximum retries exceeded for endpoint {endpoint}")
            return False
            
        return True

    def wait_before_retry(self, endpoint, attempt):
        backoff = self.calculate_backoff(attempt, endpoint)
        logger.warning(f"Rate limit hit for {endpoint}. Waiting {backoff:.2f}s before retry (attempt {attempt + 1}/{self.max_retries})")
        time.sleep(backoff)
