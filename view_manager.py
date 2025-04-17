import logging
import time
from typing import List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class ViewManager:
    def __init__(self, rotation_interval: int = 10):
        self.rotation_interval = rotation_interval
        self._active_brokers: List[int] = []
        self._last_rotation = time.time()
        self._current_index = -1  # -1 represents the ranking page

    def set_active_brokers(self, broker_ids: List[int]):
        self._active_brokers = broker_ids

    def get_active_brokers(self) -> List[int]:
        return self._active_brokers.copy()

    def should_rotate(self) -> bool:
        current_time = time.time()
        if current_time - self._last_rotation >= self.rotation_interval:
            self._last_rotation = current_time
            return True
        return False

    def get_next_page(self) -> str:
        if not self._active_brokers:
            return "ranking"

        self._current_index += 1
        if self._current_index >= len(self._active_brokers):
            self._current_index = -1
            return "ranking"

        return f"broker/{self._active_brokers[self._current_index]}"

    def rotate_if_needed(self) -> Optional[str]:
        if not self.should_rotate():
            return None

        return self.get_next_page()