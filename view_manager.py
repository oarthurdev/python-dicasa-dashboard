
import logging
import time
import pandas as pd
from typing import List, Optional

logger = logging.getLogger(__name__)

class ViewManager:
    def __init__(self, rotation_interval: int = 10):
        self.rotation_interval = rotation_interval
        self._active_brokers: List[int] = []
        self._last_rotation = time.time()
        self._current_index = -1

    def set_active_brokers(self, broker_ids: List[int]):
        """Set active brokers in order of their appearance in broker_points"""
        self._active_brokers = sorted(broker_ids, key=lambda x: broker_ids.index(x))

    def get_active_brokers(self) -> List[int]:
        return self._active_brokers.copy()

    def get_next_page(self) -> str:
        """Get next page in rotation sequence"""
        if not self._active_brokers:
            return "ranking"

        self._current_index += 1
        if self._current_index >= len(self._active_brokers):
            self._current_index = -1
            return "ranking"
            
        current_broker = self._active_brokers[self._current_index]
        return f"broker/{current_broker}"
