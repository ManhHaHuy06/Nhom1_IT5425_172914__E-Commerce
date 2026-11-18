"""Virtual clock: virtual_now = v0 + speed * (wall_now - wall0)."""
import time
from datetime import datetime, timedelta


class VirtualClock:
    def __init__(self, v0: datetime, speed: float):
        self.v0, self.speed, self.wall0 = v0, speed, time.time()

    def now(self) -> datetime:
        return self.v0 + timedelta(seconds=(time.time() - self.wall0) * self.speed)
