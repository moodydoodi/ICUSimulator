"""SimClock — decouples simulated time from wall clock.

speed=1.0 : real-time replay
speed=2.0 : 2× faster (useful for quickly stepping through a long stay)
speed=0.5 : half-speed (useful for inspecting a fast rhythm in detail)
"""

import time


class SimClock:
    def __init__(self, speed: float = 1.0):
        self.speed = speed
        self._sim_time: float = 0.0
        self._last_wall: float = time.monotonic()

    def tick(self) -> float:
        """Advance and return current simulated time in seconds."""
        now = time.monotonic()
        self._sim_time += (now - self._last_wall) * self.speed
        self._last_wall = now
        return self._sim_time

    def reset(self):
        self._sim_time = 0.0
        self._last_wall = time.monotonic()

    @property
    def sim_time(self) -> float:
        return self._sim_time
