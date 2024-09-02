import json
import time

class TimeLog():
    def __init__(self):
        self.h_time = {}
        self.start_time = time.time()

    def add(self, k):
        end_time = time.time()
        delta = end_time - self.start_time
        self.start_time = end_time
        if k not in self.h_time:
            self.h_time[k] = 0
        self.h_time[k] += delta

    def __str__(self):
        return json.dumps(self.h_time)
