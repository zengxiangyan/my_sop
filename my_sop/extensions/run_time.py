import time

class RunTime:
    def __init__(self):
        self.summary = {}
        self.start_time = time.time()

    def add(self, k):
        if k not in self.summary:
            self.summary[k] = 0
        end_time = time.time()
        self.summary[k] += (end_time - self.start_time)
        self.start_time = end_time

    def print(self):
        for k in self.summary:
            print('{} used: {:.4f}'.format(k, self.summary[k]))