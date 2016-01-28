import sched, time
from threading import Thread


class Scheduler(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.sched = sched.scheduler(time.time, time.sleep)
        self.stopping = False

    def cancel(self, event):
        return self.sched.cancel(event)

    def enter(self, *args):
        return self.sched.enter(*args)

    def run(self):
        while not self.stopping:
            if self.sched.empty():
                time.sleep(.1)
            else:
                self.sched.run()

    def stop(self):
        self.stopping = True
        for event in self.sched.queue:
            self.sched.cancel(event)
