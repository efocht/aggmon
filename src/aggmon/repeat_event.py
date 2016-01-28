class RepeatEvent(object):
    def __init__(self, scheduler, _interval, _function, *args, **kwargs):
        self.scheduler  = scheduler
        self.event      = None
        self.interval   = _interval
        self.function   = _function
        self.args       = args
        self.kwargs     = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self._function(*self.args, **self.kwargs)
        self.start()

    def start(self):
        if not self.is_running:
            self._event = self.scheduler.enter(self.interval, 1, self._run, ())
            self.is_running = True

    def stop(self):
        self.scheduler.cancel(self._event)
