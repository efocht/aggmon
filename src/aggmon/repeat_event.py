import logging


log = logging.getLogger( __name__ )


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

    def __repr__(self):
        return "RepeatEvent(interval=%r, %s, args=%r, kwargs=%r)" % (self.interval, self.function.__name__, self.args, self.kwargs)

    def _run(self):
        self.is_running = False
        log.debug("_run %r(%r, %r)" % (self.function, self.args, self.kwargs))
        res = self.function(*self.args, **self.kwargs)
        log.debug("res = %r" % res)
        self.start()

    def start(self):
        if not self.is_running:
            self.event = self.scheduler.enter(self.interval, 1, self._run, ())
            self.is_running = True

    def stop(self):
        self.scheduler.cancel(self.event)
