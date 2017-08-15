from threading import Timer
import logging


__all__ = ["RepeatTimer"]
log = logging.getLogger( __name__ )


class RepeatTimer(object):
    def __init__(self, _interval, _function, *args, **kwargs):
        self._timer     = None
        self.interval   = _interval
        self.function   = _function
        self.args       = args
        self.kwargs     = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        log.debug("timer triggered, function=%s, args=%r" % (self.function.__name__, self.args));
        res = self.function(*self.args, **self.kwargs)
        self.start()

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.daemon = True
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False
