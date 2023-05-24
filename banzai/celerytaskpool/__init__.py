from celery.concurrency.solo import TaskPool
from celery.worker import state


class SingleShot(TaskPool):

    def apply_async(self, *args, **kwargs):
        r = super().apply_async(*args, **kwargs)
        state.should_stop = 0

        return r
