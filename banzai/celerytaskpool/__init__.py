from celery.concurrency.solo import TaskPool
from celery.exceptions import WorkerTerminate


class SingleShot(TaskPool):

    def on_apply(self, *args, **kwargs):
        super().on_apply(*args, **kwargs)
        raise WorkerTerminate()
