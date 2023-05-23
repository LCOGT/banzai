from celery.concurrency.solo import TaskPool
from celery.exceptions import WorkerTerminate


class SingleShot(TaskPool):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        f = super().on_apply

        def on_apply(*nargs, **nkwargs):
            f(*nargs, **nkwargs)
            raise WorkerTerminate()

        self.on_apply = on_apply
