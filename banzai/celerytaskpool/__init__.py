from celery.concurrency.solo import TaskPool


class SingleShot(TaskPool):

    def on_apply(self, *args, **kwargs):
        r = super().on_apply(*args, **kwargs)

        self.terminate()
