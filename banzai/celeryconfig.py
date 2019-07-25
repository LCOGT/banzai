broker_url = 'redis://localhost:6379/0'
imports = ('banzai.main', 'banzai.celery',)
worker_prefetch_multiplier = 1
worker_max_tasks_per_child = 100
task_serializer = 'pickle'
