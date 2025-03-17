broker_url = 'pyamqp://guest@localhost//'
imports = ('banzai.main', 'banzai.celery',)
worker_prefetch_multiplier = 1
worker_max_tasks_per_child = 100
task_acks_late = True
visibility_timeout = 86400
task_reject_on_worker_lost = True
broker_connection_retry_on_startup = True
