"""Celery entry point for the editors' sample check (``datasets/qc_sample.py``).

Queued on ``recon`` (the queue that carries the other paced gateway work); the worker listens
with ``-Q celery,recon`` (``entrypoints/entrypoint-celery.sh``).
"""
from celery import shared_task


@shared_task(name="datasets.qc_gateway_sample", acks_late=True)
def qc_gateway_sample(event_pk, user_pk):
    from datasets import qc_sample
    return qc_sample.take_and_store(event_pk, user_pk)
