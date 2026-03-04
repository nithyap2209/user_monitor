"""Celery tasks for background sync jobs."""

from app.extensions import celery
from app.services.sync_engine import sync_page as _sync_page, sync_all_pages as _sync_all


@celery.task(bind=True, max_retries=3, default_retry_delay=60)
def sync_page(self, page_id):
    """Sync a single connected page in the background."""
    try:
        result = _sync_page(page_id)
        return result
    except Exception as exc:
        self.retry(exc=exc)


@celery.task(bind=True)
def sync_company_pages(self, company_id):
    """Sync all active pages for a company."""
    results = _sync_all(company_id)
    return {"company_id": company_id, "results": results}


@celery.task
def scheduled_sync_all():
    """Periodic task: sync all active pages across all companies.

    Register in Celery beat schedule:
        celery.conf.beat_schedule = {
            'sync-every-30-min': {
                'task': 'app.tasks.sync_tasks.scheduled_sync_all',
                'schedule': 1800.0,
            },
        }
    """
    from app.models.company import Company

    companies = Company.query.filter_by(is_active=True).all()
    for company in companies:
        sync_company_pages.delay(company.id)
