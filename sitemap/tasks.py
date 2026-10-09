import logging
from celery import shared_task
from django.db import transaction
from django.core.paginator import Paginator
from sitemap.models import Toponym
from places.models import Place
from utils.whens import yearspan, merge_yearspans

logger = logging.getLogger(__name__)


@shared_task
def populate_toponyms():
    """
    Populate the Toponym table with data from the Place table
    """
    Toponym.objects.all().delete()
    logger.info("Emptied the Toponym table.")

    total_fetched = 0
    page_size = 1000

    try:
        # Only places anyone may see (place#310): the table feeds sitemap.xml and the
        # public /search/<toponym> page, so a non-public or embargoed dataset's names
        # must never enter it.
        from django.contrib.auth.models import AnonymousUser
        from api.dataset_access import visible_places_q
        places = (Place.objects.filter(visible_places_q(AnonymousUser()))
                  .prefetch_related('names', 'whens').order_by('id'))
        paginator = Paginator(places, page_size)

        for page_num in paginator.page_range:
            logger.info(f"Processing page {page_num}")
            page = paginator.page(page_num)

            for place in page.object_list:
                ccodes = [code for code in (place.ccodes or []) if code]  # Remove empty strings

                yearspans = []
                for pw in place.whens.all():
                    data = pw.jsonb
                    if data and 'timespans' in data:
                        for timespan in data['timespans']:
                            extracted_yearspan = yearspan(timespan)
                            if extracted_yearspan:
                                yearspans.append(extracted_yearspan)

                toponyms = [name_instance.toponym for name_instance in place.names.all()]
                for toponym in toponyms:
                    if toponym:
                        # Call update_or_create_toponym for each related toponym
                        update_or_create_toponym(toponym, ccodes, yearspans)
                        total_fetched += 1

            logger.info(f"Finished processing page {page_num}.")

        logger.info(f"Finished processing {total_fetched} toponyms from the Place table.")

    except Exception as e:
        logger.error(f"Error fetching data from Place table: {e}")
        raise

    return f"Processed {total_fetched} toponyms."


@transaction.atomic
def update_or_create_toponym(name, ccodes, yearspans):
    """
    Update or create a toponym in the Toponym table.
    If it exists, extend ccodes and yearspans, and increment instance_count.
    """
    toponym, created = Toponym.objects.get_or_create(
        name=name,
        defaults={'ccodes': ccodes, 'yearspans': yearspans}
    )

    if not created:
        # If it exists, extend ccodes and timespans and increment instance_count
        toponym.ccodes = list(set(toponym.ccodes + ccodes))
        toponym.yearspans = merge_yearspans(toponym.yearspans + yearspans)
        toponym.instance_count += 1
        toponym.save()
        logger.debug(f"Updated existing toponym: {name}.")
    else:
        logger.debug(f"Created new toponym: {name}.")
