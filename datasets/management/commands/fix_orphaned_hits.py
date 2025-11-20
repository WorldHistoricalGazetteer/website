from django.core.management.base import BaseCommand
from django.db.models import Q, Count
from datasets.models import Hit
from places.models import Place
from django_celery_results.models import TaskResult


class Command(BaseCommand):
    help = 'Fix orphaned unreviewed hits for places that are marked as reviewed'

    def add_arguments(self, parser):
        parser.add_argument(
            '--task-id',
            type=str,
            help='Fix only a specific task ID',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be fixed without actually fixing it',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        task_id = options.get('task_id')

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN MODE - No changes will be made'))

        # Build task filter
        if task_id:
            tasks = TaskResult.objects.filter(task_id=task_id)
            if not tasks.exists():
                self.stdout.write(self.style.ERROR(f'Task {task_id} not found'))
                return
        else:
            # Get all reconciliation tasks
            tasks = TaskResult.objects.filter(
                task_name__startswith='align_'
            ).exclude(
                status__in=['FAILURE', 'ARCHIVED']
            )

        self.stdout.write(f'Checking {tasks.count()} tasks...\n')

        total_places_fixed = 0
        total_hits_fixed = 0

        for task in tasks:
            tid = task.task_id
            auth = task.task_name[6:].replace('local', '')

            # Determine review field
            if auth in ['whg', 'idx']:
                review_field = 'review_whg'
            elif auth in ['wd', 'wdlocal']:
                review_field = 'review_wd'
            else:
                review_field = 'review_tgn'

            # Find places marked as reviewed (=1) but with unreviewed hits
            bad_places = Place.objects.filter(
                hit__task_id=tid,
                hit__reviewed=False,
                **{review_field: 1}
            ).distinct()

            if not bad_places.exists():
                continue

            self.stdout.write(
                self.style.WARNING(
                    f'\nTask {tid} ({task.task_name}):'
                )
            )
            self.stdout.write(
                f'  Found {bad_places.count()} places with orphaned hits'
            )

            places_fixed = 0
            hits_fixed = 0

            for place in bad_places:
                # Count unreviewed hits for this place
                unreviewed_count = Hit.objects.filter(
                    task_id=tid,
                    place_id=place.id,
                    reviewed=False
                ).count()

                if unreviewed_count > 0:
                    self.stdout.write(
                        f'    Place {place.id} ({place.title}): '
                        f'{unreviewed_count} orphaned hits'
                    )

                    if not dry_run:
                        # Mark hits as reviewed
                        updated = Hit.objects.filter(
                            task_id=tid,
                            place_id=place.id,
                            reviewed=False
                        ).update(reviewed=True)

                        hits_fixed += updated
                        places_fixed += 1
                    else:
                        hits_fixed += unreviewed_count
                        places_fixed += 1

            if places_fixed > 0:
                total_places_fixed += places_fixed
                total_hits_fixed += hits_fixed

                if not dry_run:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f'  ✓ Fixed {places_fixed} places, '
                            f'{hits_fixed} hits marked as reviewed'
                        )
                    )

        # Summary
        self.stdout.write('\n' + '=' * 60)
        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    f'DRY RUN: Would fix {total_places_fixed} places '
                    f'with {total_hits_fixed} orphaned hits'
                )
            )
            self.stdout.write('Run without --dry-run to apply fixes')
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f'✓ COMPLETE: Fixed {total_places_fixed} places, '
                    f'{total_hits_fixed} hits marked as reviewed'
                )
            )