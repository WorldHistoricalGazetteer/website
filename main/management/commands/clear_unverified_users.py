from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model
from django.db import connection
from django.db.models import Q

User = get_user_model()

def get_unverified_inactive_users():
    with connection.cursor() as cursor:
        cursor.execute("""
            WITH unverified_users AS (
                SELECT id, username, email_confirmed, last_login
                FROM auth_users
            ),
            user_references AS (
                SELECT id FROM unverified_users u
                WHERE
                    -- The user appears in *any* of these tables? Then exclude:
                    EXISTS (SELECT 1 FROM collection_user cu WHERE cu.user_id = u.id)
                    OR EXISTS (SELECT 1 FROM collection_group_user cgu WHERE cgu.user_id = u.id)
                    OR EXISTS (SELECT 1 FROM dataset_user du WHERE du.user_id_id = u.id)
                    OR EXISTS (SELECT 1 FROM collections co WHERE co.owner_id = u.id)
                    OR EXISTS (SELECT 1 FROM collection_group cg WHERE cg.owner_id = u.id)
                    OR EXISTS (SELECT 1 FROM datasets d WHERE d.owner_id = u.id)
                    
                    OR EXISTS (SELECT 1 FROM areas a WHERE a.owner_id = u.id)
                    OR EXISTS (SELECT 1 FROM sources s WHERE s.owner_id = u.id)
                    OR EXISTS (SELECT 1 FROM place_name pn WHERE pn.reviewer_id = u.id)
                    OR EXISTS (SELECT 1 FROM place_geom pg WHERE pg.reviewer_id = u.id)
                    OR EXISTS (SELECT 1 FROM place_link pl WHERE pl.reviewer_id = u.id)
                    OR EXISTS (SELECT 1 FROM resources r WHERE r.owner_id = u.id)
                    OR EXISTS (SELECT 1 FROM trace_annotations ta WHERE ta.owner_id = u.id)
                    OR EXISTS (SELECT 1 FROM django_admin_log dal WHERE dal.user_id = u.id)
                    OR EXISTS (SELECT 1 FROM guardian_userobjectpermission guop WHERE guop.user_id = u.id)
                    OR EXISTS (SELECT 1 FROM authtoken_token at WHERE at.user_id = u.id)
                    OR EXISTS (SELECT 1 FROM download_files df WHERE df.user_id = u.id)
                    OR EXISTS (SELECT 1 FROM log l WHERE l.user_id = u.id)
                    OR EXISTS (SELECT 1 FROM comments c WHERE c.user_id = u.id)
                    OR EXISTS (SELECT 1 FROM close_matches cm WHERE cm.created_by_id = u.id)
                    OR EXISTS (SELECT 1 FROM auth_users_groups aug WHERE aug.user_id = u.id)
                    OR EXISTS (SELECT 1 FROM auth_users_user_permissions auup WHERE auup.user_id = u.id)
            )
            SELECT
                u.id,
                u.username,
                u.email_confirmed,
                (u.last_login IS NOT NULL) AS ever_logged_in,
                FALSE AS has_contributions -- by definition here, filtered out
            FROM unverified_users u
            WHERE
                u.email_confirmed = FALSE
                AND u.last_login IS NULL
                AND u.id NOT IN (SELECT id FROM user_references)
            ORDER BY u.id;
        """)
        user_ids = [row[0] for row in cursor.fetchall()]

    return User.objects.filter(id__in=user_ids)

class Command(BaseCommand):
    help = 'Delete users who have never verified their email address, never logged in, and have no contributions'

    def handle(self, *args, **options):
        users_to_delete = get_unverified_inactive_users()
        count = users_to_delete.count()
        if count == 0:
            self.stdout.write("No matching unverified inactive users to delete.")
            return

        self.stdout.write(f"Deleting {count} users...")
        users_to_delete.delete()
        self.stdout.write("Done.")
