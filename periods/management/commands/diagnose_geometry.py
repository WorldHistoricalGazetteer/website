"""
Django management command to diagnose why MultiPolygons are being saved as GeometryCollections.

Usage: python manage.py diagnose_geometry
"""

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management.base import BaseCommand
from django.db import connection


class Command(BaseCommand):
    help = "Diagnose geometry storage issues in SpatialEntity model"

    def handle(self, *args, **options):
        self.stdout.write("=" * 80)
        self.stdout.write("GEOMETRY STORAGE DIAGNOSTIC")
        self.stdout.write("=" * 80)

        from periods.models import SpatialEntity

        # 1. Check the field definition
        self.stdout.write("\n1. Checking SpatialEntity.geometry field type:")
        geometry_field = SpatialEntity._meta.get_field('geometry')
        self.stdout.write(f"   Field class: {geometry_field.__class__.__name__}")
        self.stdout.write(f"   Field type: {geometry_field.get_internal_type()}")
        self.stdout.write(f"   SRID: {geometry_field.srid}")

        # 2. Check the actual PostGIS column type
        self.stdout.write("\n2. Checking PostGIS column definition:")
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    column_name,
                    udt_name,
                    data_type
                FROM information_schema.columns 
                WHERE table_name = 'periods_spatialentity' 
                AND column_name = 'geometry';
            """)
            result = cursor.fetchone()
            if result:
                self.stdout.write(f"   Column: {result[0]}")
                self.stdout.write(f"   UDT Name: {result[1]}")
                self.stdout.write(f"   Data Type: {result[2]}")

            # Get PostGIS geometry type constraint
            cursor.execute("""
                SELECT type 
                FROM geometry_columns 
                WHERE f_table_name = 'periods_spatialentity' 
                AND f_geometry_column = 'geometry';
            """)
            geom_type = cursor.fetchone()
            if geom_type:
                self.stdout.write(f"   PostGIS Geometry Type: {geom_type[0]}")
            else:
                self.stdout.write("   PostGIS Geometry Type: Not found in geometry_columns")

        # 3. Test saving a simple MultiPolygon
        self.stdout.write("\n3. Testing save behavior with simple MultiPolygon:")

        # Create test entity or get existing one
        test_entity = SpatialEntity.objects.first()
        if not test_entity:
            self.stdout.write("   No entities found to test with")
            return

        self.stdout.write(f"   Using entity: {test_entity.uri}")
        self.stdout.write(f"   Current geometry type: {test_entity.geometry.geom_type if test_entity.geometry else 'None'}")

        # Create a simple test MultiPolygon
        poly1 = Polygon(((0, 0), (0, 1), (1, 1), (1, 0), (0, 0)))
        poly2 = Polygon(((2, 2), (2, 3), (3, 3), (3, 2), (2, 2)))
        test_mp = MultiPolygon([poly1, poly2])

        self.stdout.write(f"\n   Created test MultiPolygon:")
        self.stdout.write(f"   - Type: {test_mp.geom_type}")
        self.stdout.write(f"   - Valid: {test_mp.valid}")
        self.stdout.write(f"   - Num polygons: {len(test_mp)}")
        self.stdout.write(f"   - WKT: {test_mp.wkt[:100]}...")

        # Save it
        original_geom = test_entity.geometry
        test_entity.geometry = test_mp
        test_entity.save()

        # Reload and check
        test_entity.refresh_from_db()
        self.stdout.write(f"\n   After save and refresh:")
        self.stdout.write(f"   - Type: {test_entity.geometry.geom_type}")
        self.stdout.write(f"   - WKT: {test_entity.geometry.wkt[:100]}...")

        # Check at SQL level
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT ST_GeometryType(geometry), ST_AsText(geometry)
                FROM periods_spatialentity
                WHERE uri = %s;
            """, [test_entity.uri])
            result = cursor.fetchone()
            if result:
                self.stdout.write(f"\n   Direct SQL query result:")
                self.stdout.write(f"   - ST_GeometryType: {result[0]}")
                self.stdout.write(f"   - ST_AsText: {result[1][:100]}...")

        # Restore original geometry
        test_entity.geometry = original_geom
        test_entity.save()

        # 4. Check if there's a custom save method
        self.stdout.write("\n4. Checking for custom save method on SpatialEntity:")
        if hasattr(SpatialEntity, 'save'):
            save_method = SpatialEntity.save
            if save_method.__module__ != 'django.db.models.base':
                self.stdout.write(f"   Custom save method found in: {save_method.__module__}")
                import inspect
                try:
                    self.stdout.write(f"   Source file: {inspect.getfile(save_method)}")
                except:
                    self.stdout.write("   Could not determine source file")
            else:
                self.stdout.write("   Using default Django save method")

        # 5. Check for signals
        self.stdout.write("\n5. Checking for pre_save/post_save signals:")
        from django.db.models.signals import pre_save, post_save

        pre_save_receivers = pre_save._live_receivers(SpatialEntity)
        post_save_receivers = post_save._live_receivers(SpatialEntity)

        if pre_save_receivers:
            self.stdout.write(f"   Found {len(pre_save_receivers)} pre_save signal(s):")
            for receiver in pre_save_receivers:
                self.stdout.write(f"   - {receiver}")
        else:
            self.stdout.write("   No pre_save signals found")

        if post_save_receivers:
            self.stdout.write(f"   Found {len(post_save_receivers)} post_save signal(s):")
            for receiver in post_save_receivers:
                self.stdout.write(f"   - {receiver}")
        else:
            self.stdout.write("   No post_save signals found")

        # 6. Sample some actual geometries from the database
        self.stdout.write("\n6. Sampling actual geometries in database:")
        sample_entities = SpatialEntity.objects.filter(geometry__isnull=False)[:5]
        for entity in sample_entities:
            self.stdout.write(f"\n   Entity: {entity.uri}")
            self.stdout.write(f"   - Geometry type: {entity.geometry.geom_type}")
            self.stdout.write(f"   - WKT preview: {entity.geometry.wkt[:80]}...")

        self.stdout.write("\n" + "=" * 80)
        self.stdout.write("DIAGNOSTIC COMPLETE")
        self.stdout.write("=" * 80)