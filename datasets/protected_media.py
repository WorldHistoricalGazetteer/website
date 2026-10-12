"""Media files that belong to a dataset or a user are served only to people allowed to see them
(place#310).

nginx served ``/media/`` straight from disk, so two kinds of file were fetchable by anyone who
could guess the path, whatever the dataset's ``public`` flag said:

* ``user_<name>/<original filename>`` — uploaded dataset files (``DatasetFile``) and dataset PDFs,
  for private datasets as much as public ones;
* ``downloads/`` — exports written for one requester (``<user id>_<label or coll id>_<time>``) and
  the shared export caches (``whg_<type>_<id>.<ext>.gz``), which hold whole datasets and collections.

nginx now hands those two prefixes to this view, which decides and then serves the file through an
``internal`` nginx location (``X-Accel-Redirect``) so the bytes never pass through a worker. Every
other prefix (help pages, collection and dataset images, resources) stays public and is still served
by nginx directly. Anything refused gets the same 404 as a missing file, so a private file is not
confirmed to exist.
"""
import mimetypes
import os
import posixpath
import re

from django.conf import settings
from django.http import FileResponse, Http404, HttpResponse

from api.dataset_access import dataset_visible_to

# Requester-owned exports from utils.tasks.make_download: "<user id>_<label or coll id>_<time>.<ext>".
# "anon_" marks one written for an anonymous requester: it holds only public material.
_EXPORT = re.compile(r'^(?P<owner>\d+|anon)_.+')

# Internal nginx location that aliases MEDIA_ROOT (see server-admin/nginx-template.j2).
ACCEL_PREFIX = '/protected-media/'


def _is_staff(user):
    return bool(getattr(user, 'is_authenticated', False) and (user.is_staff or user.is_superuser))


def _clean(path):
    """The path relative to MEDIA_ROOT, or None for anything that is not already plain. A path
    that normalisation would change (``..``, ``.``, ``//``) is refused rather than rewritten, so the
    prefix the decision is made on is the file that gets served."""
    if '\x00' in path or path.startswith('/') or posixpath.normpath(path) != path:
        return None
    return path


def _may_read_upload(user, path):
    """``user_<name>/<file>``: the directory's own user, staff, or anyone who may view a dataset
    whose uploaded file or PDF is exactly this path."""
    from datasets.models import Dataset, DatasetFile

    if _is_staff(user):
        return True
    owner_dir = path.split('/', 1)[0][len('user_'):]
    if getattr(user, 'is_authenticated', False) and owner_dir in {user.username, getattr(user, 'name', None)}:
        return True
    for df in DatasetFile.objects.filter(file=path).select_related('dataset_id'):
        if dataset_visible_to(df.dataset_id, user):
            return True
    for ds in Dataset.objects.filter(pdf=path):
        if dataset_visible_to(ds, user):
            return True
    return False


def _may_read_download(user, name):
    """``downloads/<name>``: an export belongs to the user it was written for; the shared caches
    are read through their own gated views, so directly only by staff."""
    if _is_staff(user):
        return True
    m = _EXPORT.match(name)
    if not m:
        return False
    if m['owner'] == 'anon':
        return True
    return bool(getattr(user, 'is_authenticated', False) and m['owner'] == str(user.id))


def may_read(user, path):
    if path.startswith('user_') and '/' in path:
        return _may_read_upload(user, path)
    if path.startswith('downloads/') and path.count('/') == 1:
        return _may_read_download(user, path.split('/', 1)[1])
    return False


def protected_media(request, path):
    path = _clean(path)
    if not path or not may_read(request.user, path):
        raise Http404
    full = os.path.join(settings.MEDIA_ROOT, path)
    if not os.path.isfile(full):
        raise Http404
    filename = os.path.basename(path)
    if getattr(settings, 'PROTECTED_MEDIA_ACCEL', not settings.DEBUG):
        response = HttpResponse()
        response['X-Accel-Redirect'] = ACCEL_PREFIX + path
        ctype, encoding = mimetypes.guess_type(filename)
        # A .gz is delivered as the gzip file it is, never decoded on the way.
        response['Content-Type'] = 'application/gzip' if encoding == 'gzip' else (ctype or 'application/octet-stream')
    else:
        response = FileResponse(open(full, 'rb'), filename=filename)
    # Private to this viewer: no shared cache may keep it.
    response['Cache-Control'] = 'private, no-store'
    return response
