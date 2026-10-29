"""Tests for the host-difference seams in server-admin: deploy.sh's helpers, load_env.py's path
overrides, set_image_tag.py's default path, and that the templates still render the paths the current
servers use. Run from the repository root:

    python3 -m unittest server-admin/tests/test_deploy_tooling.py -v

These exist because every one of these seams must leave the current servers' behaviour exactly as it
was while letting a differently-built host (another deploy user, no root, Docker's compose plugin)
use the same script.
"""
import importlib.util
import os
import re
import shutil
import stat
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ADMIN = ROOT / 'server-admin'


def deploy_functions():
    """The helper functions of deploy.sh as a string (the script cannot be sourced: it runs main)."""
    text = (ADMIN / 'deploy.sh').read_text()
    start = text.index('# ─── Host differences')
    end = text.index('# ─── Usage')
    return 'WHG_IMAGE="worldhistoricalgazetteer/web"\n' + text[start:end]


def run_bash(script, path_dirs, env=None):
    """Run a bash snippet with PATH limited to the given directories (plus bash's own builtins)."""
    full = {'PATH': ':'.join(str(d) for d in path_dirs), 'HOME': '/nonexistent'}
    full.update(env or {})
    # /bin/bash by absolute path, because PATH is deliberately restricted
    return subprocess.run(['/bin/bash', '-c', script], capture_output=True, text=True, env=full)


def make_exe(directory, name, body):
    path = Path(directory) / name
    path.write_text('#!/bin/bash\n' + body + '\n')
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return path


class PickComposeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.funcs = deploy_functions()
        # awk, sort, tr etc. are not needed by these functions

    def pick(self, env=None):
        return run_bash(self.funcs + '\npick_compose', [self.tmp], env).stdout.strip()

    def test_standalone_docker_compose_is_kept_where_present(self):
        make_exe(self.tmp, 'docker-compose', 'exit 0')
        make_exe(self.tmp, 'docker', 'exit 0')   # even with the plugin available
        self.assertEqual(self.pick(), 'docker-compose')

    def test_plugin_used_when_there_is_no_standalone(self):
        make_exe(self.tmp, 'docker', '[ "$1 $2" = "compose version" ] && exit 0; exit 1')
        self.assertEqual(self.pick(), 'docker compose')

    def test_override_wins(self):
        make_exe(self.tmp, 'docker-compose', 'exit 0')
        self.assertEqual(self.pick({'WHG_COMPOSE': 'podman-compose'}), 'podman-compose')

    def test_neither_available_falls_back_to_the_familiar_name(self):
        self.assertEqual(self.pick(), 'docker-compose')


class PickSudoTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.funcs = deploy_functions()

    def pick(self, env=None):
        # `set -e` as in deploy.sh: a non-zero return here would kill the script
        r = run_bash('set -euo pipefail\n' + self.funcs + '\nSUDO="$(pick_sudo)"; echo "[$SUDO]"', [self.tmp], env)
        self.assertEqual(r.returncode, 0, r.stderr)
        return r.stdout.strip()

    def test_sudo_used_where_installed(self):
        make_exe(self.tmp, 'sudo', 'exit 0')
        self.assertEqual(self.pick(), f'[{self.tmp}/sudo]')

    def test_no_sudo_flag_disables_it_even_if_installed(self):
        make_exe(self.tmp, 'sudo', 'exit 0')
        self.assertEqual(self.pick({'WHG_NO_SUDO': '1'}), '[]')

    def test_absent_sudo_is_empty_not_an_error(self):
        self.assertEqual(self.pick(), '[]')

    def test_run_admin_py_runs_without_sudo_and_passes_overrides(self):
        script = self.tmp / 'show.py'
        script.write_text('import os, sys\nprint(os.environ["WHG_SITES_DIR"], sys.argv[1:])\n')
        py = shutil.which('python3')
        os.symlink(py, self.tmp / 'python3')
        os.symlink(shutil.which('env'), self.tmp / 'env')
        r = run_bash(self.funcs + f'\nSUDO=""\nrun_admin_py {script} --site x',
                     [self.tmp], {'WHG_SITES_DIR': '/data/sites'})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(r.stdout.strip(), "/data/sites ['--site', 'x']")


class WhgImageServicesTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        for tool in ('awk', 'sort', 'tr'):
            os.symlink(shutil.which(tool), self.tmp / tool)
        self.funcs = deploy_functions()

    def services(self, ps_output):
        # a `docker` that records its arguments and prints canned `docker ps` rows
        (self.tmp / 'rows').write_text(ps_output + '\n')
        # bash builtins only (PATH is restricted): read/echo, no cat
        make_exe(self.tmp, 'docker',
                 f'echo "$@" > "{self.tmp}/args"\n'
                 f'while IFS= read -r line; do echo "$line"; done < "{self.tmp}/rows"')
        r = run_bash(self.funcs + '\nwhg_image_services whgazetteer-org_main', [self.tmp])
        self.assertEqual(r.returncode, 0, r.stderr)
        return r.stdout.split()

    def test_only_services_running_the_whg_image_are_named(self):
        rows = textwrap.dedent('''\
            worldhistoricalgazetteer/web:1.0.20|web
            worldhistoricalgazetteer/web:1.0.20|celery_worker
            worldhistoricalgazetteer/web:1.0.20|celery_beat
            worldhistoricalgazetteer/web:1.0.20|flower
            postgis/postgis:15-3.4|postgres
            redis:bullseye|redis
            ghcr.io/hocuspocus:latest|hocuspocus''')
        self.assertEqual(self.services(rows), ['celery_beat', 'celery_worker', 'flower', 'web'])
        self.assertIn('name=whgazetteer-org_main', (self.tmp / 'args').read_text())

    def test_lookalike_image_names_and_unlabelled_containers_are_excluded(self):
        rows = textwrap.dedent('''\
            worldhistoricalgazetteer/web-other:1|web2
            worldhistoricalgazetteer/webpack:1|webpack
            worldhistoricalgazetteer/web:1.0.20|
            worldhistoricalgazetteer/web|web''')
        self.assertEqual(self.services(rows), ['web'])   # bare image name (no tag) still counts

    def test_podman_registry_qualified_names_match_and_lookalikes_still_do_not(self):
        rows = textwrap.dedent('''\
            docker.io/worldhistoricalgazetteer/web:1.0.20|web
            docker.io/worldhistoricalgazetteer/web|celery_worker
            localhost/worldhistoricalgazetteer/web:1|celery_beat
            docker.io/worldhistoricalgazetteer/web-other:1|web2
            docker.io/xworldhistoricalgazetteer/web:1|imposter
            docker.io/library/redis:bullseye|redis''')
        self.assertEqual(self.services(rows), ['celery_beat', 'celery_worker', 'web'])

    def test_nothing_running_gives_nothing(self):
        self.assertEqual(self.services('redis:bullseye|redis'), [])


class TemplateAndLoaderTests(unittest.TestCase):
    def render(self, template, **extra):
        from jinja2 import Template
        names = set(re.findall(r'{{\s*([A-Za-z_][A-Za-z0-9_]*)', (ROOT / template).read_text()))
        values = {n: 'x' for n in names}
        values.update(ENV_CONTEXT='whgazetteer-org', BRANCH='main',
                      SITES_DIR='/home/whgadmin/sites', DATABASES_DIR='/home/whgadmin/databases')
        for k in ('MOUNT_LABEL', 'USERNS_WHG'):
            values.pop(k, None)   # opt-in; the stand-in value 'x' would switch them on
        values.update(extra)
        return Template((ROOT / template).read_text()).render(values)

    def test_default_render_keeps_the_current_servers_paths(self):
        compose = self.render('compose/docker-compose-template.j2')
        self.assertIn('/home/whgadmin/sites/logs/whgazetteer-org:/app/whg/logs', compose)
        self.assertIn('/home/whgadmin/sites/data_dumps:/app/data_dumps', compose)
        self.assertIn('/home/whgadmin/databases/whgazetteer-org:/var/lib/postgresql/data', compose)
        nginx = self.render('server-admin/nginx-template.j2')
        self.assertIn('alias /home/whgadmin/sites/whgazetteer-org/media/;', nginx)

    def test_overrides_move_every_path_and_leave_no_hardcoded_home(self):
        kw = dict(SITES_DIR='/srv/whg/sites', DATABASES_DIR='/srv/whg/databases')
        compose = self.render('compose/docker-compose-template.j2', **kw)
        nginx = self.render('server-admin/nginx-template.j2', **kw)
        for text in (compose, nginx):
            self.assertNotIn('/home/whgadmin', text)
        self.assertIn('/srv/whg/sites/logs/whgazetteer-org:/app/whg/logs', compose)
        self.assertIn('/srv/whg/databases/whgazetteer-org:/var/lib/postgresql/data', compose)
        self.assertIn('alias /srv/whg/sites/data_dumps/;', nginx)

    def test_set_image_tag_default_follows_the_sites_dir_override(self):
        spec = importlib.util.spec_from_file_location('set_image_tag_t', ADMIN / 'set_image_tag.py')
        old = os.environ.get('WHG_SITES_DIR')
        try:
            os.environ['WHG_SITES_DIR'] = '/srv/whg/sites'
            mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
            self.assertEqual(str(mod.DEFAULT_TEMPLATE), '/srv/whg/sites/env_template.py')
            os.environ.pop('WHG_SITES_DIR')
            mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
            self.assertEqual(str(mod.DEFAULT_TEMPLATE), '/home/whgadmin/sites/env_template.py')
        finally:
            if old is None: os.environ.pop('WHG_SITES_DIR', None)
            else: os.environ['WHG_SITES_DIR'] = old

    def test_load_env_end_to_end_without_root_paths(self):
        """Run load_env.py as a non-root deploy user would: everything it reads or writes is under
        directories that user owns."""
        tmp = Path(tempfile.mkdtemp()); self.addCleanup(shutil.rmtree, tmp, True)
        repo = tmp / 'whgazetteer-org'
        for d in ('server-admin', 'compose', 'entrypoints'):
            shutil.copytree(ROOT / d, repo / d, ignore=shutil.ignore_patterns('tests', '__pycache__', '*.pyc'))
        (repo / 'whg').mkdir(); (repo / '.env').mkdir()
        sites = tmp / 'sites'; sites.mkdir()
        (sites / 'env_template.py').write_text(textwrap.dedent('''\
            ENV_VARS = {'base': {'DB_PASSWORD': 'pw', 'DB_NAME': 'db', 'SUBNET': '10.9.0.0/16',
                                 'NGINX_SERVER_NAME': 'whg.test', 'DOCKER_IMAGE': 'img',
                                 'DOCKER_IMAGE_TAG': '1'},
                        'sites': {'whgazetteer-org': {}}}
            '''))
        nginx_dir = tmp / 'nginx.d'          # does not exist yet: the loader creates it
        subprocess.run(['git', 'init', '-q'], cwd=repo, check=True)
        subprocess.run(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', 'commit', '-q',
                        '--allow-empty', '-m', 'x'], cwd=repo, check=True)
        env = dict(os.environ, WHG_SITES_DIR=str(sites), WHG_DATABASES_DIR=str(tmp / 'databases'),
                   WHG_NGINX_DIR=str(nginx_dir))
        r = subprocess.run(['python3', str(repo / 'server-admin' / 'load_env.py')], cwd=repo,
                           env=env, capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        compose = (repo / 'docker-compose-autocontext.yml').read_text()
        self.assertIn(f'{sites}/logs/whgazetteer-org:/app/whg/logs', compose)
        self.assertIn(f'{tmp}/databases/whgazetteer-org:/var/lib/postgresql/data', compose)
        self.assertTrue((nginx_dir / 'whg.test').is_file(), 'nginx file should land in WHG_NGINX_DIR')
        self.assertNotIn('/home/whgadmin', compose)


class TileserverServiceTests(unittest.TestCase):
    """The tile server service is opt-in (TILES_DIR) and hardened; hosts without TILES_DIR (today's dev and
    prod) must not get it, because a bind mount of a missing directory would create it root-owned."""

    def services(self, **extra):
        import yaml
        from jinja2 import Template
        text = (ROOT / 'compose/docker-compose-template.j2').read_text()
        names = set(re.findall(r'{{\s*([A-Za-z_][A-Za-z0-9_]*)', text))
        values = {n: 'x' for n in names}
        values.update(ENV_CONTEXT='whgazetteer-org', BRANCH='main', SITES_DIR='/s', DATABASES_DIR='/d',
                      SUBNET='10.9.0.0/16')
        for k in ('TILES_DIR', 'TILESERVER_IMAGE', 'TILESERVER_IMAGE_TAG', 'TILESERVER_USER', 'TILESERVER_PORT',
                  'TILES_MOUNT_OPTS', 'MEM_LIMIT_TILESERVER', 'OLLAMA_NETWORK', 'OLLAMA_HOST_CONTEXT',
                  'MOUNT_LABEL', 'USERNS_WHG'):
            values.pop(k, None)
        values.update(extra)
        return yaml.safe_load(Template(text).render(values))['services']

    def test_absent_without_tiles_dir(self):
        self.assertNotIn('tileserver', self.services())

    def test_present_and_hardened_with_tiles_dir(self):
        svc = self.services(TILES_DIR='/srv/tiles')['tileserver']
        self.assertEqual(svc['image'], 'maptiler/tileserver-gl-light:v4.7.0')   # pinned, same as the droplet
        self.assertEqual(svc['user'], '1000:1000')
        self.assertTrue(svc['read_only'])
        self.assertEqual(svc['volumes'], ['/srv/tiles:/data:ro'])
        self.assertEqual(svc['ports'], ['127.0.0.1:8080:8080'])              # loopback only
        self.assertEqual(svc['command'], ['--config', '/data/configs/config.json'])

    def test_overrides(self):
        svc = self.services(TILES_DIR='/t', TILESERVER_USER='1001:1001', TILESERVER_PORT='18080',
                            TILES_MOUNT_OPTS='ro,z', MEM_LIMIT_TILESERVER='512m',
                            TILESERVER_IMAGE_TAG='v4.8.0')['tileserver']
        self.assertEqual(svc['user'], '1001:1001')
        self.assertEqual(svc['volumes'], ['/t:/data:ro,z'])
        self.assertEqual(svc['ports'], ['127.0.0.1:18080:8080'])
        self.assertEqual(svc['mem_limit'], '512m')
        self.assertTrue(svc['image'].endswith(':v4.8.0'))


if __name__ == '__main__':
    unittest.main()


class RootlessVariantTests(TileserverServiceTests):
    """MOUNT_LABEL / USERNS_WHG are for rootless Podman on SELinux; unset they must change nothing."""

    def host_binds(self, svc):
        return [v for v in svc.get('volumes', []) if isinstance(v, str) and v.startswith('/')]

    def test_unset_means_no_label_and_no_userns(self):
        for name, svc in self.services(DB_DIR='/db', BASE_DIR='/b').items():
            self.assertNotIn('userns_mode', svc, name)
            for v in self.host_binds(svc):
                self.assertNotIn(':z', v, f'{name}: {v}')
        self.assertIn('/b:/app', self.services(DB_DIR='/db', BASE_DIR='/b')['web']['volumes'])

    def test_label_goes_on_every_host_bind_and_not_on_named_volumes(self):
        svcs = self.services(MOUNT_LABEL='z', DB_DIR='/db', BASE_DIR='/b', TILES_DIR='/t', TILES_MOUNT_OPTS='ro,z')
        for name in ('web', 'celery_worker', 'celery_beat', 'flower', 'redis'):
            binds = self.host_binds(svcs[name])
            self.assertTrue(binds, name)
            for v in binds:
                self.assertTrue(v.endswith(':z'), f'{name}: {v}')
        self.assertTrue(any(v == '/b:/app:z' for v in svcs['web']['volumes']))
        self.assertIn('shared-tmp:/var/tmp', svcs['web']['volumes'])      # a named volume is left alone
        self.assertIn('/t:/data:ro,z', svcs['tileserver']['volumes'])     # tile server keeps its own option

    def test_userns_only_on_the_whg_image_services(self):
        svcs = self.services(USERNS_WHG='keep-id:uid=1000,gid=1000')
        with_it = sorted(n for n, s in svcs.items() if s.get('userns_mode'))
        self.assertEqual(with_it, ['celery_beat', 'celery_worker', 'flower', 'web'])
