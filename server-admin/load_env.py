# load_env.py
import json
import re
import os
import shutil
import importlib.util
import subprocess
import stat
import pwd
import grp
from dotenv import load_dotenv
from collections import OrderedDict
from jinja2 import Template

def get_docker_network_subnet(network_name='whgazetteer-org_whgazetteer-org'):
    try:
        output = subprocess.check_output([
            'docker', 'network', 'inspect', network_name
        ])
        net_info = json.loads(output)
        subnet = net_info[0]['IPAM']['Config'][0]['Subnet']
        return subnet
    except Exception as e:
        print(f"Failed to detect subnet for {network_name}: {e}")
        return None

def get_git_branch():
    try:
        # Run 'git rev-parse --abbrev-ref HEAD' to get the current branch name
        branch = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).strip().decode('utf-8')
        return branch
    except subprocess.CalledProcessError:
        return None

def get_git_release():
    """Return a short Git commit hash for tagging the current release."""
    try:
        return subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).strip().decode('utf-8')
    except subprocess.CalledProcessError:
        return 'unknown'

def update_entrypoints(entrypoints_path):

    for file in os.listdir(entrypoints_path):
        file_path = os.path.join(entrypoints_path, file)
        
        # Skip directories
        if os.path.isdir(file_path):
            continue

        try:
            # Remove any Windows-style line endings (carriage returns)
            with open(file_path, 'r+b') as f:
                content = f.read()
                content = content.replace(b'\r\n', b'\n')
                f.seek(0)
                f.write(content)
                f.truncate()
            # Ensure file is executable by any user within the container
            os.chmod(file_path, os.stat(file_path).st_mode | stat.S_IEXEC)
            
        except PermissionError as e:
            print(f"PermissionError: {e} - file_path: {file_path}")
        except Exception as e:
            print(f"Error: {e} - file_path: {file_path}")

def load_template(template_path):
    if not os.path.isfile(template_path):
        raise FileNotFoundError(f"Template file {template_path} does not exist.")
    
    spec = importlib.util.spec_from_file_location("env_template", template_path)
    env_template = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(env_template)
    return env_template.ENV_VARS

def apply_context_overrides(template_vars, context):
    context_vars = template_vars.get('base', {}).copy()
    context_vars.update(template_vars.get('sites', {}).get(context, {}))
    context_vars['BASE_DIR'] = os.getcwd()
    context_vars['ENV_CONTEXT'] = context 
    context_vars['BRANCH'] = get_git_branch().replace('/', '--')
    context_vars['POSTGRES_USER'] = 'postgres'
    context_vars['POSTGRES_PASSWORD'] = context_vars['DB_PASSWORD']
    context_vars['POSTGRES_DB'] = context_vars['DB_NAME']
    context_vars['CELERY_BROKER_URL'] = f"redis://redis:6379/0"
    context_vars['GLITCHTIP_RELEASE'] = get_git_release()

    return OrderedDict(sorted(context_vars.items()))

def write_env_file(env_vars, output_path):
    with open(output_path, 'w') as file:
        for key, value in env_vars.items():
            file.write(f'{key}={value}\n')

# A bare digit string is only safe to emit unquoted if it is a valid Python integer literal. `isdigit()`
# is not that test: "098610660" is all digits and raises SyntaxError ("leading zeros in decimal integer
# literals are not permitted"), which is a broken settings module and a site that will not boot. That is
# not hypothetical — a git short hash of 098610660 took the dev site down. Roughly one commit in a
# thousand draws an all-digit hash beginning with zero, so the failure is rare, load-bearing and
# arrives at deploy time.
_INT_LITERAL = re.compile(r'0|[1-9][0-9]*')


def write_python_file(env_vars, output_path):
    with open(output_path, 'w') as file:
        file.write("# This file is auto-generated from .env\n\n")
        for key, value in env_vars.items():
            if _INT_LITERAL.fullmatch(value):
                file.write(f"{key} = {value}\n")
            elif value.lower() in ['true', 'false']:
                file.write(f"{key} = {value.capitalize()}\n")
            else:
                file.write(f"{key} = {value!r}\n")

def render_jinja_template(template_path, env_vars, output_path):
    with open(template_path, 'r') as file:
        template_content = file.read()

    template = Template(template_content)
    rendered_content = template.render(env_vars)

    with open(output_path, 'w') as file:
        file.write(rendered_content)

def load_environment(context='local', 
        template_path='env_template.py', 
        output_path='../.env/.env',
        compose_template_path='../compose/docker-compose-template.j2',
        compose_output_path='../docker-compose-autocontext.yml',
        hba_template_path='../compose/pg_hba-template.j2',
        hba_output_path='../compose/pg_hba.conf',
        entrypoints_path='../entrypoints',
        python_output_path='../whg/local_settings_autocontext.py',
        nginx_template_path='nginx-template.j2',
        nginx_output_path='/etc/nginx/sites-available/',
        scripts_to_make_executable=['./replicate_live_db.sh','./build_docker.py','./restore_db_from_dump.sh']
        ):
    # Ensure paths are relative to the script's directory
    script_dir = os.path.dirname(__file__)
    template_path = os.path.join(script_dir, template_path) if context == 'local' else f'/home/whgadmin/sites/{template_path}'
    output_path = os.path.join(script_dir, output_path)
    compose_template_path = os.path.join(script_dir, compose_template_path)
    compose_output_path = os.path.join(script_dir, compose_output_path)
    hba_template_path = os.path.join(script_dir, hba_template_path)
    hba_output_path = os.path.join(script_dir, hba_output_path)
    entrypoints_path = os.path.join(script_dir, entrypoints_path)    
    python_output_path = os.path.join(script_dir, python_output_path)
    nginx_template_path = os.path.join(script_dir, nginx_template_path)
    nginx_output_path = os.path.join(script_dir, nginx_output_path)
    
    # Generate environment variable file
    try:
        template_vars = load_template(template_path)
    except FileNotFoundError as e:
        print(e)
        return
    
    env_vars = apply_context_overrides(template_vars, context)

    if not env_vars.get('SUBNET'):
        env_vars['SUBNET'] = get_docker_network_subnet() or '172.20.0.0/16'

    write_env_file(env_vars, output_path)
    load_dotenv(output_path)
    
    # Render files using Jinja2
    render_jinja_template(compose_template_path, env_vars, compose_output_path)
    render_jinja_template(hba_template_path, env_vars, hba_output_path)
    if not context == 'local':
        render_jinja_template(nginx_template_path, env_vars, os.path.join(nginx_output_path, env_vars.get('NGINX_SERVER_NAME')))
        
    update_entrypoints(entrypoints_path)
    write_python_file(env_vars, python_output_path)

    # Make specified scripts executable
    for script in scripts_to_make_executable:
        script_path = os.path.join(script_dir, script)
        try:
            # Remove any Windows-style line endings (carriage returns)
            with open(script_path, 'r+b') as f:
                content = f.read()
                content = content.replace(b'\r\n', b'\n')
                f.seek(0)
                f.write(content)
                f.truncate()
            os.chmod(script_path, os.stat(script_path).st_mode | stat.S_IEXEC)
            print(f"Made {script_path} executable.")
        except Exception as e:
            print(f"Failed to make {script_path} executable: {e}")

if __name__ == "__main__":
    print(f"Current working directory: {os.getcwd()}")
    context = os.path.basename(os.getcwd())
    if context not in ['dev-whgazetteer-org', 'whgazetteer-org']:
        context = 'local'
    load_environment(context)
    
    print(f"Context '{context}': environment variables loaded, and written to `.env`, `docker-compose.yml`, and nginx configuration. Entrypoint permissions fixed.")
