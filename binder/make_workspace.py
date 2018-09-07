import os, shutil
from jupyterlab_launcher.workspaces_handler import _slug

service_path = os.environ.get('JUPYTERHUB_SERVICE_PREFIX')
home = os.environ.get('HOME')
workspace_path = service_path.strip('/') + '/lab' if service_path else '/lab'
filename = _slug(workspace_path, '') + '.jupyterlab-workspace'
shutil.copy(
        './binder/jupyterlab-workspace.json',
        f'{home}/.jupyter/lab/workspaces/{filename}'
        )

