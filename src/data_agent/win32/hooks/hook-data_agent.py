# https://stackoverflow.com/questions/48884766/pyinstaller-on-a-setuptools-package
import logging

import pkg_resources
from PyInstaller.utils.hooks import (
    collect_data_files,
    collect_submodules,
    copy_metadata,
)

log = logging.getLogger(__name__)

# datas = copy_metadata('data_agent')
datas = collect_data_files(
    "data_agent", include_py_files=True, includes=["./win32/config_default.yaml"]
)
datas.extend(copy_metadata("data_agent"))
hiddenimports = collect_submodules("data_agent")

# Find connector packages
installed_connectors = [
    pkg.key.replace("-", "_")
    for pkg in pkg_resources.working_set
    if pkg.key.startswith("data-agent-")
]
for connector in installed_connectors:
    log.info(f"Including Data Agent connector: {connector}")
    datas.extend(copy_metadata(connector))
    hiddenimports.extend(collect_submodules(connector))


# import os
#
# datas = [(f'{os.path.dirname(__file__)}\\..\\win32\\config_default.yaml', 'win32')]
