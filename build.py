from __future__ import annotations

import os
import shutil

from pathlib import Path

from extension_helpers import get_extensions
import os
from setuptools import Distribution
from setuptools.command.build_ext import build_ext

def build() -> None:
    ext_modules = get_extensions()

    distribution = Distribution({
        "name": "package",
        "ext_modules": ext_modules
    })
    cmd = build_ext(distribution)
    cmd.ensure_finalized()
    cmd.run()

    # Copy built extensions back to the project
    for output in cmd.get_outputs():
        output = Path(output)
        relative_extension = output.relative_to(cmd.build_lib)

        shutil.copyfile(output, relative_extension)
        mode = os.stat(relative_extension).st_mode
        mode |= (mode & 0o444) >> 2
        os.chmod(relative_extension, mode)


if __name__ == "__main__":
    build()
