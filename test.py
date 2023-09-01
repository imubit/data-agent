import os
import shutil
import sys

import pytest

basedir = os.path.abspath(os.path.dirname(__file__))


def main():
    argv = []

    argv.extend(sys.argv[1:] + ["-v", "-l", "--durations=0"])

    result = pytest.main(argv)

    try:
        os.remove(os.path.join(basedir, ".coverage"))

    except OSError:
        pass

    try:
        shutil.rmtree(os.path.join(basedir, ".cache"))

    except OSError:
        pass

    try:
        shutil.rmtree(os.path.join(basedir, "tests/.cache"))
    except OSError:
        pass

        sys.exit(result)


if __name__ == "__main__":
    main()
