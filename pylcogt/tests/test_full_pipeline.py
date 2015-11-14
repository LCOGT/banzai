"""

@author: mnorbury
"""
import pytest

from pylcogt.main import main
from pylcogt.dbs import create_db


def setup_function(function):
    create_db('sqlite:///test.db')


def teardown_function(function):
    pass


def test_something():
    import os
    print os.system('ls')
    main('--raw-path /home/mnorbury/Pipeline/ --processed-path /home/mnorbury/tmp/ --log-level debug --site elp --epoch 20150325'.split())

    assert 1 == 2


if __name__ == '__main__':
    pytest.main([__file__, ])
