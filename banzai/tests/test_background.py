# -*- coding: utf-8 -*-
"""
Created on Mon Jun 13 14:17:53 2016

@author: rstreet
"""

from __future__ import division
from .utils import FakeImage
from banzai.background import SkyBackgroundTest


def test_no_input_images():
    tester = SkyBackgroundTest(None)
    images = tester.do_stage([])
    assert len(images) == 0


def test_group_by_keywords():
    tester = SkyBackgroundTest(None)
    assert tester.group_by_keywords is None


def test_background_step():
    tester = SkyBackgroundTest(None)
    nx = 501
    ny = 503

    images = [FakeImage(nx=nx, ny=ny) for x in range(3)]
    for image in images:
        image.header['SATURATE'] = 65535

    images = tester.do_stage(images)
    for image in images:
        assert 'AMPINFX' image.header['AMPINFX'][0] == 0.0
    assert len(images) == 3

