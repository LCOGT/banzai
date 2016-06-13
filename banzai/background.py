# -*- coding: utf-8 -*-
"""
Created on Mon Jun 13 13:56:27 2016

@author: rstreet
"""

from __future__ import division

class SkyBackgroundTest(Stage):
    """
    Quality control test for structure in the sky background of an image
    
    The most common cause of this failure is shutter failure, which results in 
    dark bands of varying width across the bottom of the image.
    """
    
    def __init__(self, pipeline_context):
        super(SaturationTest, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        
        pass
        
        return images