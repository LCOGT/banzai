# -*- coding: utf-8 -*-
"""
Created on Mon Jun 13 13:56:27 2016

@author: rstreet
"""

from __future__ import division
import numpy as np

class SkyBackgroundTest(Stage):
    """
    Quality control test for structure in the sky background of an image
    
    The most common cause of this failure is shutter failure, which results in 
    dark bands of varying width across the bottom of the image.
    """
    
    # Empirically-determined thresholds:
    skystddev_threshold = 0.7
    skygradient_threshold = 0.01
    dsky_threshold = 0.01
    dstddev_threshold = 0.1
    inflexion_threshold = 0.03

    def __init__(self, pipeline_context):
        super(SaturationTest, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        
        for image in images:
            
            # Calculate statistics of pixel box grid
            (xposition, yposition, sky_mean, sky_stddev) = \
                                            box_statistics(image)
            
            # Calculate the differential mean background .vs. pixel curve
            # i.e., the change in mean wrt pixel.
            dmean_dpix = np.zeros([len(sky_mean)-1,len(xposition)])
            for i,xcen in enumerate(xposition):
              	dmean_dpix[:,i] = (sky_mean[0:-1,i] - sky_mean[1:,i]) / \
                                       (yposition[0:-1] - yposition[1:])
            dpixels = yposition[1:]
        
            # Look for an inflexion point in the sky gradient with y-position, 
            # and calculate the metrics used for the QC test
            (y_inflex, amp_inflex, delta_mean, delta_stddev) = \
                    gradient_inflexion(xposition, yposition, dmean_dpix)
        
            # Establish test criteria, which are set relative to the
            # the mean sky background in this particular image:
            inflexion_threshold = delta_mean + 3.0*delta_stddev
            
            # Quality control decision:
            if inflexion_threshold > dmean_stddev*3.0:
                if y_inflex.std() == 0.0 and \
                       amp_inflex.mean() > inflexion_threshold: 
                    inflexion_test = True
                else: 
                    inflexion_test = False
            else:
              	inflexion_test = False
    
            if inflexion_test == True:
                self.logger.info(\
                'Measured amplitude of inflexion in background gradient', \
                extra=logging_tags)
                images.remove(image)
            else:
                image.header['AMPINFX'] = (amp_inflex.mean(), \
                                "Amplitude of inflexion in background gradient")

        return images

def box_statistics(image):
    """Function to map the sky background across an image by calculating 
    basic statistics in a series of small pixel regions in a regular grid, 
    working in columns up the frame, since the most common modes of error
    (shutter failure) creates horizontal banding structure.
    """
    
    # Excluding the very edges of the frame, calculate the pixel
    # positions of a grid of boxes across the frame.  This divides the
    # frame into N horizontal sections and N intervals increasing
    # in y-pixel.  Boxes need to be small relative to the width of the 
    # frame to ensure they sample different background structure 
    # if their is any:
    naxis1 = image.header['NAXIS1']
    naxis2 = image.header['NAXIS2']
    img_xmin = 100
    img_xmax = naxis2 - 100
    img_ymin = 100
    img_ymax = naxis1 - 100
    
    dypix = int(float(img_ymax)/10.0)
    dxpix = (float(img_xmax)/3.0)*0.5   
    xposition = arange(float(img_xmin)+dxpix,float(img_xmax),dxpix)

    # Compute the overall frame statistics, and normalize the 
    # image flux by the mean of its background:
    test_region = image.data[img_ymin:img_ymax,img_xmin:img_xmax]
    (image_mean,image_stddev) = rms_clip(test_region,3.0,3)
    image_norm = image.data / image_mean

    # Working up the frame, calculate the sky background mean and stddev
    # within each box:
    ymin = float(img_ymin)
    ymax = ymin + dypix
    yposition = []
    sky_mean = []
    sky_stddev = []
    while ymax < img_ymax:
	yposition.append(float(ymin) + float(dypix)/2.0)
	meanlist = []
	stddevlist = []
      	for xcen in xposition:
	    xmin = xcen - (dxpix/2.0)
	    xmax = xcen + (dxpix/2.0)
	    (mean,stddev) = rms_clip(image_norm[ymin:ymax,xmin:xmax],3.0,3)
	    meanlist.append(mean)
	    stddevlist.append(stddev)
	sky_mean.append(meanlist)
	sky_stddev.append(stddevlist)
	ymin = ymax
	ymax = ymin + dypix

    yposition = array(yposition)
    sky_mean = array(sky_mean) 
    sky_stddev = array(sky_stddev)

    return xposition, yposition, sky_mean, sky_stddev
 

def rms_clip(region_data,sigma,niter):
    """Function to calculate the mean and standard deviation of a set of pixel 
    values given an image region data array, using an iterative, sigma-clipping 
    function
    """
    
    region_data = region_data[np.logical_not(np.isnan(region_data))]
    
    idx = np.where(region_data < 1e9)
    for it in range(0,niter,1):
        mean = region_data[idx].mean(dtype='float64')
        std = region_data[idx].std(dtype='float64')
        idx1 = np.where(region_data >= (mean-sigma*std))
        idx2 = np.where(region_data <= (mean+sigma*std))
        idx = np.intersect1d(idx1[0],idx2[0])
    mean = region_data[idx].mean(dtype='float64')
    std = region_data[idx].std(dtype='float64')
    
    return mean, std

def gradient_inflexion(xposition,yposition,dmean_dpix):
    """Function to identify an inflexion point in the sky gradient with 
    y-position, using this to locate sudden changes in the background
    of the image
    """
    
    # Taking each column of boxes vertically up the frame in turn, find the box
    # where the rate of change of the sky background is greatest, recording
    # both the y-pixel position of the inflexion and the amplitude of it for 
    # each column.  
    y_inflex = []
    amp_inflex = []
    delta_inflex = []
    for i,xcen in enumerate(xposition):
        (dmean_col,dmean_stddev_col) = rms_clip(dmean_dpix[:,i],3.0,3)
        delta = abs(dmean_dpix[:,i] - dmean_col)
        idx = np.where(delta == delta.max())
        ypos = (yposition[idx[0][0]+1] + yposition[idx[0][0]+1]) / 2.0
        y_inflex.append(ypos)
        amp_inflex.append(delta[idx[0][0]])
        deltalist = delta.tolist()
        deltalist.pop(idx[0][0])
        delta_inflex.append(deltalist)
    y_inflex = array(y_inflex)
    amp_inflex = array(amp_inflex)
    delta_inflex = array(delta_inflex)
    
    (delta_mean,delta_stddev) = rms_clip(delta_inflex,3.0,3)

    return y_inflex, amp_inflex, delta_mean, delta_stddev
    