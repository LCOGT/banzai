****************
PyLCOGT package
****************

This pipeline is designed to take raw data taken from Las Cumbres Observatory Global Telescope (LCOGT) Network and produce
science quality data products.

There are several steps we first take to remove the instrument signature from the data.

Throughout the pipeline we use a robust sigma clipped mean which is explained below.

Robust Sigma-Clipped Mean
-------------------------
Sigma clipping is a standard technique to reject bad pixels. However, outliers artificially increase the standard
deviation (std) of the points (which makes outliers appear to be fewer sigma away from the peak) making it difficult to
produce robust results. We have adopted a slightly different method. We use the median absolute deviation (mad) to
estimate the scatter of the distribution (traditionally characterized by the standard deviation). The mad is related to the
std by

\sigma = 1.4826 * mad

We have termed this the "robust standard deviation" (rstd). Using the robust standard deviation, we mask pixels reliably and
take a mean of the remaining pixels as usual.

Individual Camera Considerations
--------------------------------
The Spectral and SBIG cameras produce single extension fits files. The Sinistro frames, however, produce
four amplifier outputs. Currently this data is saved as a data cube (but alternatively could be stored as
a multi-extension fits file). Currently the fits-preprocessor subtracts the overscan, removes amplifier cross-talk,
and mosaics the 4 amplifiers into a single image before the pipeline processes the data. These will eventually be
merged into a single process, here in this pipeline. The other difficulty with the Sinistro camera is that
the top amplifiers (3 and 4) have more light sensitive pixels than the bottom amplifiers (1 and 2). In
January 2016, this was taken in account. The cameras were set to read out more rows. This in turn produced
parallel overscan rows in amplifiers 1 and 2, but now all of the light sensitive pixels are read out properly for
amplifiers 3 and 4. The fits-preprocessor has been updated accordingly. Amplifiers 3 and 4 also have
an odd number of light sensitive pixels, meaning that on camera binning must be done with care.
Currently only 1x1 binning is supported for Sinsitros.

Bad Pixel Masks
---------------

Overscan
========
At LCOGT, some of our cameras have overscan regions and others that do not. Of the currently deployed instruments,
the Sinistro cameras and the Spectral cameras have overscan regions. The SBIG cameras are capable of producing an
overscan region but are currently configured to not produce either parallel or serial overscan.

We currently use the header the keyword BIASSEC to identify the overscan region. If this keyword is set to
"Unknown", then we simply skip subtracting the overscan.

The Sinistro frames do have overscan regions, but the overscan is currently removed by the preprocessor.


Crosstalk
=========
Currently, only Sinstro images are read out using multiple amplifiers. The Sinsitro frames do have significant
crosstalk between amplifiers, but this is currently removed by the preprocessor.


Mosaic
======
Again, only the Sinstro frames currently read out with multiple amplifiers so mosaicing the amplifiers
is only required for Sinsitros. This is currently done by the preprocessor.


Master Bias Creation
====================
For all instruments, we take many full-frame bias exposures every afternoon and morning. The afternoon and morning sets
of bias frames are typically reduced together for quality control and to increase statistics.

When creating a master bias frame for the night, we first calculate the sigma clipped mean of each image.
In this case, outliers that are 3.5 rstd from the median are rejected before calculating the mean. As
the read noise is approximately Gaussian (to surprisingly high precision), the median is a robust estimation
of the center of the pixel brightness distribution.

We then stack the individual bias frames. On a pixel by pixel basis, we reject 3 rstd outliers, and then
take the sigma clipped mean. This should remove and structure produced by the readout process. The noise
in each pixel should scale as sqrt(number of bias images). We take ~64 frames per night reducing the
noise per pixel to read noise (RN) / 8. Thus, only a few counts of noise are being added to the frames,
not significantly impacting the final science frames.


Bias Subtraction
================
Full frame bias images are subtracted from each of the darks, flat field images, and science frames.
The master bias frame that was taken closest in time to the current data will be used.
This will add a few counts of noise to each image, but it solves two problems. First, if there is systematic
structure in the read out, this will be removed. Second, this will remove the bias values for images
that do not have an overscan region.


Master Dark Creation
====================


Dark Subtraction
================


Master Flat Field Creation
==========================


Flat Field Correction
=====================


Astrometry
==========


Source Detection
================


Cosmic Ray Detection
====================


Photometric Calibration
=======================


Reference/API
=============

.. automodapi:: pylcogt
