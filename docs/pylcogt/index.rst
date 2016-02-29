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
and mosaics the 4 amplifiers into a single image before the pipeline processes the data.The preprocessor will eventually be
merged into this this pipeline.

The other difficulty with the Sinistro camera is that
the top amplifiers (3 and 4) have more light sensitive pixels than the bottom amplifiers (1 and 2). All of the parallel
overscan rows are discarded. One edge row is discarded for the amplifiers that have an extra light sensitive row.
The final frame size is 4096x4096.
Currently only 1x1 binning is supported for Sinsitros.


Bad Pixel Masks
---------------
Currently bad pixel masks are not included, but will be soon. Using robust statistics should guard from the effects of
bad pixels (mostly) so we still produce clean data products.


Overscan
========
At LCOGT, some of our cameras have overscan regions and others that do not. Of the currently deployed instruments,
the Sinistro cameras and the Spectral cameras have overscan regions. The SBIG cameras are capable of producing an
overscan region but are currently configured to not produce either parallel or serial overscan.

We currently use the header the keyword BIASSEC to identify the overscan region. If this keyword is set to
"Unknown", then we simply skip subtracting the overscan. We estimate a single overscan value for the whole image
(rather than row by row).
This is saved in the header under the keyword 'BIASLVL'. If there is no overscan section, this value is derived
from the average value from the bias frames.

The Sinistro frames do have overscan regions, but the overscan is currently removed by the preprocessor.


Crosstalk
=========
Currently, only Sinstro images are read out using multiple amplifiers. The Sinsitro frames do have significant
crosstalk between amplifiers, but this is currently removed by the preprocessor. The crosstalk is removed using
linear coefficients that relate each quadrant to every other quadrant.


Mosaic
======
Again, only the Sinstro frames currently read out with multiple amplifiers so mosaicing the amplifiers
is only required for Sinsitros. This is currently done by the preprocessor.


Trim
====
After the overscan is subracted, the bias, dark, flat-field, and science images are trimmed
based on the TRIMSEC header keyword.


Master Bias Creation
====================
For all instruments, we take many full-frame bias exposures every afternoon and morning. The afternoon and morning sets
of bias frames are typically reduced together for quality control and to increase statistics.

When creating a master bias frame for the night, we first calculate the sigma clipped mean of each image.
In this case, outliers that are 3.5 rstd from the median are rejected before calculating the mean. As
the read noise is approximately Gaussian (to surprisingly high precision), the median is a robust estimation
of the center of the pixel brightness distribution.

We then stack the individual bias frames. On a pixel by pixel basis, we reject 3 rstd outliers, and then
take the mean. This should remove and structure produced by the readout process. The noise
in each pixel should scale as sqrt(number of bias images). We take ~64 frames per night reducing the
noise per pixel to read noise (RN) / 8. Thus, only a few counts of noise are being added to the frames in quadrature.
This is much less than the ~10 electron read noise, meaning that this does not increase the noise in the science
frames in any significant way.


Bias Subtraction
================
Full frame bias images are subtracted from each of the darks, flat field images, and science frames.
The master bias frame that was taken closest in time to the current data will be used.
This will add a few counts of noise to each pixel, but it solves two problems. First, if there is systematic
structure in the read out, this will be removed. Second, this will remove the bias values for images
that do not have an overscan region.

If no bias frame exists for this instrument, the data will not be reduced and an exception will be
raised.


Master Dark Creation
====================
For all instruments, we take full-frame dark exposures every afternoon and morning. Like the bias frames,
the afternoon and morning dark frames are combined together to increase statistics. Typically, a
total of 20x300s images are taken.

When creating a master dark frame, each individual frame is scaled by the exposure time (read from the
header). The sigma clipped mean of the scaled frames is then calculated on a pixel by pixel basis.
We reject any 3 rstd outliers, similar to the master bias creation.

Our cameras have dark currents of 0.1-0.2 electrons / s per pixel. For 20x300s this corresponds to
1 - 2 electrons of additional noise per pixel added in quadrature (given the same length science frame,
and not including the Poisson noise from the dark current itself). Again, this is much smaller than the
read noise so it will not affect the noise properties of the final science frames.


Dark Subtraction
================
Full-frame master dark frames, scaled to the exposure time of the frame,
are subtracted from all flat-field and science images. The most recent
master dark frame is used. Often this is taken on the same day. If no dark frame exists for this
instrument, the data will not be reduced and an exception will be raised.


Master Flat Field Creation
==========================
Twilight flats are taken every day. However, flat-field images for every filter are not taken daily,
because twilight is not long enough to take all of them. Instead the choice of filter is rotated,
based on the necessary exposure time to get a high signal to noise image and the popularity of the
filter for science programs. Typically, a master flat field is produced about once a week for any
given filter. When a flat-field image is taken for a given filter is taken in the evening twilight,
it is also taken in morning twilight for quality control. Typically, 5 flat field frames are taken
in the evening and 5 taken in the morning per filter. The frames are dithered so that we can remove
stars in the combined master flat field.

Each individual flat-field image is normalized to unity before combing them.
The normalization is calculated finding the robust sigma clipped mean (3.5 rstd outliers are rejected) of
the central region of the image. For the central region, we choose the central 25% of the field (the region
has dimensions that are half of the full image).

The flat-field frames are then stacked using a sigma clipped mean, similar to the master bias and
dark frames. We again choose to reject 3 rstd outliers.

Currently, there are two main failure modes of individual flat-field images: saturation and shutter
failures. Currently, frames with either of these issues are not rejected outright (however, in future
versions, they likely will be). However, this is not an issue because we do robust sigma clipping.
Any pixels affected by these failure modes will be rejected automatically.


Flat Field Correction
=====================
Master flat field images (normalized to unity) are divided out of every science frame. The most recent
master flat-field image for the given telescope, filter, and binning is used. If no flat field exists,
the data will not be reduced and an exception will be raised.


Source Detection
================
Source detection uses the "Source Extraction in Python" (SEP; https://github.com/kbarbary/sep).
This is similar to Source Extractor, but is written purely in Python and Cython. This allows more
customization.

We estimate the background by taking a 3x3 median filter of the image and the doing a 32x32 block
average of the image.

We use the default match filter for source detection that is provided by SEP.

We do aperture photometry using an elliptical aperture that is set by 2.5 times the Kron radius. This
produces approximately the same results as FLUX_AUTO from SExtractor.

We set the source detection limit at 3 times the global rms of the image. MINAREA is set to 5,
 the default. This should minimize false detections, but may miss the faintest sources.

The catalog is returned as the 'CAT' as fits binary table extension of the final science image. The catalog
has the following columns: the position in pixel coordinates, (X, Y), the flux (Flux), the error in the flux
(Fluxerr), the semi-major and semi-minor axes (a, b), and the position angle (theta).


Astrometry
==========
The WCS is found by using Astrometry.net (Lang et al. 2009, arXiv:0910.2233). We use the catalog from
the source detection (the previous step) as input.

We adopt a code tolerance of 0.003 (a factor of 3 smaller than the default), but increase the centroid
uncertainty to be 20 pixels. The large centroid uncertainty allows the algorithm to find quads even
if the initial guess is quite poor and even if there is significant distortion. However, decreasing
the code tolerance forces the algorithm to only use high quality quads, making the solution more
robust. We also go deeper into the catalogs (200 quads deep) to increase the chances of a successful
astrometry solution.

Currently no non-linear distortion is included in the WCS (the current WCS solution only has a center,
a pixel scale, and a rotation). At worst (in the image corners), the offset between
coordinates with non-linear distortion terms included and those without are ~5 arcseconds.

Eventually, astrometry.net may be replaced with a purely Python alternative
(e.g. AliPy; http://obswww.unige.ch/~tewes/alipy/).


Cosmic Ray Detection
====================
Future feature: Likely will be implemented using the AstroSCRAPPY package, a variation of the
LA Cosmic algorithm (Van Dokkum, 2001, PASP, 113, 1420)


Photometric Calibration
=======================
Future Feature: Likely based on photometric calibration code by Stefano Valenti. Likely, Pan-Starrs 1
catalog will be used.


Reference/API
=============

.. automodapi:: pylcogt
