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
Currently bad pixel masks are not included. Using robust statistics should guard from the effects of
bad pixels (mostly) so we still produce clean data products. This feature is one of the highest on the
list of priorities and will be added soon.


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
This will add a few counts of noise to each pixel, but it solves two problems. First, if there is systematic
structure in the read out, this will be removed. Second, this will remove the bias values for images
that do not have an overscan region.


Trim
====
After the overscan and bias are subtracted, the darks, flat-field, and science images are trimmed
based on the TRIMSEC header keyword.


Master Dark Creation
====================
For all instruments, we take full-frame dark exposures every afternoon and morning. Like the bias frames,
the afternoon and morning dark frames are combined together to increase statistics. Typically, a
total of 20x300s images are taken.

When creating a master dark frame, each individual frame is scaled by the exposure time (read from the
header). The sigma clipped mean of the scaled frames is then calculated on a pixel by pixel basis.
We reject any 3 rstd outliers, similar to the master bias creation.

Our cameras have dark currents of 0.1-0.2 electrons / s per pixel. For 20x300s this corresponds to
1 - 2 electrons of additional noise per pixel (given the same length science frame, and not including
the Poisson noise from the dark current itself).


Dark Subtraction
================
Full-frame master dark frames are subtracted from all flat-field and science images. The most recent
master dark frame is used. Often this is taken on the same day.


Master Flat Field Creation
==========================
Twilight flats are taken every day. However, flat-field images for every filter are not taken daily,
because twilight is not long enough to take all of them. Instead the choice of filter is rotated,
based on the necessary exposure time to get a high signal to noise image and the popularity of the
filter for science programs. Typically, a master flat field is produced about once a week for any
given filter. When a flat-field image is taken for a given filter is taken in the evening twilight,
it is also taken in morning twilight for quality control. Typically, 5 flat field frames are taken
in the evening and 5 taken in the morning.

Each individual flat-field image is normalized to unity. The normalization is calculated by fitting a
Gamma distribution to the pixel distribution. The fit is done as an iteratively reweighted,
Levenberg-Marquardt least squares fit. The weights are given by Andrew's Wave weighting function,
a common weighting scale for robust M-estimation. The Gamma distribution was chosen because it includes
a tail of values greater than the mode. This is particularly important for CCDs with a large variation
in pixel sensitivities, e.g. the Spectral Camera at blue wavelengths. Before fitting the Gamma distribution,
outliers that are 4 rstd away from the median are removed.

The flat-field frames are then stacked using a sigma clipped mean, similar to the master bias and
dark frames. We again choose to reject 3 rstd outliers.

Currently, there are two main failure modes of individual flat-field images: saturation and shutter
failures. Currently, frames with either of these issues are not rejected outright (however, in future
versions, they likely will be). However, this is not an issue because we do robust sigma clipping.
Any pixels affected by these failure modes will be rejected automatically.


Flat Field Correction
=====================
Master flat field images (normalized to unity) are divided out of every science frame. The most recent
master flat-field image for the given telescope, filter, and binning is used.


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

We set the source detection limit at 3 times the global rms of the image. This should minimize false
detections, but may miss the faintest sources.


Astrometry
==========
The WCS is found by using Astrometry.net (Lang et al. 2009, arXiv:0910.2233). We use the catalog from
the source detection (the previous step) as input.

Currently no image distortion is included in the WCS.
We adopt a code tolerance of 0.003 (a factor of 3 smaller than the default), but increase the centroid
uncertainty to be 20 pixels. The large centroid uncertainty allows the algorithm to find quads even
if the initial guess is quite poor and even if there is significant distortion. However, decreasing
the code tolerance forces the algorithm to only use high quality quads, making the solution more
robust. We also go deeper into the catalogs (200 quads deep) to increase the chances of a successful
astrometry solution.

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
