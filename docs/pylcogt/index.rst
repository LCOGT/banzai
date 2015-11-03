****************
PyLCOGT package
****************

This pipeline is designed to take raw data taken from Las Cumbres Observatory Global Telescope Network and produce
science quality data products.

There are several steps we first take to remove the instrument signature from the data.

Throughout the pipeline we use a robust sigma clipped mean which is explained below.

Robust Sigma-Clipped Mean
-------------------------
Sigma clipping is a standard technique to reject bad pixels. However, outliers artificially increase the standard
deviation (std) of the points (which makes outliers appear to be fewer sigma away from the peak)making it difficult to
produce robust results. We have adopted a slightly different method. We use the median absolute deviation (mad) to
estimate the scatter of the distribution (typically characterized by the standard deviation). The mad is related to the
std by

\sigma = 1.4826 * mad

We have termed this the "robust standard deviation". Using the robust standard deviation, we mask pixels reliably and
take a mean of the remaining pixels as usual.

Bias Subtraction
================
For all instruments, we take many full-frame bias exposures every afternoon and morning. The afternoon and morning sets
of bias frames are typically reduced together for quality control and to increase statistics.

When creating a master bias frame for the night, we remove the mean

Reference/API
=============

.. automodapi:: pylcogt
