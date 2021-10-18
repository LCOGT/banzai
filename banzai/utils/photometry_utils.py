import requests
from astropy.table import Table
from astropy import units
from astropy.coordinates import SkyCoord
import numpy as np
import emcee
from scipy.optimize import minimize
from banzai.utils.stats import robust_standard_deviation


def get_reference_sources(header, reference_catalog_url, nx=None, ny=None):
    # We need to covert to a dict instead of a fits header here. We also need to drop any comment and history cards
    # so we just do this dict comprehension because oddly enough astropy.io.fits does not seem to have this.
    payload = {key: header[key] for key in header}
    payload['NAXIS'] = 2
    if nx is not None:
        payload['NAXIS1'] = nx
    if ny is not None:
        payload['NAXIS2'] = ny
    response = requests.post(reference_catalog_url, json=payload)
    if not response.ok:
        try:
            response_message = response.json()["message"]
        except:
            response_message = ''
        message = f'{response.status_code}: {response.reason} for url: {response.url}. {response_message}'
        raise requests.HTTPError(message, response=response)
    return response.json()


def match_catalogs(input_catalog, reference_catalog, match_threshold=1.0) -> Table:
    """
    Match objects between catalogs by RA and Dec within a threshold (in arcseconds).

    Parameters
    ----------
    input_catalog: astropy Table-like
              Catalog with RA and Dec to be matched
    reference_catalog: astropy Table-like
              Reference catalog with RA and Dec to matched
    match_threshold: float
                     Threshold in offset to call a source a match in arcseconds

    Returns
    -------
    An astropy table with only matched sources but columns from both catalogs

    Notes
    -----
    We assume the reference catalog positions are better so we adopt them
    """
    input_catalog, reference_catalog = Table(input_catalog), Table(reference_catalog)

    input_coordinates = SkyCoord(ra=input_catalog['ra'], dec=input_catalog['dec'],
                                 unit=(units.deg, units.deg))
    reference_coordinates = SkyCoord(ra=reference_catalog['ra'], dec=reference_catalog['dec'],
                                     unit=(units.deg, units.deg))
    match_indexes, offsets, _ = input_coordinates.match_to_catalog_sky(reference_coordinates)

    matched_catalog = Table()
    good_matches = offsets <= (match_threshold * units.arcsec)
    for colname in input_catalog.colnames:
        matched_catalog[colname] = input_catalog[colname][good_matches]

    for colname in reference_catalog.colnames:
        matched_catalog[colname] = reference_catalog[colname][match_indexes][good_matches]

    return matched_catalog


def log_zeropoint_likelihood(theta, mags, mag_errors, catalog_mags, catalog_errors, colors, color_errors):
    # Our model assumes a zeropoint, color term, and a scatter term
    # catalog = -2.5 * log10(counts / exptime) + zeropoint + color_term * colors
    zeropoint, color_term, scatter = theta
    model = mags + zeropoint + color_term * colors
    sigma_squared = scatter ** 2.0 + mag_errors ** 2.0 + catalog_errors ** 2.0 + color_errors ** 2.0
    return -0.5 * np.sum((catalog_mags - model) ** 2.0 / sigma_squared + np.log(2.0 * np.pi * sigma_squared))


def fit_photometry(matched_catalog, image_filter, color_to_fit, exptime):
    mags, mag_errors = to_magnitude(matched_catalog['flux'], matched_catalog['fluxerr'], 0.0, exptime)
    catalog_mags, catalog_errors = matched_catalog[f'{image_filter}mag'], matched_catalog[f'{image_filter}magerr']

    colors = matched_catalog[f'{color_to_fit.split("-")[0]}mag'] - matched_catalog[f'{color_to_fit.split("-")[1]}mag']
    color_errors = matched_catalog[f'{color_to_fit.split("-")[0]}magerr'] ** 2.0
    color_errors += matched_catalog[f'{color_to_fit.split("-")[1]}magerr'] ** 2.0
    color_errors = np.sqrt(color_errors)

    zeropoint_guess = np.median(catalog_mags - mags)
    scatter_guess = robust_standard_deviation(catalog_mags - mags)
    initial_guess = [zeropoint_guess, 0.0, scatter_guess]

    # Reject outliers
    sources_to_fit = np.abs(catalog_mags - mags - zeropoint_guess) < (5.0 * scatter_guess)
    mags, mag_errors = mags[sources_to_fit], mag_errors[sources_to_fit]
    catalog_mags, catalog_errors = catalog_mags[sources_to_fit], catalog_errors[sources_to_fit]
    colors, color_errors = colors[sources_to_fit], color_errors[sources_to_fit]

    best_fit = minimize(lambda *args: -log_zeropoint_likelihood(*args), initial_guess,
                        args=(mags, mag_errors, catalog_mags, catalog_errors, colors, color_errors),
                        method='Nelder-Mead')

    nwalkers, ndim = 10, 3
    walker_starting_points = np.atleast_2d(best_fit.x).T + \
        np.array([np.random.uniform(-0.3, 0.3, size=nwalkers),
                  np.random.uniform(-0.1, 0.1, size=nwalkers),
                  np.random.uniform(-best_fit.x[-1], 0.5, size=nwalkers)])

    sampler = emcee.EnsembleSampler(nwalkers, ndim, log_zeropoint_likelihood,
                                    args=(mags, mag_errors, catalog_mags, catalog_errors, colors, color_errors))
    sampler.run_mcmc(walker_starting_points.T, 5000, progress=False)
    flattened_samples = sampler.get_chain(discard=100, thin=15, flat=True).T
    zeropoint = np.median(flattened_samples[0])
    zeropoint_error = (np.percentile(flattened_samples[0], 84) - np.percentile(flattened_samples[0], 16)) / 2.0
    color_term = np.median(flattened_samples[1])
    color_error = (np.percentile(flattened_samples[1], 84) - np.percentile(flattened_samples[1], 16)) / 2.0

    return zeropoint, zeropoint_error, color_term, color_error


def to_magnitude(flux, flux_error, zeropoint, exptime):
    """
    Convert flux to magnitudes

    Parameters
    ----------
    flux: float array
          flux measurements in counts
    flux_error: float array
                flux errors in counts
    zeropoint: float
               zeropoint in units of counts / s
    exptime: float
             Exposure time of the image (s)

    Returns
    -------
    magnitude: float array
               converted magnitudes
    magnitude_errors: float array
                      uncertainties on the converted magnitudes calculated with standard uncertainty propagation
    """
    mag = -2.5 * np.log10(flux / exptime) + zeropoint
    mag_error = 2.5 / np.log(10.0) * np.abs(flux_error / flux)
    return mag, mag_error
