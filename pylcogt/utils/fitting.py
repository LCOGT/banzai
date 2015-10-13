from __future__ import absolute_import, print_function, division

__author__ = 'cmccully'


from astropy import modeling
from statsmodels import robust
import numpy as np

from . import stats

# Iterative reweighting, least squares
def irls(x, data, errors, model, tol=1e-6, robust_norm=robust.norms.AndrewWave(),
         maxiter=10):
    # Fit using the Levenberg-Marquardt algorithm
    # This method can fit non-linear functions and does not require derivatives.
    fitter = modeling.fitting.LevMarLSQFitter()

    scatter = errors
    # Do an initial fit of the model
    # Use 1 / sigma^2 as weights
    weights = (errors ** -2.0).flatten()

    fitted_model = fitter(model, x, data, weights=weights)

    not_converged=True
    last_chi = np.inf
    iter = 0
    # Until converged
    while not_converged:
        # Update the weights
        residuals = data - fitted_model(x)

        # Save the chi^2 to check for convergence
        chi = ((residuals / scatter) ** 2.0).sum()

        # update the scaling (the MAD of the residuals)
        scatter = stats.median_absolute_deviation(residuals)
        # Convert to standard deviation
        scatter *= 1.4826
        weights = robust_norm.weights(residuals / scatter).flatten()

        # refit
        fitted_model = fitter(model, x, data, weights=weights)

        # Converged when the change in the chi^2 (or l2 norm or whatever) is
        # less than the tolerance. Hopefully this should converge quickly.
        if iter >= maxiter or np.abs(chi - last_chi) < tol:
            not_converged = False
        else:
            last_chi = chi
            iter += 1

    return fitted_model
