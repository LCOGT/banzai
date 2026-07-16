import math

import numpy as np
from auto_stretch import apply_stretch
from PIL import Image


def stretch_for_display(data, max_size=1800):
    """Create a display-ready uint8 image from FITS image data.

    Decimation happens before both the float64 conversion and the stretch, because stretching a full big-sensor frame
    costs a full-size float64 copy, GB-scale memory, and seconds per arrival; stride-decimated previews are visually
    equivalent and finish in milliseconds.
    """
    data = np.asarray(data)
    longest_side = max(data.shape)
    stride = max(1, math.ceil(longest_side / max_size))
    display_data = np.asarray(data[::stride, ::stride], dtype=np.float64)

    finite_mask = np.isfinite(display_data)
    if not finite_mask.all():
        fill_value = np.median(display_data[finite_mask]) if finite_mask.any() else 0.0
        display_data = np.where(finite_mask, display_data, fill_value)

    stretched_data = apply_stretch(display_data)
    display_image = np.clip(stretched_data * 255.0, 0.0, 255.0).astype(np.uint8)
    return np.flipud(display_image)


def save_jpg(display_image, path, max_size, quality=85):
    """Save a display image as a grayscale JPEG, shrinking but never upscaling it.

    Parameters
    ----------
    display_image : numpy.ndarray
        Two-dimensional display-ready image data.
    path : str or pathlib.Path
        Output JPEG path.
    max_size : int
        Maximum width or height in pixels; PIL thumbnail only shrinks.
    quality : int, optional
        JPEG quality setting.

    Returns
    -------
    str or pathlib.Path
        The output path.
    """
    image = Image.fromarray(np.asarray(display_image, dtype=np.uint8))
    image.thumbnail((max_size, max_size))
    image.save(path, quality=quality)
    return path
