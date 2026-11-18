import math

import numpy as np
from PIL import Image


# The look of a finished stack (PixInsight's screen transfer function defaults): sky at a quarter of full scale, and
# a black point this many average deviations below it.
TARGET_BACKGROUND = 0.25
SHADOWS_CLIP = 1.25


def midtones_transfer(midtone, x):
    """PixInsight midtones transfer function: maps 0 to 0, ``midtone`` to 0.5 and 1 to 1."""
    return (midtone - 1) * x / ((2 * midtone - 1) * x - midtone)


def stretch_stack(data, progress=1.0):
    """Stretch linear image data to [0, 1] for display.

    The stretch is a midtones curve with black just below the sky level, the sky at a quarter of full scale, and
    the brightest pixel at white. Only ratios in the data matter, so a sum and a mean of the same subs look alike.

    ``progress`` is the fraction of a stack's planned subs that ``data`` holds. Below 1, the image gets the stretch
    its finished stack should get but only that fraction of the light, so previews start dark and brighten into the
    finished image. At 1 or more it has no effect.
    """
    progress = min(progress, 1.0)
    sky = np.median(data)
    average_deviation = np.mean(np.abs(data - sky))
    # Noise falls as the square root of the subs collected, so this is where the finished stack's black point lands.
    black = sky - SHADOWS_CLIP * average_deviation * math.sqrt(progress)
    span = data.max() - black
    if average_deviation == 0 or span <= 0:
        return np.zeros_like(data)

    midtone = midtones_transfer(TARGET_BACKGROUND, (sky - black) / span)
    collected = np.clip(progress * (data - black) / span, 0.0, 1.0)
    return np.clip(midtones_transfer(midtone, collected), 0.0, 1.0)


def block_average(data, max_size):
    """Shrink an image by a whole-number factor so its longest side is at most ``max_size``, averaging each block.

    Averaging uses every pixel, so faint detail and small stars survive the shrink. The blocks are a view of
    ``data``, so a big-sensor frame is not copied. Rows and columns that do not fill a block are dropped.
    """
    factor = max(1, math.ceil(max(data.shape) / max_size))
    factor = min(factor, min(data.shape))  # keep at least one block along a very short side
    rows = data.shape[0] // factor * factor
    columns = data.shape[1] // factor * factor
    blocks = data[:rows, :columns].reshape(rows // factor, factor, columns // factor, factor)
    return blocks.mean(axis=3, dtype=np.float64).mean(axis=1)


def stretch_for_display(data, max_size=1800, progress=1.0):
    """Create a display-ready uint8 image from FITS image data.

    The image is shrunk before the stretch, because stretching a full big-sensor frame costs a full-size float64
    copy and GB-scale memory on every arrival.

    Parameters
    ----------
    data : numpy.ndarray
        Two-dimensional image data.
    max_size : int, optional
        Longest side of the stretched image, in pixels, before any JPEG resizing.
    progress : float, optional
        Fraction of the stack's planned subs that ``data`` contains. At 1 or more the image gets the finished-stack
        stretch; below 1 it is shown as a partly built exposure (see ``stretch_stack``).
    """
    display_data = block_average(np.asarray(data), max_size)

    finite_mask = np.isfinite(display_data)
    if not finite_mask.all():
        fill_value = np.median(display_data[finite_mask]) if finite_mask.any() else 0.0
        display_data = np.where(finite_mask, display_data, fill_value)

    stretched_data = stretch_stack(display_data, progress)
    display_image = np.clip(stretched_data * 255.0, 0.0, 255.0).astype(np.uint8)
    return np.flipud(display_image)


def save_jpg(display_image, path, max_size, quality=85):
    """Save a display image as a grayscale JPEG, shrinking but never upscaling it.

    Parameters
    ----------
    display_image : numpy.ndarray
        Two-dimensional uint8 image, as ``stretch_for_display`` returns.
    path : str or pathlib.Path
        Output JPEG path.
    max_size : int
        Maximum width or height in pixels; PIL thumbnail only shrinks.
    quality : int, optional
        JPEG quality setting.
    """
    image = Image.fromarray(display_image)
    image.thumbnail((max_size, max_size))
    image.save(path, quality=quality)
