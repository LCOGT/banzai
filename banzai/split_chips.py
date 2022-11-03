from astropy.io import fits
import string


def split_chips(filename):
    """
    Splits a multiextension FITS file into separate files for each physical chip.

    The number of amps per chip is determined using N-AMPS-X * N-AMPS-Y / N-DET-X / N-DET-Y from the header. The
    original primary header is copied into each new file. A new keyword CHIP is added to the header, containing a
    single letter a-z. This is also appended to the filename.

    Parameters
    ----------
    filename : str
        The filename or path to the original multiextension FITS file.

    Returns
    -------
    image_paths : list
        A list of dictionaries: [{'path': 'chipa.fits'}, {'path': 'chipb.fits'}, ...]
    """
    hdulist = fits.open(filename)
    hdr = hdulist[0].header
    namps_per_chip = hdr['N-AMPS-X'] * hdr['N-AMPS-Y'] // hdr['N-DET-X'] // hdr['N-DET-Y']
    ext1 = 1
    image_paths = []
    while ext1 < len(hdulist):
        primary_hdu = hdulist[0].copy()
        chip_letter = string.ascii_lowercase[ext1 // namps_per_chip]
        primary_hdu.header['CHIP'] = chip_letter
        new_hdulist = fits.HDUList([primary_hdu, *hdulist[ext1:ext1+namps_per_chip]])
        new_filename = filename.replace('.fits', f'{chip_letter}.fits')
        new_hdulist.writeto(new_filename, overwrite=True)
        ext1 += namps_per_chip
        image_paths.append({'path': new_filename})
    return image_paths
