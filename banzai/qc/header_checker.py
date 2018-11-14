"""
This module performs basic sanity checks that the main image header keywords are the correct
format and validates their values.
@author:ebachelet
"""
import logging

from banzai.stages import Stage

logger = logging.getLogger(__name__)


class HeaderSanity(Stage):
    """
    Stage to validate important header keywords.
    """

    RA_MIN = 0.0
    RA_MAX = 360.0
    DEC_MIN = -90.0
    DEC_MAX = 90.0

    def __init__(self, pipeline_context):
        super(HeaderSanity, self).__init__(pipeline_context)

        self.expected_header_keywords = ['RA', 'DEC', 'CAT-RA', 'CAT-DEC',
                                         'OFST-RA', 'OFST-DEC', 'TPT-RA',
                                         'TPT-DEC', 'PM-RA', 'PM-DEC',
                                         'CRVAL1', 'CRVAL2', 'CRPIX1',
                                         'CRPIX2', 'EXPTIME']

    def do_stage(self, images):
        """
        Run stage to validate header.

        Parameters
        ----------
        images : list
                 a list of banzais.image.Image object.

        Returns
        -------
        images: list
                the list of validated images object after header check

       """
        for image in images:
            logger.info("Checking header sanity.", image=image)
            bad_keywords = self.check_keywords_missing_or_na(image)
            self.check_ra_range(image, bad_keywords)
            self.check_dec_range(image, bad_keywords)
            self.check_exptime_value(image, bad_keywords)
        return images

    def check_keywords_missing_or_na(self, image):
        """
        Logs an error if the keyword is missing or 'N/A' (the default placeholder value).

        Parameters
        ----------
        image : object
                a  banzais.image.Image object.

        Returns
        -------
        bad_keywords: list
                a list of any keywords that are missing or NA

        Notes
        -----
        Some header keywords for bias and dark frames (e.g., 'OFST-RA') are excpted to be non-valued,
        but the 'N/A' placeholder values should be overwritten by 'NaN'.

        """
        qc_results = {}
        missing_keywords = []
        na_keywords = []
        for keyword in self.expected_header_keywords:
            if keyword not in image.header:
                sentence = 'The header key {0} is not in image header!'.format(keyword)
                logger.error(sentence, image=image)
                missing_keywords.append(keyword)
            elif image.header[keyword] == 'N/A':
                sentence = 'The header key {0} got the unexpected value : N/A'.format(keyword)
                logger.error(sentence, image=image)
                na_keywords.append(keyword)
        are_keywords_missing = len(missing_keywords) > 0
        are_keywords_na = len(na_keywords) > 0
        qc_results["header.keywords.missing.failed"] = are_keywords_missing
        qc_results["header.keywords.na.failed"] = are_keywords_na
        if are_keywords_missing:
            qc_results["header.keywords.missing.names"] = missing_keywords
        if are_keywords_na:
            qc_results["header.keywords.na.names"] = na_keywords
        self.save_qc_results(qc_results, image)
        return missing_keywords + na_keywords

    def check_ra_range(self, image, bad_keywords=None):
        """
        Logs an error if the keyword right_ascension is not inside
        the expected range (0<ra<360 degrees) in the image header.

        Parameters
        ----------
        image : object
                a  banzais.image.Image object.
        bad_keywords: list
                a list of any keywords that are missing or NA

        """
        if bad_keywords is None:
            bad_keywords = []
        if 'CRVAL1' not in bad_keywords:
            ra_value = image.header['CRVAL1']
            is_bad_ra_value = (ra_value > self.RA_MAX) | (ra_value < self.RA_MIN)
            if is_bad_ra_value:
                sentence = 'The header CRVAL1 key got the unexpected value : {0}'.format(ra_value)
                logger.error(sentence, image=image)
            self.save_qc_results({"header.ra.failed": is_bad_ra_value,
                                  "header.ra.value": ra_value}, image)

    def check_dec_range(self, image, bad_keywords=None):
        """
        Logs an error if the keyword declination is not inside
        the expected range (-90<dec<90 degrees) in the image header.

        Parameters
        ----------
        image : object
                a  banzais.image.Image object.
        bad_keywords: list
                a list of any keywords that are missing or NA

        """
        if bad_keywords is None:
            bad_keywords = []
        if 'CRVAL2' not in bad_keywords:
            dec_value = image.header['CRVAL2']
            is_bad_dec_value = (dec_value > self.DEC_MAX) | (dec_value < self.DEC_MIN)
            if is_bad_dec_value:
                sentence = 'The header CRVAL2 key got the unexpected value : {0}'.format(dec_value)
                logger.error(sentence, image=image)
            self.save_qc_results({"header.dec.failed": is_bad_dec_value,
                                  "header.dec.value": dec_value}, image)

    def check_exptime_value(self, image, bad_keywords=None):
        """
        Logs an error if OBSTYPE is not BIAS and EXPTIME <= 0

        Parameters
        ----------
        image : object
                a  banzais.image.Image object.
        bad_keywords: list
                a list of any keywords that are missing or NA
        """
        if bad_keywords is None:
            bad_keywords = []
        if 'EXPTIME' not in bad_keywords and 'OBSTYPE' not in bad_keywords:
            exptime_value = image.header['EXPTIME']
            qc_results = {"header.exptime.value": exptime_value}
            if image.header['OBSTYPE'] != 'BIAS':
                is_exptime_null = exptime_value <= 0.0
                if is_exptime_null:
                    sentence = 'The header EXPTIME key got the unexpected value {0}:' \
                               'null or negative value'.format(exptime_value)
                    logger.error(sentence, image=image)
                qc_results["header.exptime.failed"] = is_exptime_null
            self.save_qc_results(qc_results, image)
