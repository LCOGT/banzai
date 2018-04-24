"""
This module performs basic sanity checks that the main image header keywords are the correct
format and validates their values.
@author:ebachelet
"""
from banzai.qc.qc_stage import QCStage
from banzai import logs


class HeaderSanity(QCStage):
    """
      Stage to validate important header keywords.
    """

    RA_MIN = 0.0
    RA_MAX = 360.0
    DEC_MIN = -90.0
    DEC_MAX = 90.0

    def __init__(self, pipeline_context):
        super(HeaderSanity, self).__init__(pipeline_context)

        self.header_expected_format = {'RA': str, 'DEC': str, 'CAT-RA': str, 'CAT-DEC': str,
                                       'OFST-RA': str, 'OFST-DEC': str, 'TPT-RA': str,
                                       'TPT-DEC': str, 'PM-RA': str, 'PM-DEC': str,
                                       'CRVAL1': float, 'CRVAL2': float, 'CRPIX1': int,
                                       'CRPIX2': int, 'EXPTIME': float}

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        """ Run stage to validate header.

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

            for keyword in self.header_expected_format.keys():
                self.check_header_keyword_present(keyword, image)
                # Remove for now, until we can re-evaluate what exactly the
                # expected formats should be. There could be multiple expected
                # formats per keyword.
                #self.check_header_format(keyword, image)
                self.check_header_na(keyword, image)

            self.check_ra_range(image)
            self.check_dec_range(image)
            self.check_exptime_value(image)

        return images

    def check_header_keyword_present(self, keyword, image):
        """ Logs an error if the keyword is not in the image header.

        Parameters
        ----------
        keyword : str
                  the keyword of interest
        image : object
                a  banzais.image.Image object.

        """
        header = image.header

        logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)
        keyword_missing = keyword not in header
        if keyword_missing:
            sentence = 'The header key ' + keyword + ' is not in image header!'
            self.logger.error(sentence, extra=logging_tags)
        self.save_qc_results({keyword + "_missing": keyword_missing}, image)

    def check_header_format(self, keyword, image):
        """ Logs an error if the keyword is not the expected type
            in the image header.

        Parameters
        ----------
        keyword : str
                  the keyword of interest
        image : object
                a  banzais.image.Image object.

        """

        header_value = image.header[keyword]

        logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)

        if not isinstance(header_value, self.header_expected_format[keyword]):
            sentence = ('The header key ' + keyword + ' got an unexpected format : ' + type(
                header_value).__name__ + ' in place of ' + self.header_expected_format[
                    keyword].__name__)

            self.logger.error(sentence, extra=logging_tags)

    def check_header_na(self, keyword, image):
        """ Logs an error if the keyword is 'N/A' instead of the
            expected type in the image header.

        Parameters
        ----------
        keyword : str
                  the keyword of interest
        image : object
                a  banzais.image.Image object.

        """

        header_value = image.header[keyword]

        logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)

        if isinstance(header_value, str):
            keyword_na = 'N/A' in header_value
            if keyword_na:
                sentence = 'The header key ' + keyword + ' got the unexpected value : N/A'
                self.logger.error(sentence, extra=logging_tags)
            self.save_qc_results({keyword + "_na": keyword_na}, image)

    def check_ra_range(self, image):
        """ Logs an error if the keyword right_ascension is not inside
            the expected range (0<ra<360 degrees) in the image header.

        Parameters
        ----------
        image : object
                a  banzais.image.Image object.

        """
        ra_value = image.header['CRVAL1']

        logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)

        if isinstance(ra_value, float):

            bad_ra_val = (ra_value > self.RA_MAX) | (ra_value < self.RA_MIN)
            if bad_ra_val:
                sentence = 'The header CRVAL1 key got the unexpected value : ' + str(ra_value)
                self.logger.error(sentence, extra=logging_tags)
            self.save_qc_results({"bad_RA_val": bad_ra_val}, image)


    def check_dec_range(self, image):
        """Logs an error if the keyword declination is not inside
            the expected range (-90<dec<90 degrees) in the image header.

        Parameters
        ----------
        image : object
                a  banzais.image.Image object.

        """

        dec_value = image.header['CRVAL2']

        logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)

        bad_dec_val = (dec_value > self.DEC_MAX) | (dec_value < self.DEC_MIN)
        if bad_dec_val:
            sentence = 'The header CRVAL2 key got the unexpected value : ' + str(dec_value)
            self.logger.error(sentence, extra=logging_tags)
        self.save_qc_results({"bad_dec_val": bad_dec_val}, image)

    def check_exptime_value(self, image):
        """Logs an error if :
        -1) the keyword exptime is not higher than 0.0
        -2) the keyword exptime is equal to 0.0 and 'OBSTYPE' keyword is 'EXPOSE'

        Parameters
        ----------
        image : object
                a  banzais.image.Image object.

        """

        logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)

        exptime_value = image.header['EXPTIME']
        exptime_negative = exptime_value < 0.0
        if exptime_negative:
            sentence = 'The header EXPTIME key got the unexpected value : negative value'
            self.logger.error(sentence, extra=logging_tags)
            self.save_qc_results({"exptime_negative": exptime_negative}, image)

        if 'OBSTYPE' in image.header:
            exptime_zero = (exptime_value == 0.0) & (image.header['OBSTYPE'] == 'EXPOSE')
            if exptime_zero:
                sentence = 'The header EXPTIME key got the unexpected value : 0.0'
                self.logger.error(sentence, extra=logging_tags)
                return
            self.save_qc_results({"exptime_zero": exptime_zero}, image)

