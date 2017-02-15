"""
This module performs controls on several critical parameters of image header, contains in
header_expected_format dictionnary.
@author:ebachelet
"""
from banzai.stages import Stage
from banzai import logs

class HeaderSanity(Stage):
    """
       This class contains the needed function/definitions to performs the header check.
    """

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
        """ Performs several checks on the header, see the relative functions.
        :param list images: a list of image object.
        :returns: the list of image object after check
        :rtype: list
        """
        for image in images:


            for keyword in self.header_expected_format.keys():
                self.check_header_keyword_present(keyword, image)
                self.check_header_format(keyword, image)
                self.check_header_na(keyword, image)

            self.check_ra_range(image)
            self.check_dec_range(image)
            self.check_exptime_value(image)

        return images

    def check_header_keyword_present(self, keyword, image):
        """ Return a warning if the keyword is not in the image header.
        :param str keyword: the stry keyword you look inside the header
        :param object image: an image object
        """
        header = image.header

        logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)

        if keyword not in header:
            sentence = 'WARNING : The header key ' + keyword + ' is not in image header!'

            self.logger.error(sentence, extra=logging_tags)

            return 'WARNING'
        return

    def check_header_format(self, keyword, image):
        """ Return a warning if the keyword is not the expected type
            in the image header.
        :param str keyword: the stry keyword you look inside the header
        :param object image: an image object
        """
        header_value = image.header[keyword]

        logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)

        if not isinstance(header_value, self.header_expected_format[keyword]):
            sentence = ('The header key ' + keyword + ' got an unexpected format : ' + type(
                header_value).__name__ + ' in place of ' + self.header_expected_format[
                    keyword].__name__)

            self.logger.error(sentence, extra=logging_tags)

        return

    def check_header_na(self, keyword, image):
        """ Return a warning if the keyword is 'N/A' instead of the
            expected type in the image header.
        :param str keyword: the stry keyword you look inside the header
        :param object image: an image object
        """
        header_value = image.header[keyword]

        logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)

        if isinstance(header_value, str):

            if 'N/A' in header_value:
                sentence = 'The header key ' + keyword + ' got the unexpected value : N/A'
                self.logger.error(sentence, extra=logging_tags)

        return

    def check_ra_range(self, image):
        """ Return an error if the keyword right_ascension is not inside
            the expected range (0<ra<360 degrees) in the image header.
        :param object image: an image object
        """
        ra_value = image.header['CRVAL1']

        logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)

        if isinstance(ra_value, float):

            if (ra_value > 360) | (ra_value < 0):
                sentence = 'The header CRVAL1 key got the unexpected value : ' + str(ra_value)
                self.logger.error(sentence, extra=logging_tags)

        return

    def check_dec_range(self, image):
        """ Return an error if the keyword declination is not inside
            the expected range (-90<ra<90 degrees) in the image header.
        :param object image: an image object
        """
        dec_value = image.header['CRVAL2']

        logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)

        if (dec_value > 90) | (dec_value < -90):
            sentence = 'The header CRVAL2 key got the unexpected value : ' + str(dec_value)
            self.logger.error(sentence, extra=logging_tags)

        return

    def check_exptime_value(self, image):
        """ Return an error if :
		-1) the keyword exptime is not higher than 0.0
		-2) the keyword exptime is equal to 0.0 and 'OBSTYPE' keyword is 'EXPOSE'
            if the OBSTYPE is expose.
        :param object image: an image object
        """
        exptime_value = image.header['EXPTIME']

        logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)

        if exptime_value < 0.0:
            sentence = 'The header EXPTIME key got the unexpected value : negative value'
            self.logger.error(sentence, extra=logging_tags)

            return

        obstype = image.header['OBSTYPE']
        if (exptime_value == 0.0) & (obstype == 'EXPOSE'):
            sentence = 'The header EXPTIME key got the unexpected value : 0.0'
            self.logger.error(sentence, extra=logging_tags)

            return
