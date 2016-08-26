from banzai.stages import Stage
from banzai import logs

import os


class HeaderSanity(Stage):
    """
    Check the header of the image.

    """

    def __init__(self, pipeline_context):
        super(HeaderSanity, self).__init__(pipeline_context)

        self.Header_expected_format = {'RA': str, 'DEC': str, 'CAT-RA': str, 'CAT-DEC': str, 'OFST-RA': str,
                                       'OFST-DEC': str, 'TPT-RA': str, 'TPT-DEC': str, 'PM-RA': str, 'PM-DEC': str,
                                       'CRVAL1': float, 'CRVAL2': float, 'CRPIX1': int, 'CRPIX2': int,
                                       'EXPTIME': float}

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):

        for image in images:


            for keyword in self.Header_expected_format.keys():
                self.check_header_keyword_present(keyword, image)
                self.check_header_format(keyword, image)
                self.check_header_NA(keyword, image)

            self.check_RA_range(image)
            self.check_DEC_range(image)
            self.check_EXPTIME_value(image)

        return images

    def check_header_keyword_present(self, keyword, image):

        header = image.header

        logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)

        if keyword not in header:
            sentence = 'WARNING : The header key ' + keyword + ' is not in image header!'

            self.logger.error(sentence, extra=logging_tags)

            return 'WARNING'
        return

    def check_header_format(self, keyword, image):

        header_value = image.header[keyword]

        logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)

        if not isinstance(header_value, self.Header_expected_format[keyword]):
            sentence = 'The header key ' + keyword + ' got an unexpected format : ' + type(header_value).__name__ + \
                       ' in place of ' + self.Header_expected_format[keyword].__name__

            self.logger.error(sentence, extra=logging_tags)

        return

    def check_header_NA(self, keyword, image):

        header_value = image.header[keyword]

        logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)

        if isinstance(header_value, str):

            if ('N/A' in header_value):
                sentence = 'The header key ' + keyword + ' got the unexpected value : N/A'
                self.logger.error(sentence, extra=logging_tags)

        return

    def check_RA_range(self, image):

        RA_value = image.header['CRVAL1']

        logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)

        if isinstance(RA_value, float):

            if (RA_value > 360) | (RA_value < 0):
                sentence = 'The header CRVAL1 key got the unexpected value : ' + str(RA_value)
                self.logger.error(sentence, extra=logging_tags)

        return

    def check_DEC_range(self, image):

        DEC_value = image.header['CRVAL2']

        logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)

        if (DEC_value > 90) | (DEC_value < -90):
            sentence = 'The header CRVAL2 key got the unexpected value : ' + str(DEC_value)
            self.logger.error(sentence, extra=logging_tags)

        return

    def check_EXPTIME_value(self, image):

        EXPTIME_value = image.header['EXPTIME']

        logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)

        if EXPTIME_value < 0.0:
            sentence = 'The header EXPTIME key got the unexpected value : negative value'
            self.logger.error(sentence, extra=logging_tags)

            return

        OBSTYPE = image.header['OBSTYPE']
        if (EXPTIME_value == 0.0) & (OBSTYPE == 'EXPOSE'):
            sentence = 'The header EXPTIME key got the unexpected value : 0.0'
            self.logger.error(sentence, extra=logging_tags)

            return
