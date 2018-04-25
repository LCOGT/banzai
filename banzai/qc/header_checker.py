"""
This module performs basic sanity checks that the main image header keywords are the correct
format and validates their values.
@author:ebachelet
"""
from banzai.stages import Stage
from banzai import logs


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

            self.check_header_keyword_present(self.header_expected_format.keys(), image)
            # Remove for now, until we can re-evaluate what exactly the
            # expected formats should be. There could be multiple expected
            # formats per keyword.
            # self.check_header_format(keyword, image)
            self.check_header_na(self.header_expected_format.keys(), image)
            self.check_ra_range(image)
            self.check_dec_range(image)
            self.check_exptime_value(image)

        return images

    def check_header_keyword_present(self, keywords, image):
        """ Logs an error if the keyword is not in the image header.

        Parameters
        ----------
        keyword : str
                  the keyword of interest
        image : object
                a  banzais.image.Image object.

        """

        logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)
        missing_keywords = []
        for keyword in keywords:
            if keyword not in image.header:
                sentence = 'The header key ' + keyword + ' is not in image header!'
                missing_keywords.append(keyword)
                self.logger.error(sentence, extra=logging_tags)
        any_keywords_missing = True if len(missing_keywords) else False
        self.save_qc_results({"HeaderKeywordsMissing": any_keywords_missing,
                               "header_keywords_mising": missing_keywords}, image)

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

    def check_header_na(self, keywords, image):
        """ Logs an error if the keyword is 'N/A' instead of the
            expected type in the image header.

        Parameters
        ----------
        keyword : str
                  the keyword of interest
        image : object
                a  banzais.image.Image object.

        """
        logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)
        na_keywords = []
        for keyword in keywords:
            try:
                header_value = image.header[keyword]
                if isinstance(header_value, str):
                    if 'N/A' in header_value:
                        sentence = 'The header key ' + keyword + ' got the unexpected value : N/A'
                        self.logger.error(sentence, extra=logging_tags)
                        na_keywords.append(keyword)
            except:
                pass
        any_keywords_na = True if len(na_keywords) else False
        self.save_qc_results({"HeaderKeywordsNA": any_keywords_na,
                              "header_keywords_NA": na_keywords}, image)

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
        try:
            bad_ra_value = (ra_value > self.RA_MAX) | (ra_value < self.RA_MIN)
            if bad_ra_value:
                sentence = 'The header CRVAL1 key got the unexpected value : ' + str(ra_value)
                self.logger.error(sentence, extra=logging_tags)
            self.save_qc_results({"HeaderBadRAValue": bad_ra_value}, image)
            self.save_qc_results({"header_ra_value": ra_value}, image)
        except:
            pass


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
        try:
            bad_dec_value = (dec_value > self.DEC_MAX) | (dec_value < self.DEC_MIN)
            if bad_dec_value:
                sentence = 'The header CRVAL2 key got the unexpected value : ' + str(dec_value)
                self.logger.error(sentence, extra=logging_tags)
            self.save_qc_results({"HeaderBadDecValue": bad_dec_value}, image)
            self.save_qc_results({"header_dec_value": dec_value}, image)
        except:
            pass

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

        try:
            exptime_value = image.header['EXPTIME']
            self.save_qc_results({"header_exptime": exptime_value}, image)
        except:
            return

        try:
            exptime_negative = exptime_value < 0.0
            if exptime_negative:
                sentence = 'The header EXPTIME key got the unexpected value : negative value'
                self.logger.error(sentence, extra=logging_tags)
            self.save_qc_results({"HeaderExptimeNegative": exptime_negative}, image)
        except:
            pass

        try:
            exptime_zero = (exptime_value == 0.0) & (image.header['OBSTYPE'] == 'EXPOSE')
            if exptime_zero:
                sentence = 'The header EXPTIME key got the unexpected value : 0.0'
                self.logger.error(sentence, extra=logging_tags)
            self.save_qc_results({"HeaderExptimeZero": exptime_zero}, image)
        except:
            pass

