import logging

from banzai.stages import Stage

logger = logging.getLogger(__name__)


class GainNormalizer(Stage):
    def __init__(self, pipeline_context):
        super(GainNormalizer, self).__init__(pipeline_context)

    def do_stage(self, image):

        logger.info('Multiplying by gain', image=image, extra_tags={'gain': image.gain})

        gain = image.gain
        if validate_gain(gain):
            logger.error('Gain missing. Rejecting image.', image=image)
            return None

        if image.data_is_3d():
            for i in range(image.get_n_amps()):
                image.data[i] *= gain[i]
            image.header['SATURATE'] *= min(gain)
            image.header['MAXLIN'] *= min(gain)
        else:
            image.data *= image.gain
            image.header['SATURATE'] *= image.gain
            image.header['MAXLIN'] *= image.gain

        image.gain = 1.0
        image.header['GAIN'] = 1.0

        return image


def validate_gain(gain):
    """
    Validate the gain in the image

    Parameters
    ----------
    gain: float or list of floats
          gain value(s)

    Returns
    -------
    missing: boolean
             True if gain is missing or invalid
    """
    missing = True
    if not gain:
        return missing

    try:
        missing = not all(gain)
    except TypeError:
        missing = False

    return missing
