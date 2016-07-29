from banzai.stages import Stage
from banzai.utils import array_utils


class BPMUpdater(Stage):
    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        for image in images:
            bpm_slices = array_utils.array_indices_to_slices(image.bpm)
            image.bpm[image.data[bpm_slices] >= float(image.header['SATURATE'])] = 2
        return images
