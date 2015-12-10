import os

from .. import dbs

__author__ = 'cmccully'

def make_output_directory(pipeline_context, image):
    # Get the telescope from the image
    telescope = dbs.get_telescope(image)
    # Create output directory if necessary
    output_directory = os.path.join(pipeline_context.processed_path, telescope.site,
                                    telescope.instrument, image.dayobs)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    return