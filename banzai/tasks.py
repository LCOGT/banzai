from __future__ import absolute_import, division, print_function, unicode_literals

from banzai import dbs
from banzai import stages
from banzai import main
from banzai.utils import date_utils
import banzai
import logging
import os
from celery import Celery


logger = logging.getLogger('banzai')

app = Celery('tasks')
app.config_from_object('banzai.settings')


@app.task(bind=True, max_retries=3, default_retry_delay=3 * 60)
def reduce_preview_image(path, pipeline_context):
    if dbs.need_to_make_preview(path, db_address=pipeline_context.db_address,
                                max_tries=pipeline_context.max_preview_tries):
        stages_to_do = stages.get_stages_todo()

        logging_tags = {'tags': {'filename': os.path.basename(path)}}
        logger.info('Running preview reduction on {}'.format(path), extra=logging_tags)
        pipeline_context.filename = os.path.basename(path)
        pipeline_context.raw_path = os.path.dirname(path)

        # Increment the number of tries for this file
        dbs.increment_preview_try_number(path, db_address=pipeline_context.db_address)
        try:
            output_files = banzai.main.run(stages_to_do, pipeline_context,
                                           image_types=['EXPOSE', 'STANDARD'])
            if len(output_files) > 0:
                dbs.set_preview_file_as_processed(path, db_address=pipeline_context.db_address)
            else:
                logging_tags = {'tags': {'filename': os.path.basename(path)}}
                logger.error("Could not produce preview image. {0}".format(path),
                             extra=logging_tags)
        except Exception as e:
            logging_tags = {'tags': {'filename': os.path.basename(path)}}
            logger.error("Exception producing preview frame. {0}. {1}".format(e, path),
                         extra=logging_tags)


@app.task
def reduce_end_of_night(site, pipeline_context, instrument=None, dayobs=None):

    if instrument is None:
        telescopes = dbs.get_schedulable_telescopes(site, db_address=pipeline_context.db_address)
    else:
        telescopes = [dbs.get_telescope(site, instrument, db_address=pipeline_context.db_address)]

    if dayobs is None:
        timezone = dbs.get_timezone(site, db_address=pipeline_context.db_address)
        dayobs = date_utils.get_dayobs(timezone=timezone)

    for telescope in telescopes:
        pipeline_context.raw_path = os.path.join(pipeline_context.raw_path_root, site,
                                                 telescope.instrument, dayobs, 'raw')
        try:
            # Run the reductions on the given dayobs
            main.make_master_bias(pipeline_context)
            main.make_master_dark(pipeline_context)
            main.make_master_flat(pipeline_context)
            main.reduce_science_frames(pipeline_context)
            main.reduce_trailed_frames(pipeline_context)

        except Exception as e:
            logger.error(e)
