import os
import logging

from banzai import dbs, settings
from banzai.utils import date_utils, file_utils, fits_utils
from banzai import munge
from astropy.io import fits
import tempfile

logger = logging.getLogger('banzai')


class ObservationalFrame:
    def __init__(self, primary_hdu, image_hdus):
        self.image_hdus = image_hdus
        self.primary_hdu = primary_hdu

    def __iadd__(self, other):
        self.primary_hdu.data += other

    @property
    def meta(self):
        return self.primary_hdu.meta

    @classmethod
    def open(cls, filename):
        return cls(primary_hdu, [hdu for hdu in read_image_hdus(filename)])

class MasterCalibrationImage(ObservationalFrame):
    # Sort the images by reverse observation date, so that the most recent one
    # is used to create the filename and select the day directory
    images.sort(key=lambda image: image.dateobs, reverse=True)
    make_calibration_name = file_utils.make_calibration_filename_function(self.calibration_type,
                                                                          self.runtime_context)
    master_calibration_filename = make_calibration_name(images[0])
    master_header = create_master_calibration_header(images[0].header, images)
    master_image = FRAME_CLASS(self.runtime_context, data=stacked_data, header=master_header)
    master_image.filename = master_calibration_filename
    master_image.bpm = master_bpm
    master_bpm = np.array(stacked_data == 0.0, dtype=np.uint8)

    def create_master_calibration_header(old_header, images):
        header = fits.Header()
        for key in old_header.keys():
            try:
                # Dump empty header keywords and ignore old histories.
                if len(key) > 0 and key != 'HISTORY' and key != 'COMMENT':
                    for i in range(old_header.count(key)):
                        header[key] = (old_header[(key, i)], old_header.comments[(key, i)])
            except ValueError as e:
                logger.error('Could not add keyword {key}: {error}'.format(key=key, error=e))
                continue
        header = fits_utils.sanitizeheader(header)
        observation_dates = [image.dateobs for image in images]
        mean_dateobs = date_utils.mean_date(observation_dates)

        header['DATE-OBS'] = (date_utils.date_obs_to_string(mean_dateobs), '[UTC] Mean observation start time')
        header['ISMASTER'] = (True, 'Is this a master calibration frame')

        for i, image in enumerate(images):
            header['IMCOM{:03d}'.format(i + 1)] = image.filename, 'Image combined to create master calibration image'
        return header


class CCDData:
    def __init__(self, data, meta, mask=None, uncertainty=None):
        self.data = data
        self.meta = meta
        self.mask = mask
        self.uncertainty = uncertainty

        # TODO raise an exception if gain is not valid
        # TODO: Log a warning if header keywords are missing

        # TODO: On setting the mask, raise an exception if it is the wrong size
        # TODO: on load, check, header saturation, 1000's etc. Basically anything that makes the data dead on arrival

        # TODO: add a bias_level ? subtract_bias_level, HEADER keyword('Mean bias level of master bias')
        # TODO: add a save status method?
        #master_bias_filename = os.path.basename(master_calibration_image.filename)
        #image.header['L1IDBIAS'] = (master_bias_filename, 'ID of bias frame')
        #image.header['L1STATBI'] = (1, "Status flag for bias frame correction")
        # image.calibration_status['overscan'] = int(bias_region is None), 'Status flag for overscan correction')
        # TODO: on write fill in any of the missing status keywords:
        # TODO: add subtract_overscan?
        # TODO: add trim
        # TODO: load 3d data cubes into multiple extensions on creation
        # TODO: add a copy_to function
        # TODO: primary hdu setter needs to add primary data to image extension if the ccddata is an image type
    def __iadd__(self, other):
        self.data += other

    def __isub__(self, other):
        self.data -= other

    def __imul__(self, other):
        self.data *= other
        self.meta['SATURATE'] *= other
        self.meta['GAIN'] *= other
        self.meta['MAXLIN'] *= other

    def __idiv__(self, other):
        self.data /= other
        self.meta['SATURATE'] /= other
        self.meta['GAIN'] /= other
        self.meta['MAXLIN'] /= other

    @property
    def gain(self):
        return self.meta['GAIN']

    @gain.setter
    def gain(self, value):
        self.meta['GAIN'] = value

    @property
    def saturate(self):
        return self.meta['SATURATE']

    @saturate.setter
    def saturate(self, value):
        self.meta['SATURATE'] = value

    def get_region(self, region_name):
        pass

    def _trim_image(region):
        trimsec = fits_utils.parse_region_keyword(image.header['TRIMSEC'])

        if trimsec is not None:
            image.data = image.data[trimsec]
            image.bpm = image.bpm[trimsec]

            # Update the NAXIS and CRPIX keywords
            image.header['NAXIS1'] = trimsec[1].stop - trimsec[1].start
            image.header['NAXIS2'] = trimsec[0].stop - trimsec[0].start
            if 'CRPIX1' in image.header:
                image.header['CRPIX1'] -= trimsec[1].start
            if 'CRPIX2' in image.header:
                image.header['CRPIX2'] -= trimsec[0].start

            image.header['L1STATTR'] = (1, 'Status flag for overscan trimming')
        else:
            logger.warning('TRIMSEC was not defined.', image=image, extra_tags={'trimsec': image.header['TRIMSEC']})
            image.header['L1STATTR'] = (0, 'Status flag for overscan trimming')
        return image.header['NAXIS1'], image.header['NAXIS2']


    def get_mosaic_size(image, n_amps):
        """
        Get the necessary size of the output mosaic image

        Parameters
        ----------
        image: banzai.images.Image
               image (with extensions) to mosaic
        n_amps: int
                number of amplifiers (fits extensions) in the image

        Returns
        -------
        nx, ny: int, int
                The number of pixels in x and y that needed for the output mosaic.

        Notes
        -----
        Astropy fits data arrays are indexed y, x.
        """
        ccdsum = image.ccdsum.split(' ')
        x_pixel_limits, y_pixel_limits = get_detsec_limits(image, n_amps)
        nx = (np.max(x_pixel_limits) - np.min(x_pixel_limits) + 1) // int(ccdsum[0])
        ny = (np.max(y_pixel_limits) - np.min(y_pixel_limits) + 1) // int(ccdsum[1])
        return nx, ny


    def copy_to():
        mosaiced_data = np.zeros((ny, nx), dtype=np.float32)
        mosaiced_bpm = np.zeros((ny, nx), dtype=np.uint8)
        x_detsec_limits, y_detsec_limits = get_detsec_limits(image, image.get_n_amps())
        xmin = min(x_detsec_limits) - 1
        ymin = min(y_detsec_limits) - 1
        for i in range(image.get_n_amps()):
            ccdsum = image.extension_headers[i].get('CCDSUM', image.ccdsum)
            x_binning, y_binning = ccdsum.split(' ')
            datasec = image.extension_headers[i]['DATASEC']
            amp_slice = fits_utils.parse_region_keyword(datasec)

            detsec = image.extension_headers[i]['DETSEC']
            mosaic_slice = get_windowed_mosaic_slices(detsec, xmin, ymin, x_binning, y_binning)


            mosaiced_data[mosaic_slice] = image.data[i][amp_slice]
            mosaiced_bpm[mosaic_slice] = image.bpm[i][amp_slice]

        image.data = mosaiced_data
        image.bpm = mosaiced_bpm
        # Flag any missing data
        image.bpm[image.data == 0.0] = 1
        image.update_shape(nx, ny)
        update_naxis_keywords(image, nx, ny)
class ImageData(CCDData):
    # TODO: get requested and central coordinates (Image subclass?)
    pass


class Image(object):

    def __init__(self, runtime_context, filename):
        self._hdu_list = fits_utils.init_hdu()

        # Amplifier specific (in principle)
        self.data, self.header, extensions = fits_utils.open_image(filename)
        self.readnoise = float(self.header.get('RDNOISE', 0.0))
        if len(self.extension_headers) > 0 and 'GAIN' in self.extension_headers[0]:
                self.gain = [h['GAIN'] for h in self.extension_headers]
        else:
            self.gain = eval(str(self.header.get('GAIN')))
        self.ccdsum = self.header.get('CCDSUM')
        self.nx = self.header.get('NAXIS1')
        self.ny = self.header.get('NAXIS2')

        # Observation specific
        self.filename = os.path.basename(filename)
        self.request_number = self.header.get('REQNUM')
        self.instrument = dbs.get_instrument(runtime_context)
        self.epoch = str(self.header.get('DAY-OBS'))
        self.configuration_mode = fits_utils.get_configuration_mode(self.header)
        self.block_id = self.header.get('BLKUID')
        self.block_start = date_utils.parse_date_obs(self.header.get('BLKSDATE', '1900-01-01T00:00:00.00000'))
        self.molecule_id = self.header.get('MOLUID')
        self.obstype = self.header.get('OBSTYPE')
        self.dateobs = date_utils.parse_date_obs(self.header.get('DATE-OBS', '1900-01-01T00:00:00.00000'))
        self.datecreated = date_utils.parse_date_obs(self.header.get('DATE', date_utils.date_obs_to_string(self.dateobs)))
        self.exptime = float(self.header.get('EXPTIME', np.nan))

        # Imaging specific
        self.filter = self.header.get('FILTER')
        self.ra, self.dec = fits_utils.parse_ra_dec(self.header)
        self.pixel_scale = float(self.header.get('PIXSCALE', np.nan))
        munge.munge(self)

        # Calibrations?
        self.is_bad = False
        self.is_master = self.header.get('ISMASTER', False)
        self.attributes = settings.CALIBRATION_SET_CRITERIA.get(self.obstype, {})

        # What about NRES composite data products? Some extra extensions just need to go along for the ride

    def __del__(self):
        self._hdu_list.close()
        self._hdu_list._file.close()

    def write(self, runtime_context):
        file_utils.save_pipeline_metadata(self.header, runtime_context.rlevel)
        output_filename = file_utils.make_output_filename(self.filename, runtime_context.fpack, runtime_context.rlevel)
        output_directory = file_utils.make_output_directory(runtime_context.processed_path, self.instrument.site,
                                                            self.instrument.name, self.epoch,
                                                            preview_mode=runtime_context.preview_mode)
        filepath = os.path.join(output_directory, output_filename)
        fits_utils.write_fits_file(filepath, self._hdu_list, runtime_context)
        if self.obstype in settings.CALIBRATION_IMAGE_TYPES:
            dbs.save_calibration_info(filepath, self, db_address=runtime_context.db_address)
        if runtime_context.post_to_archive:
            file_utils.post_to_archive_queue(filepath, runtime_context.broker_url)

    def add_fits_extension(self, extension):
        self._hdu_list.append(extension)

    def update_shape(self, nx, ny):
        self.nx = nx
        self.ny = ny

    def data_is_3d(self):
        return len(self.data.shape) > 2

    def get_n_amps(self):
        if self.data_is_3d():
            n_amps = self.data.shape[0]
        else:
            n_amps = 1
        return n_amps

    def get_inner_image_section(self, inner_edge_width=0.25):
        """
        Extract the inner section of the image with dimensions:
        ny * inner_edge_width * 2.0 x nx * inner_edge_width * 2.0

        Parameters
        ----------

        inner_edge_width: float
                          Size of inner edge as fraction of total image size

        Returns
        -------
        inner_section: array
                       Inner section of image
        """
        if self.data_is_3d():
            logger.error("Cannot get inner section of a 3D image", image=self)
            raise ValueError

        inner_nx = round(self.nx * inner_edge_width)
        inner_ny = round(self.ny * inner_edge_width)
        return self.data[inner_ny: -inner_ny, inner_nx: -inner_nx]
