from tenacity import retry, wait_exponential, stop_after_attempt
import requests
from banzai.logs import get_logger


logger = get_logger()


@retry(wait=wait_exponential(multiplier=2, min=4, max=10), stop=stop_after_attempt(4), reraise=True)
def archive_get(url, params, auth_headers, timeout=30):
    try:
        response = requests.get(url, params=params, headers=auth_headers, timeout=timeout)
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        message = 'Error querying archive.'
        if int(response.status_code) == 429:
            message += ' Rate limited.'
        logger.error(message,
                     extra_tags={'attempt_number': archive_get.statistics['attempt_number']}
        )
        response.raise_for_status()
        raise
    except requests.exceptions.RequestException as e:
        message = "Archive download connection error when querying"
        logger.error(
            f"{message} {e}",
            extra_tags={
                'attempt_number': archive_get.statistics['attempt_number']
            }
        )
        raise
    return response


def frames_from_archive(start, end, obstype, reduction_level, runtime_context, raw=False, related_frames=False):
    archive_params = {'OBSTYPE': obstype, 'reduction_level': reduction_level, 'related_frames': related_frames}
    archive_params['start'] = start.strftime('%Y-%m-%d %H:%M')
    archive_params['end'] = end.strftime('%Y-%m-%d %H:%M')
    archive_params['limit'] = 1000
    if raw:
        frame_url = runtime_context.RAW_DATA_FRAME_URL
        auth_headers = runtime_context.RAW_DATA_AUTH_HEADER
    else:
        frame_url = runtime_context.ARCHIVE_FRAME_URL
        auth_headers = runtime_context.ARCHIVE_AUTH_HEADER

    more_frames = True
    response = archive_get(frame_url, params=archive_params, auth_headers=auth_headers)
    frames = response.json()['results']
    while more_frames:
        if response.json()['next'] is None:
            more_frames = False
        else:
            logger.debug(f"Getting more {obstype} frames. So far we have {len(frames)} frames.")
            response = archive_get(response.json()['next'], {}, auth_headers=auth_headers)
            frames += response.json()['results']
    return frames


def cross_match_missing_frames(raw_frames, reduced_frames):
    raw_frames_that_have_been_reduced = []
    for reduced_frame in reduced_frames:
        raw_frames_that_have_been_reduced += reduced_frame['related_frames']
    missing_raw_frames = []
    for raw_frame in raw_frames:
        if raw_frame['basename'] not in raw_frames_that_have_been_reduced:
            missing_raw_frames.append(raw_frame)
    return missing_raw_frames
