from tenacity import retry, wait_exponential, stop_after_attempt
import requests
from banzai.logs import get_logger


logger = get_logger()


@retry(wait=wait_exponential(multiplier=2, min=4, max=10), stop=stop_after_attempt(4), reraise=True)
def archive_get(url, params, auth_headers, timeout=30):
    """Query the LCO archive with an exponential backoff retry strategy that attempts to
       circumvent transient errors from the archive and logs the error if it cannot.

    Parameters
    ----------
    url: str
        The URL to query.
    params: dict
        The parameters to include in the query. Passthrough to requests.get.
    auth_headers: dict
        The authentication headers to include in the query. Passthrough to requests.get.
    timeout: int
        The timeout for the query in seconds. Passthrough to requests.get.

    Returns
    -------
    requests.Response
        The response from the archive query.
    """
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


def frames_from_archive(start, end, obstype, site, reduction_level, runtime_context, raw=False, related_frames=False):
    """Query the LCO archive for frames including pagination handling.

    Parameters
    ----------
    start: datetime
        The start time for the query.
    end: datetime
        The end time for the query.
    obstype: str
        The OBSTYPE of the frames to query for.
    site: str
        Site ID to query for.
    reduction_level: int
        The reduction level of the frames to query for.
    runtime_context: Context object
        The runtime context object containing configuration settings.
    raw: bool
        Whether to query the raw data archive or the reduced data archive. Defaults to False.
    related_frames: bool
        Whether to include related frames in the query. Defaults to False.

    Returns
    -------
    list of dicts:
        The frames returned from the archive query.
    """
    archive_params = {'OBSTYPE': obstype,
                      'reduction_level': reduction_level,
                      'include_related_frames': related_frames,
                      'SITEID': site}
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
    """
    Cross match a list of raw frames and reduced frames to find any missing raw frames.

    Parameters
    ----------
    raw_frames: list of dicts
        The raw frames to cross match. Each dict should have a 'basename' key.

    reduced_frames: list of dicts
        The reduced frames to cross match. Each dict should have a 'related_frames' key
        that contains a list of related raw frame basenames.
    Returns
    -------
    list of dicts
        The raw frames that are missing from the reduced frames.
    
    Notes
    -----
    The reduced frames list should include related frames in the query to create the list.
    """
    raw_frames_that_have_been_reduced = []
    for reduced_frame in reduced_frames:
        raw_frames_that_have_been_reduced += reduced_frame['related_frames']
    missing_raw_frames = []
    for raw_frame in raw_frames:
        if raw_frame['basename'] not in raw_frames_that_have_been_reduced:
            missing_raw_frames.append(raw_frame)
    return missing_raw_frames
