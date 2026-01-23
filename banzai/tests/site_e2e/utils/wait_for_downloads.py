"""
Utilities for waiting on calibration file downloads.
"""
import logging
import os
import time

logger = logging.getLogger(__name__)


def wait_for_downloads(cache_dir: str, expected_files: list[str], timeout: int = 120) -> bool:
    """
    Wait for calibration files to be downloaded to cache directory.

    Polls the cache directory until all expected files exist.

    Args:
        cache_dir: Path to calibration cache directory
        expected_files: List of filenames to wait for
        timeout: Maximum seconds to wait

    Returns:
        True if all files exist within timeout, False otherwise.
    """
    start_time = time.time()
    poll_interval = 5

    logger.info(f"Waiting for {len(expected_files)} files to be downloaded to {cache_dir}...")

    while time.time() - start_time < timeout:
        missing_files = []

        for filename in expected_files:
            filepath = os.path.join(cache_dir, filename)
            if not os.path.exists(filepath):
                missing_files.append(filename)

        if not missing_files:
            logger.info(f"All {len(expected_files)} expected files are present")
            return True

        elapsed = int(time.time() - start_time)
        found_count = len(expected_files) - len(missing_files)
        logger.info(
            f"Found {found_count}/{len(expected_files)} files, "
            f"missing {len(missing_files)}: {missing_files[:3]}{'...' if len(missing_files) > 3 else ''} "
            f"({elapsed}s elapsed)"
        )

        time.sleep(poll_interval)

    logger.error(f"Timeout waiting for downloads after {timeout} seconds")
    logger.error(f"Still missing: {missing_files}")
    return False
