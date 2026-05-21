from banzai.query import cross_match_missing_frames


def test_cross_match_missing_frames():
    raw_frames = [{'basename': 'raw1'}, {'basename': 'raw2'}, {'basename': 'raw3'}]
    reduced_frames = [{'related_frames': ['raw1', 'raw4']}, {'related_frames': ['raw3']}]
    missing_frames = cross_match_missing_frames(raw_frames, reduced_frames)
    assert missing_frames == [{'basename': 'raw2'}]
