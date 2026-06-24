from banzai.query import cross_match_missing_frames


def test_cross_match_missing_frames():
    raw_frames = [
        {'basename': 'raw1', 'id': 1},
        {'basename': 'raw2', 'id': 2},
        {'basename': 'raw3', 'id': 3}
    ]
    reduced_frames = [{'related_frames': [1, 4]}, {'related_frames': [3]}]
    missing_frames = cross_match_missing_frames(raw_frames, reduced_frames)
    assert missing_frames == [{'basename': 'raw2', 'id': 2}]
