from include.generator.generate_events import _weighted_source

def test_weighted_source_returns_a_triple():
    result = _weighted_source()
    assert len(result) == 3
