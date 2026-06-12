"""Shared pytest fixtures.

The main job here is to keep workflow-generation tests off the network.
``generate_dataset_workflow`` introspects the source file by shelling out to
``ogrinfo`` (via ``_count_source_features`` and ``_detect_geometry_type``).
Most tests pass a remote fixture URL, so those calls reach across the network;
when the NRP endpoint is slow they hang past the 10 s pytest timeout and flake
CI (PRs #109–#111 each burned reruns on exactly this). Stub both with
deterministic defaults so generation logic is exercised without any I/O.
"""

import pytest


@pytest.fixture(autouse=True)
def stub_source_introspection(request, monkeypatch):
    """Replace the network-touching source-introspection helpers with stubs.

    Tests that need a specific feature count already re-patch
    ``_count_source_features`` in their own body, and that override wins
    (it runs after this fixture). No test exercises the real detectors, so
    deterministic defaults are safe; a test can still opt back into the real
    implementations with ``@pytest.mark.realnetwork``.
    """
    if request.node.get_closest_marker("realnetwork"):
        return
    try:
        import cng_datasets.k8s.workflows as wf
    except Exception:
        return
    # 5 matches the canonical test fixture (test-fixture.gpkg, 5 features), so
    # the chunking-math tests that count it get the same answer offline. Tests
    # needing a different count re-patch in their own body (that override wins).
    monkeypatch.setattr(wf, "_count_source_features", lambda *a, **k: 5, raising=False)
    monkeypatch.setattr(wf, "_detect_geometry_type", lambda *a, **k: "polygon", raising=False)
