"""Unit tests for the pure coordination-detection helpers in cn_astroturf_mapper."""
from worker.workflows.cn_astroturf_mapper import (
    AstroturfMapperInput,
    detect_bursts,
    detect_coordination,
    detect_status_flips,
    find_corating_clusters,
    score_handle,
)


def test_clusters_group_high_overlap_raters():
    rater_notes = {
        "a": {"n1", "n2", "n3", "n4"},
        "b": {"n1", "n2", "n3", "n4"},
        "c": {"n1", "n2", "n3", "n5"},
        "d": {"n1", "n2", "n3", "n4"},
        "e": {"n1", "n2", "n3", "n4"},
        "loner": {"x1", "x2", "x3"},
    }
    clusters = find_corating_clusters(rater_notes, min_overlap=0.5, min_cluster_size=5, min_notes=3)
    assert len(clusters) == 1
    assert clusters[0]["size"] == 5
    assert "loner" not in clusters[0]["member_participant_ids"]


def test_clusters_empty_when_too_few_raters():
    rater_notes = {"a": {"n1", "n2"}, "b": {"n1", "n2"}}
    assert find_corating_clusters(rater_notes, min_overlap=0.5, min_cluster_size=5, min_notes=3) == []


def test_burst_detects_one_sided_window():
    ratings = [{"createdAtMillis": i * 1000, "helpfulness": "HELPFUL"} for i in range(10)]
    bursts = detect_bursts({"n1": ratings}, window_ms=30_000, min_ratings=8, one_sided_ratio=0.8)
    assert len(bursts) == 1
    assert bursts[0]["count"] == 10
    assert bursts[0]["direction"] == "helpful"
    assert bursts[0]["one_sided_ratio"] == 1.0


def test_burst_ignores_mixed_window():
    ratings = [
        {"createdAtMillis": i * 1000, "helpfulness": "HELPFUL" if i % 2 == 0 else "NOT_HELPFUL"}
        for i in range(10)
    ]
    bursts = detect_bursts({"n1": ratings}, window_ms=30_000, min_ratings=8, one_sided_ratio=0.8)
    assert bursts == []


def test_status_flip_detected_after_helpful():
    hist = [
        {"createdAtMillis": 1, "status": "CURRENTLY_RATED_HELPFUL"},
        {"createdAtMillis": 2, "status": "NEEDS_MORE_RATINGS"},
    ]
    flips = detect_status_flips({"n1": hist})
    assert len(flips) == 1
    assert flips[0]["new_status"] == "NEEDS_MORE_RATINGS"


def test_score_handle_scales_and_caps():
    assert score_handle(0, [], [], []) == (0.0, "none")
    big_cluster = {"size": 20}
    score, severity = score_handle(10, [big_cluster, dict(big_cluster)], [{"x": 1}] * 10, [{"y": 1}] * 10)
    assert score == 80.0
    assert severity == "critical"


def test_detect_reports_noted_tweet_ids():
    p = AstroturfMapperInput(target_handles=["h"])
    targets = [{"handle": "h", "tweet_ids": ["t1", "t2"], "tweets_scanned": 2}]
    notes = [{"noteId": "n1", "tweetId": "t1", "author_participant_id": "x", "createdAtMillis": 1,
              "classification": "MISINFORMED_OR_POTENTIALLY_MISLEADING", "summary_excerpt": "s"}]
    data = {"ratings_by_note": {}, "status_by_note": {}, "total_ratings_kept": 0}
    import asyncio

    f = asyncio.run(detect_coordination(targets, notes, data, p))
    assert f["per_handle"]["h"]["noted_tweet_ids"] == ["t1"]
    assert f["per_handle"]["h"]["notes_on_their_posts"] == 1
