import numpy as np

from . import (dj_config, pipeline, testdata_paths, load_animal,
               delay_response_behavior_ingestion,
               multi_target_licking_behavior_ingestion,
               foraging_behavior_ingestion, tracking_ingestion)


def test_delay_response_tracking_ingest(tracking_ingestion,
                                        pipeline, testdata_paths):
    experiment = pipeline['experiment']
    tracking = pipeline['tracking']
    behavior_ingest = pipeline['behavior_ingest']

    rel_path = testdata_paths['tracking-delay-response']
    session_key = (experiment.Session
                   & (behavior_ingest.BehaviorIngest.BehaviorFile
                      & f'behavior_file = "{rel_path}"')).fetch1('KEY')

    trial_count = len(experiment.BehaviorTrial & session_key)

    assert len(tracking.Tracking & session_key) == trial_count
    assert (tracking.TrackingDevice
            & (tracking.Tracking & session_key)).fetch1('tracking_device') == 'Camera 0'

    tracked_features = {k: len(v & session_key)
                        for k, v in tracking.Tracking().tracking_features.items()}

    assert (set([k for k in tracked_features if tracked_features[k] != 0])
            == {'NoseTracking', 'TongueTracking', 'JawTracking'})


def test_multi_target_licking_tracking_ingest(tracking_ingestion,
                                        pipeline, testdata_paths):
    experiment = pipeline['experiment']
    tracking = pipeline['tracking']
    behavior_ingest = pipeline['behavior_ingest']

    rel_path = testdata_paths['multi-target-licking-c']
    session_key = (experiment.Session
                   & (behavior_ingest.BehaviorIngest.BehaviorFile
                      & f'behavior_file = "{rel_path}"')).fetch1('KEY')

    trial_count = len(experiment.BehaviorTrial & session_key)

    cameras = (tracking.TrackingDevice
               & (tracking.Tracking & session_key)).fetch('tracking_device')
    assert list(cameras) == ['Camera 3', 'Camera 4']

    assert len(tracking.Tracking & session_key & 'tracking_device = "Camera 3"') == trial_count

    tracked_features = {k: len(v & session_key & 'tracking_device = "Camera 3"')
                        for k, v in tracking.Tracking().tracking_features.items()}

    assert (set([k for k in tracked_features if tracked_features[k] != 0])
            == {'LickPortTracking', 'TongueTracking', 'JawTracking'})
