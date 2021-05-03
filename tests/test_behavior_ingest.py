from . import (dj_config, pipeline, testdata_paths, load_animal,
               delay_response_behavior_ingestion,
               multi_target_licking_behavior_ingestion,
               foraging_behavior_ingestion)


def test_delay_response_behavior_ingest(delay_response_behavior_ingestion,
                                        pipeline, testdata_paths):
    experiment = pipeline['experiment']
    behavior_ingest = pipeline['behavior_ingest']

    assert len(experiment.Session & 'username = "daveliu"') == 8
    assert len(experiment.Session & 'username = "susu"') == 10

    # test-case for 1 Susu's session
    rel_path = testdata_paths['delay-response-daveliu']
    session_key = (experiment.Session
                   & (behavior_ingest.BehaviorIngest.BehaviorFile
                      & f'behavior_file = "{rel_path}"')).fetch1('KEY')
    assert len(experiment.BehaviorTrial & session_key) == 504
    assert len(experiment.BehaviorTrial & session_key
               & 'trial_instruction = "right"'
               & 'outcome = "hit"') == 207
    assert len(experiment.TrialEvent & session_key) == 2558

    # test-case for 1 Susu's session
    rel_path = testdata_paths['delay-response-susu']
    session_key = (experiment.Session
                   & (behavior_ingest.BehaviorIngest.BehaviorFile
                      & f'behavior_file = "{rel_path}"')).fetch1('KEY')
    assert len(experiment.BehaviorTrial & session_key) == 450
    assert len(experiment.BehaviorTrial & session_key
               & 'trial_instruction = "right"'
               & 'outcome = "hit"') == 181
    assert len(experiment.TrialEvent & session_key) == 2379


def test_multi_target_licking_behavior_ingest(multi_target_licking_behavior_ingestion,
                                        pipeline, testdata_paths):
    experiment = pipeline['experiment']
    behavior_ingest = pipeline['behavior_ingest']

    rel_path = testdata_paths['multi-target-licking']
    session_key = (experiment.Session
                   & (behavior_ingest.BehaviorIngest.BehaviorFile
                      & f'behavior_file = "{rel_path}"')).fetch1('KEY')
    assert len(experiment.BehaviorTrial & session_key) == 39
    assert len(experiment.MultiTargetLickingSessionBlock
               & session_key) == 207
    assert len(experiment.TrialEvent & session_key) == 2558



