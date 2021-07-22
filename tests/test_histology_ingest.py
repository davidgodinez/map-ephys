import numpy as np

from . import (dj_config, pipeline, testdata_paths, load_animal,
               delay_response_behavior_ingestion,
               multi_target_licking_behavior_ingestion,
               foraging_behavior_ingestion,
               histology_ingestion)


def test_format1_histology_ingest(pipeline, testdata_paths, histology_ingestion):
    experiment = pipeline['experiment']
    ephys = pipeline['ephys']
    histology = pipeline['histology']
    ephys_ingest = pipeline['ephys_ingest']
    behavior_ingest = pipeline['behavior_ingest']

    rel_path = testdata_paths['ks2-npx2.0_MS']
    rel_path_win = rel_path.replace(r'/', r'\\')

    insertion_key = (ephys_ingest.EphysIngest.EphysFile.proj(
        insertion_number = 'probe_insertion_number')
                     * ephys.ProbeInsertion
                     & [f'ephys_file LIKE "%{rel_path}%"',
                        f'ephys_file LIKE "%{rel_path_win}%" ESCAPE "|"']).fetch1('KEY')

    assert len(histology.ElectrodeCCFPosition.ElectrodePosition & insertion_key) == 318
    assert len(histology.LabeledProbeTrack.Point & insertion_key) == 20


def test_format2_histology_ingest(pipeline, testdata_paths, histology_ingestion):
    experiment = pipeline['experiment']
    ephys = pipeline['ephys']
    histology = pipeline['histology']
    behavior_ingest = pipeline['behavior_ingest']

    rel_path = testdata_paths['multi-target-licking-c']
    session_key = (experiment.Session
                   & (behavior_ingest.BehaviorIngest.BehaviorFile
                      & f'behavior_file = "{rel_path}"')).fetch1('KEY')
    insertion_key = (ephys.ProbeInsertion & session_key & 'insertion_number = 1').fetch1('KEY')

    assert len(histology.ElectrodeCCFPosition.ElectrodePosition & insertion_key) == 382
    assert len(histology.LabeledProbeTrack.Point & insertion_key) == 9
