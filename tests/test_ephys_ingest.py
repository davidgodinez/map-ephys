from . import (dj_config, pipeline,
               load_animal, delay_response_behavior_ingestion,
               foraging_behavior_ingestion,
               ephys_ingestion,
               multi_target_licking_behavior_ingestion,
               multi_target_licking_ephys_ingestion,
               testdata_paths)


def test_load_insertion_info(pipeline, load_insertion_location):
    experiment = pipeline['experiment']
    ephys = pipeline['ephys']

    mtl_sessions = experiment.Session & 'rig = "RRig-MTL"'

    assert len(ephys.ProbeInsertion.InsertionLocation & mtl_sessions) == len(ephys.ProbeInsertion & mtl_sessions)
    assert len(ephys.ProbeInsertionQuality & mtl_sessions) == 2

    susu_sessions = experiment.Session & 'username = "susu"'
    assert len(ephys.ProbeInsertion.InsertionLocation & susu_sessions) == 8


def test_jrclust_ingest(pipeline, ephys_ingestion, testdata_paths):
    ephys = pipeline['ephys']
    ephys_ingestion = pipeline['ephys_ingest']

    rel_path = testdata_paths['jrclust4-npx1.0_3B']
    rel_path_win = rel_path.replace(r'/', r'\\')

    insertion_key = (ephys_ingestion.EphysIngest.EphysFile.proj(
        insertion_number='probe_insertion_number')
                     * ephys.ProbeInsertion
                     & [f'ephys_file LIKE "%{rel_path}%"',
                        f'ephys_file LIKE "%{rel_path_win}%" ESCAPE "|"']).fetch1('KEY')

    probe_type, electrode_config_name = (ephys.ProbeInsertion & insertion_key).fetch1(
        'probe_type', 'electrode_config_name')

    assert probe_type == 'neuropixels 1.0 - 3B'
    assert electrode_config_name == '1-301'

    assert len(ephys.Unit & insertion_key) == 382
    assert len(ephys.Unit & insertion_key & 'unit_quality = "good"') == 89


def test_ks2_noQC_ingest(pipeline, ephys_ingestion, testdata_paths):
    ephys = pipeline['ephys']
    ephys_ingestion = pipeline['ephys_ingest']

    rel_path = testdata_paths['ks2-npx1.0_3B-no_QC']
    rel_path_win = rel_path.replace(r'/', r'\\')

    insertion_key = (ephys_ingestion.EphysIngest.EphysFile.proj(
        insertion_number='probe_insertion_number')
                     * ephys.ProbeInsertion
                     & [f'ephys_file LIKE "%{rel_path}%"',
                        f'ephys_file LIKE "%{rel_path_win}%" ESCAPE "|"']).fetch1('KEY')

    probe_type, electrode_config_name = (ephys.ProbeInsertion & insertion_key).fetch1(
        'probe_type', 'electrode_config_name')

    assert probe_type == 'neuropixels 1.0 - 3B'
    assert electrode_config_name == '1-384'

    assert len(ephys.Unit & insertion_key) == 291
    assert len(ephys.Unit & insertion_key & 'unit_quality = "good"') == 108


def test_ks2_npx2_ingest(pipeline, ephys_ingestion, testdata_paths):
    ephys = pipeline['ephys']
    ephys_ingestion = pipeline['ephys_ingest']

    # NPX 2.0 SS
    rel_path = testdata_paths['ks2-npx2.0_SS']
    rel_path_win = rel_path.replace(r'/', r'\\')

    insertion_key = (ephys_ingestion.EphysIngest.EphysFile.proj(
        insertion_number = 'probe_insertion_number')
                     * ephys.ProbeInsertion
                     & [f'ephys_file LIKE "%{rel_path}%"',
                        f'ephys_file LIKE "%{rel_path_win}%" ESCAPE "|"']).fetch1('KEY')

    probe_type, electrode_config_name = (ephys.ProbeInsertion & insertion_key).fetch1(
        'probe_type', 'electrode_config_name')

    assert probe_type == 'neuropixels 2.0 - SS'
    assert electrode_config_name == '1-384'

    assert len(ephys.Unit & insertion_key) == 267
    assert len(ephys.Unit & insertion_key & 'unit_quality = "good"') == 172

    assert len(ephys.ClusterMetric & insertion_key) == 267
    assert len(ephys.WaveformMetric & insertion_key) == 267

    # NPX 2.0 MS
    rel_path = testdata_paths['ks2-npx2.0_MS']
    rel_path_win = rel_path.replace(r'/', r'\\')

    insertion_key = (ephys_ingestion.EphysIngest.EphysFile.proj(
        insertion_number = 'probe_insertion_number')
                     * ephys.ProbeInsertion
                     & [f'ephys_file LIKE "%{rel_path}%"',
                        f'ephys_file LIKE "%{rel_path_win}%" ESCAPE "|"']).fetch1('KEY')

    probe_type, electrode_config_name = (ephys.ProbeInsertion & insertion_key).fetch1(
        'probe_type', 'electrode_config_name')

    assert probe_type == 'neuropixels 2.0 - MS'
    assert electrode_config_name == '1-96; 1281-1376; 2561-2656; 3841-3936'

    assert len(ephys.Unit & insertion_key) == 218
    assert len(ephys.Unit & insertion_key & 'unit_quality = "good"') == 115

    assert len(ephys.ClusterMetric & insertion_key) == 218
    assert len(ephys.WaveformMetric & insertion_key) == 218


def test_multi_target_licking_ingest(pipeline, multi_target_licking_ephys_ingestion,
                                     testdata_paths):
    experiment = pipeline['experiment']
    ephys = pipeline['ephys']
    behavior_ingest = pipeline['behavior_ingest']

    rel_path = testdata_paths['multi-target-licking-c']
    session_key = (experiment.Session
                   & (behavior_ingest.BehaviorIngest.BehaviorFile
                      & f'behavior_file = "{rel_path}"')).fetch1('KEY')

    assert len(ephys.ProbeInsertion & session_key) == 2
    assert len(experiment.Breathing & session_key) == len(experiment.Piezoelectric & session_key) == len(experiment.SessionTrial & session_key)

