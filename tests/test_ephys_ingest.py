from . import (dj_config, pipeline,
               load_animal, delay_response_behavior_ingestion,
               foraging_behavior_ingestion,
               ephys_ingestion, testdata_paths)


def test_ephys_ingest(pipeline, ephys_ingestion):
    experiment = pipeline['experiment']
    ephys = pipeline['ephys']

    assert len(experiment.Session & ephys.Unit) == 5


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
