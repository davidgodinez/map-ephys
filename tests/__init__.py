# run tests: pytest -sv --cov-report term-missing --cov=map-ephys -p no:warnings

import os
import pytest
import pandas as pd
import pathlib
import datajoint as dj
import numpy as np
import logging

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


# ------------------- SOME CONSTANTS -------------------

_tear_down = False

root_data_dir = pathlib.Path(r'F:/map')
test_data_dir = root_data_dir / 'test_data_full'
project_dir = (pathlib.Path(__file__) / '../..').resolve()


# ------------------- FIXTURES -------------------

@pytest.fixture(autouse=True)
def dj_config():
    dj.config['database.host'] = 'localhost'
    dj.config['database.user'] = 'root'
    dj.config['database.password'] = 'simple'
    dj.config['database.reconnect'] = True
    dj.config['enable_python_native_blobs'] = True
    dj.config['safemode'] = False
    dj.config['cache'] = (root_data_dir / 'djcache').as_posix()

    dj.config['stores'] = {'report_store':
                               {'protocol': 'file',
                                'location': (root_data_dir / 'figure_report').as_posix(),
                                'stage': (root_data_dir / 'figure_report').as_posix()}
                           }

    dj.config['custom'] = {
        'ccf_data_paths': {
            'annotation_nrrd': project_dir / 'annotation_10.nrrd',
            'annotation_tif': project_dir / 'Annotation_new_10_ds222_32bit.tif',
            'hexcode_csv': project_dir / 'hexcode.csv',
            'region_csv': project_dir / 'mousebrainontoogy_2.csv',
            'version_name': 'CCF_2017'},
        'ephys_data_paths': [test_data_dir / 'ephys'],
        'histology_data_paths': [test_data_dir / 'ephys'],
    }
    return


@pytest.fixture
def pipeline():
    from pipeline import lab, ccf, ephys, experiment, histology, tracking, psth, shell, export
    from pipeline.ingest import behavior as behavior_ingest
    from pipeline.ingest import ephys as ephys_ingest
    from pipeline.ingest import tracking as tracking_ingest
    from pipeline.ingest import histology as histology_ingest

    yield {'lab': lab,
           'ccf': ccf,
           'ephys': ephys,
           'experiment': experiment,
           'histology': histology,
           'tracking': tracking,
           'psth': psth,
           'shell': shell,
           'export': export,
           'behavior_ingest': behavior_ingest,
           'tracking_ingest': tracking_ingest,
           'ephys_ingest': ephys_ingest,
           'histology_ingest': histology_ingest
           }


@pytest.fixture
def testdata_paths():
    return {
        'jrclust4-npx1.0_3B': 'SC022/20190303/1/SC022_g0_t0.imec0.ap_res.mat',
        'ks2-npx2.0_MS': 'SC038/catgt_SC038_111919_115124_g0/SC038_111919_g0_imec0/imec0_ks2',
        'ks2-npx2.0_SS': 'SC035/catgt_SC035_010720_111723_g0/SC035_010720_g0_imec1/imec1_ks2',
        'ks2-npx1.0_3B-no_QC': 'SC011/catgt_SC011_021919_151204_g0/SC011_021919_g0_imec2',
        'delay-response-daveliu': 'dl56_tw2_20181126_134907.mat',
        'delay-response-susu': 'SC038_SC_RecordingRig3_20191119_115037.mat',
        'multi-target-licking-a': 'DL009_af_2D_20210415_141520.mat',
        'multi-target-licking-b': 'DL010_af_2D_20210422_164220.mat',
        'multi-target-licking-c': 'DL004_af_2D_20210308_180001.mat',
        'tracking-delay-response': 'SC035_SC_RecordingRig3_20200107_111553.mat'
    }


@pytest.fixture
def load_animal(pipeline):
    shell = pipeline['shell']
    shell.load_animal(project_dir / 'tests/test_data/Multi-regionRecordingNotes_sc.xlsx')

    yield


@pytest.fixture
def delay_response_behavior_ingestion(load_animal, pipeline):
    behavior_ingest = pipeline['behavior_ingest']
    experiment = pipeline['experiment']

    # Dave's sessions
    dj.config['custom']['session.user'] = 'daveliu'
    dj.config['custom']['behavior_data_paths'] = [
        ['RRig', test_data_dir / 'behavior/daveliu', 0]
    ]

    behavior_ingest.BehaviorIngest.populate()

    # Susu's sessions
    dj.config['custom']['session.user'] = 'susu'
    dj.config['custom']['behavior_data_paths'] = [
        ['RRig2', test_data_dir / 'behavior/susu', 0]
    ]

    behavior_ingest.BehaviorIngest.populate()

    yield

    if _tear_down:
        (experiment.Session & 'username in ("susu", "daveliu")').delete()


@pytest.fixture
def foraging_behavior_ingestion(load_animal, pipeline):
    behavior_ingest = pipeline['behavior_ingest']
    experiment = pipeline['experiment']

    # foraging-task Han's sessions
    dj.config['custom']['session.user'] = 'HH'
    dj.config['custom']['behavior_bpod'] = {
        'meta_dir': test_data_dir / 'bpod_meta',
        'project_paths':
            [test_data_dir / 'behavior_rigs/Tower-1/Foraging',
             test_data_dir / 'behavior_rigs/Tower-2/Foraging',
             test_data_dir / 'behavior_rigs/Tower-2/Foraging_again',
             test_data_dir / 'behavior_rigs/Tower-2/Foraging_homecage',
             test_data_dir / 'behavior_rigs/Tower-3/Foraging_homecage']
    }

    behavior_ingest.BehaviorBpodIngest.populate(suppress_errors=True)

    yield

    if _tear_down:
        (experiment.Session & 'username in ("HH")').delete()


@pytest.fixture
def multi_target_licking_behavior_ingestion(load_animal, pipeline):
    behavior_ingest = pipeline['behavior_ingest']
    experiment = pipeline['experiment']

    # Dave's sessions
    dj.config['custom']['session.user'] = 'daveliu'
    dj.config['custom']['behavior_data_paths'] = [
        ['RRig-MTL', test_data_dir / 'behavior/multi_target_licking', 0]
    ]

    behavior_ingest.BehaviorIngest.populate()

    yield

    if _tear_down:
        session_keys = (experiment.Session & (experiment.BehaviorTrial
                                              & 'task = "multi-target-licking"')).fetch('KEY')
        (experiment.Session & session_keys).delete()


@pytest.fixture
def ephys_ingestion(delay_response_behavior_ingestion, pipeline):
    ephys_ingest = pipeline['ephys_ingest']
    experiment = pipeline['experiment']
    ephys = pipeline['ephys']

    session_keys = (experiment.Session & 'username = "susu"').fetch('KEY')

    ephys_ingest.EphysIngest.populate(session_keys)

    yield

    if _tear_down:
        (ephys.ProbeInsertion & session_keys).delete()
        (ephys_ingest.EphysIngest & session_keys).delete()


@pytest.fixture
def multi_target_licking_ephys_ingestion(multi_target_licking_behavior_ingestion, pipeline):
    ephys_ingest = pipeline['ephys_ingest']
    experiment = pipeline['experiment']
    ephys = pipeline['ephys']

    session_keys = (experiment.Session
                    & (experiment.BehaviorTrial & 'task = "multi-target-licking"')).fetch('KEY')

    ephys_ingest.EphysIngest.populate(session_keys)
    experiment.Breathing.populate(session_keys)
    experiment.Piezoelectric.populate(session_keys)

    yield

    if _tear_down:
        (ephys.ProbeInsertion & session_keys).delete()
        (ephys_ingest.EphysIngest & session_keys).delete()
        (experiment.Breathing & session_keys).delete()
        (experiment.Piezoelectric & session_keys).delete()


@pytest.fixture
def load_insertion_info(ephys_ingestion, pipeline):
    shell = pipeline['shell']
    shell.load_insertion_location(project_dir / 'tests/test_data/Multi-regionRecordingNotes_sc.xlsx')
    shell.load_insertion_location(project_dir / 'tests/test_data/RecordingNotes_dl.xlsx')
    yield


@pytest.fixture
def tracking_ingestion(delay_response_behavior_ingestion,
                       multi_target_licking_behavior_ingestion,
                       pipeline):
    tracking_ingest = pipeline['tracking_ingest']
    experiment = pipeline['experiment']
    tracking = pipeline['tracking']

    dj.config['custom']['tracking_data_paths'] = [
        ['not_used', test_data_dir / 'tracking']
    ]

    tracking_ingest.TrackingIngest.populate()

    yield

    if _tear_down:
        session_keys = (experiment.Session
                        & (experiment.BehaviorTrial
                           & 'task in ("multi-target-licking", "audio delay")')).fetch('KEY')
        (tracking.Tracking & session_keys).delete()
        (tracking_ingest.TrackingIngest & session_keys).delete()


@pytest.fixture
def histology_ingestion(delay_response_behavior_ingestion,
                       multi_target_licking_behavior_ingestion,
                       pipeline):
    histology_ingest = pipeline['histology_ingest']
    experiment = pipeline['experiment']
    histology = pipeline['histology']

    dj.config['custom']['histology_data_paths'] = [
        "F:/map/test_data_full/histology",
        "F:/map/test_data_full/ephys"]

    histology_ingest.HistologyIngest.populate()

    yield

    if _tear_down:
        session_keys = (experiment.Session
                        & (experiment.BehaviorTrial
                           & 'task in ("multi-target-licking", "audio delay")')).fetch('KEY')
        (histology.ElectrodeCCFPosition & session_keys).delete()
        (histology.LabeledProbeTrack & session_keys).delete()
        (histology_ingest.HistologyIngest & session_keys).delete()
