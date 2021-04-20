# run tests: pytest -sv --cov-report term-missing --cov=workflow-array-ephys -p no:warnings

import os
import pytest
import pandas as pd
import pathlib
import datajoint as dj
import numpy as np


# ------------------- SOME CONSTANTS -------------------

test_data_dir = pathlib.Path(r'F:/map/test_data_full')
project_dir = pathlib.Path('..').resolve()


# ------------------- FIXTURES -------------------

@pytest.fixture(autouse=True)
def dj_config():
    dj.config = {'database.host': 'localhost',
                 'database.password': 'simple',
                 'database.port': 3306,
                 'database.reconnect': True,
                 'enable_python_native_blobs': True,
                 'cache': 'F:/map/djcache'}

    dj.config['custom'] = {
        'ccf_data_paths': {
            'annotation_nrrd': project_dir / 'annotation_10.nrrd',
            'annotation_tif': project_dir / 'Annotation_new_10_ds222_32bit.tif',
            'hexcode_csv': project_dir / 'hexcode.csv',
            'region_csv': project_dir / 'mousebrainontoogy_2.csv',
            'version_name': 'CCF_2017'},
        'ephys_data_paths': [test_data_dir / 'ephys'],
        'histology_data_paths': [test_data_dir / 'ephys'],
        'tracking_data_paths': [
            ['RRig', test_data_dir / 'ephys'],
            ['RRig2', test_data_dir / 'SusuTracking']
        ]
    }
    return


@pytest.fixture
def pipeline():
    import pipeline

    yield {'lab': pipeline.lab,
           'ccf': pipeline.ccf,
           'ephys': pipeline.ephys,
           'experiment': pipeline.experiment,
           'histology': pipeline.histology,
           'tracking': pipeline.tracking,
           'psth': pipeline.psth,
           'shell': pipeline.shell}
    

@pytest.fixture
def ingest_pipeline():
    from pipeline.ingest import behavior, tracking, ephys, histology

    yield {'behavior': behavior,
           'tracking': tracking,
           'ephys': ephys,
           'histology': histology}


@pytest.fixture
def load_animal(pipeline):
    shell = pipeline['shell']
    shell.load_animal(project_dir / 'test_data/Multi-regionRecordingNotes_sc.xlsx')

    yield


@pytest.fixture
def delay_response_behavior_ingest(load_animal, ingest_pipeline, pipeline):
    behavior_ingest = ingest_pipeline['behavior']
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

    (experiment.Session & 'username in ("susu", "daveliu")').delete()


@pytest.fixture
def foraging_behavior_ingest(load_animal, ingest_pipeline, pipeline):
    behavior_ingest = ingest_pipeline['behavior']
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

    behavior_ingest.BehaviorBpodIngest.populate()

    yield

    (experiment.Session & 'username in ("HH")').delete()
