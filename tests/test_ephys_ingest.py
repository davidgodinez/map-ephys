from . import (dj_config, pipeline,
               load_animal, delay_response_behavior_ingest,
               foraging_behavior_ingest,
               ephys_ingest, testdata_paths)


def test_ephys_ingest(pipeline, ephys_ingest):
    experiment = pipeline['experiment']
    ephys = pipeline['ephys']

    assert len(experiment.Session & ephys.Unit) == 5


def test_jrclust_ingest(pipeline, ephys_ingest, testdata_paths):
    experiment = pipeline['experiment']
    ephys = pipeline['ephys']

    rel_path = testdata_paths['jrclust4-npx1.0_3B']

    ephys_ingest.EphysIngest.EphysFile.proj(
        insertion_number='probe_insertion_number') * ephys.ProbeInsertion &

    assert len(experiment.Session & ephys.Unit) == 5

