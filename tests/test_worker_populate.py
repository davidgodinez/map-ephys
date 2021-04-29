from . import (dj_config, pipeline,
               load_animal, delay_response_behavior_ingestion,
               foraging_behavior_ingestion,
               ephys_ingestion, testdata_paths)


def test_worker_populate(pipeline):
    shell = pipeline['shell']
    shell.delete_empty_ingestion_tables()
    shell.automate_computation()
