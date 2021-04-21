from . import (dj_config, pipeline,
               load_animal, delay_response_behavior_ingest,
               foraging_behavior_ingest)


def test_delay_response_behavior_ingest(delay_response_behavior_ingest,
                                        pipeline):
    experiment = pipeline['experiment']

    assert len(experiment.Session & 'username = "daveliu"') == 8
    assert len(experiment.Session & 'username = "susu"') == 10
