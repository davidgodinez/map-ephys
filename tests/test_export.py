import numpy as np
import pathlib
from . import (dj_config, project_dir, pipeline,
               load_animal, delay_response_behavior_ingestion,
               foraging_behavior_ingestion,
               ephys_ingestion, testdata_paths,
               load_insertion_info)


def test_export(load_insertion_info, pipeline):
    ephys = pipeline['ephys']
    export = pipeline['export']

    insert_key = (ephys.ProbeInsertion
                  & ephys.ProbeInsertion.InsertionLocation).fetch('KEY', limit=1)[0]

    filename = project_dir / 'tests/test_data' / ('_'.join(np.array(list(insert_key.values())).astype(str)) + '.mat')

    export.export_recording(insert_key,
                            output_dir=filename.parent,
                            filename=filename.name,
                            overwrite=True)

    assert filename.exists()

