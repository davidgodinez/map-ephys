import datajoint as dj
import pathlib
import numpy as np
import json
from datetime import datetime
from dateutil.tz import tzlocal
import pynwb
from pynwb import NWBFile, NWBHDF5IO

from pipeline import lab, experiment, tracking, ephys, histology, psth, ccf
from pipeline.util import _get_clustering_method
from pipeline.report import get_wr_sessdate

# Some constants to work with
zero_time = datetime.strptime('00:00:00', '%H:%M:%S').time()  # no precise time available

def datajoint_to_nwb(session_key):
    """
    Generate one NWBFile object representing all data
     coming from the specified "session_key" (representing one session)
    """

    session_key = (experiment.Session.proj(
        session_datetime="cast(concat(session_date, ' ', session_time) as datetime)")
                   & session_key).fetch1()

    session_identifier = f'{session_key["subject_id"]}' \
                         f'_{session_key["session"]}' \
                         f'_{session_key["session_datetime"].strftime("%Y%m%d_%H%M%S")}'

    experiment_description = (experiment.TaskProtocol
                              & (experiment.BehaviorTrial & session_key)).fetch1(
        'task_protocol_description')

    try:
        session_descr = (experiment.SessionComment & session_key).fetch1('session_comment')
    except:
        session_descr = ' '

    nwbfile = NWBFile(identifier=session_identifier,
                      session_description=session_descr, 
                      session_start_time=session_key['session_datetime'],
                      file_create_date=datetime.now(tzlocal()),
                      experimenter=list((experiment.Session & session_key).fetch('username')),
                      data_collection='',
                      institution='Janelia Research Campus',
                      experiment_description=experiment_description,
                      related_publications='',
                      keywords=[])

     # ---- Subject ----
    subject = (lab.Subject & session_key).fetch1()
    nwbfile.subject = pynwb.file.Subject(
        subject_id=str(subject['subject_id']),
        date_of_birth=datetime.combine(subject['date_of_birth'], zero_time) if subject['date_of_birth'] else None,
        sex=subject['sex'])

    # add additional columns to the electrodes table
    electrodes_query = lab.ProbeType.Electrode * lab.ElectrodeConfig.Electrode
    for additional_attribute in ['shank', 'shank_col', 'shank_row']:
        nwbfile.add_electrode_column(
            name=electrodes_query.heading.attributes[additional_attribute].name,
            description=electrodes_query.heading.attributes[additional_attribute].comment)

    # add additional columns to the units table
    units_query = (ephys.ProbeInsertion.RecordingSystemSetup
                   * ephys.Unit * ephys.UnitStat
                   * ephys.ClusterMetric * ephys.WaveformMetric
                   & session_key)

    units_omitted_attributes = ['subject_id', 'session', 'insertion_number',
                                'clustering_method', 'unit', 'unit_uid', 'probe_type',
                                'epoch_name_quality_metrics', 'epoch_name_waveform_metrics',
                                'electrode_config_name', 'electrode_group',
                                'electrode', 'waveform']

    for attr in units_query.heading.names:
        if attr in units_omitted_attributes:
            continue
        nwbfile.add_unit_column(
            name=units_query.heading.attributes[attr].name,
            description=units_query.heading.attributes[attr].comment)

    # iterate through curated clusterings and export units data
    for insert_key in (ephys.ProbeInsertion & session_key).fetch('KEY'):
        # ---- Probe Insertion Location ----
        if ephys.ProbeInsertion.InsertionLocation & insert_key:
            insert_location = {
                k: str(v) for k, v in (ephys.ProbeInsertion.InsertionLocation
                                       & insert_key).aggr(
                    ephys.ProbeInsertion.RecordableBrainRegion.proj(
                        ..., brain_region='CONCAT(hemisphere, " ", brain_area)'),
                    ..., brain_regions='GROUP_CONCAT(brain_region SEPARATOR ", ")').fetch1().items()
                if k not in ephys.ProbeInsertion.primary_key}
            insert_location = json.dumps(insert_location)
        else:
            insert_location = 'N/A'

        # ---- Electrode Configuration ----
        electrode_config = (lab.Probe * lab.ProbeType * lab.ElectrodeConfig
                            * ephys.ProbeInsertion & insert_key).fetch1()
        ephys_device_name = f'{electrode_config["probe"]} ({electrode_config["probe_type"]})'
        ephys_device = (nwbfile.get_device(ephys_device_name)
                        if ephys_device_name in nwbfile.devices
                        else nwbfile.create_device(name=ephys_device_name))

        electrode_group = nwbfile.create_electrode_group(
            name=f'{electrode_config["probe"]} {electrode_config["electrode_config_name"]}',
            description=json.dumps(electrode_config, default=str),
            device=ephys_device,
            location=insert_location)

        electrode_query = (lab.ProbeType.Electrode * lab.ElectrodeConfig.Electrode
                           & electrode_config)
        for electrode in electrode_query.fetch(as_dict=True):
            nwbfile.add_electrode(
                id=electrode['electrode'], group=electrode_group,
                filtering='', imp=-1.,
                x=np.nan, y=np.nan, z=np.nan,
                rel_x=electrode['x_coord'], rel_y=electrode['y_coord'], rel_z=np.nan,
                shank=electrode['shank'], shank_col=electrode['shank_col'], shank_row=electrode['shank_row'],
                location=electrode_group.location)

        electrode_df = nwbfile.electrodes.to_dataframe()
        electrode_ind = electrode_df.index[electrode_df.group_name == electrode_group.name]
        # ---- Units ----
        unit_query = units_query & insert_key
        for unit in unit_query.fetch(as_dict=True):
            # make an electrode table region (which electrode(s) is this unit coming from)
            unit['id'] = unit.pop('unit')
            unit['electrodes'] = np.where(electrode_ind == unit.pop('electrode'))[0]
            unit['electrode_group'] = electrode_group
            unit['waveform_mean'] = unit.pop('waveform')
            unit['waveform_sd'] = np.full(1, np.nan)

            for attr in list(unit.keys()):
                if attr in units_omitted_attributes:
                    unit.pop(attr)
                elif unit[attr] is None:
                    unit[attr] = np.nan

            nwbfile.add_unit(**unit)
            pass


    # =============================== TRACKING =============================== 
    # add tracking device 
    sessions_nose_tracking = (tracking.Tracking.NoseTracking & session_key).fetch(as_dict=True)

    for trial in sessions_nose_tracking:
        trial_dict = sessions_nose_tracking[trial]	
		sampling_rate = (tracking.TrackingDevice & trial_key).fetch1('sampling_rate')
		trial_len = len(trial['jaw_x'])
		trial_time = trial_len/sampling_rate
		trial_times = np.linspace(0,trial_time, 1/sampling_rate) # time data for spatial series
			
		keys = ['jaw_x','jaw_y', 'jaw_likelihood']
		trial_values = list(map(sessions_nose_tracking[trial].get, keys))  # data for spatial series 

        spatial_series_object = SpatialSeries(
        name='position', 
        data=trial_values,
        reference_frame='unknown',
        timestamps=trial_times)

        position_object = Position(spatial_series=spatial_series_object)

        behavior_module = nwb.create_processing_module(name='behavior',
        description='processed behavioral data')

        behavior_module.add(pos_obj)

        #This process would be repeated for each part table in tracking.Tracking 

    # =============================== PHOTO-STIMULATION ===============================
    stim_sites = {}
    for photostim_key in (experiment.Photostim & (experiment.PhotostimTrial & session_key)).fetch('KEY'):
        photostim = ((experiment.Photostim * lab.PhotostimDevice.proj('excitation_wavelength')) & photostim_key).fetch1()
        stim_device = (nwbfile.get_device(photostim['photostim_device'])
                       if photostim['photostim_device'] in nwbfile.devices
                       else nwbfile.create_device(name=photostim['photostim_device']))

        stim_site = pynwb.ogen.OptogeneticStimulusSite(
            name=f'{photostim["photostim_device"]}_{photostim["photo_stim"]}',
            device=stim_device,
            excitation_lambda=float(photostim['excitation_wavelength']),
            location=json.dumps([{k: v for k, v in stim_locs.items()
                                  if k not in experiment.Photostim.primary_key}
                                 for stim_locs in (experiment.PhotostimLocation
                                                   & photostim_key).fetch(as_dict=True)], default=str),
            description=f'excitation_duration: {photostim["duration"]}')
        nwbfile.add_ogen_site(stim_site)
        stim_sites[photostim['photo_stim']] = stim_site 

    # =============================== BEHAVIOR ===============================   
    q_photostim = ((experiment.BehaviorTrial.Event
                    & 'trial_event_type = "go"').proj(go_time='trial_event_time')
                   * experiment.PhotostimTrial.Event
                   * experiment.Photostim.proj(stim_dur='duration')
                   & session_key).proj(
                       'stim_dur', stim_time='ROUND(go_time - photostim_event_time, 2)')
    q_trial = (experiment.SessionTrial * experiment.BehaviorTrial
               * experiment.TrialName & session_key)
    q_trial = q_trial.aggr(
        q_photostim, ...,
        photostim_onset='IFNULL(GROUP_CONCAT(stim_time SEPARATOR ", "), "N/A")',
        photostim_power='IFNULL(GROUP_CONCAT(power SEPARATOR ", "), "N/A")',
        photostim_duration='IFNULL(GROUP_CONCAT(stim_dur SEPARATOR ", "), "N/A")',
        keep_all_rows=True)

    q_ephys_event = (experiment.BehaviorTrial.Event
                     & 'trial_event_type = "trigger ephys rec."').proj(
        ephys_trigger='trial_event_type', ephys_start='trial_event_time')
    q_trial_event = (experiment.BehaviorTrial.Event * q_ephys_event
                     & 'trial_event_type NOT in ("send scheduled wave", "trigger ephys rec.", "trigger imaging")'
                     & session_key).proj(
        event_start='trial_event_time - ephys_start',
        event_stop='trial_event_time - ephys_start + duration')

    skip_adding_columns = experiment.Session.primary_key + ['trial_uid', 'trial']
    invalid_events = []
    corrected_invalid_event_trials, corrected_invalid_event_types = [], []
    corrected_invalid_event_starts, correceted_invalid_event_stops = [], []

    if q_trial:
        # Get trial descriptors from TrialSet.Trial and TrialStimInfo
        trial_columns = {tag: {'name': tag,
                               'description': q_trial.heading.attributes[tag].comment}
                         for tag in q_trial.heading.names
                         if tag not in skip_adding_columns + ['start_time', 'stop_time']}
        # Photostim labels from misc.S1PhotostimTrial
        trial_columns.update({'photostim_' + tag: {
            'name': 'photostim_' + tag,
            'description': misc.S1PhotostimTrial.heading.attributes[tag].comment}
            for tag in ('onset', 'power', 'duration')})

        # Add new table columns to nwb trial-table for trial-label
        for c in trial_columns.values():
            nwbfile.add_trial_column(**c)

        # Add entry to the trial-table
        for trial in q_trial.fetch(as_dict=True):
            trial['start_time'], trial['stop_time'] = trial_times[trial['trial']]

            # mark bad trials - those with invalid go-cue time
            # marked as early-lick trials to be excluded from downstream analysis
            trial_duration = trial["stop_time"] - trial["start_time"]
            invalid_trial_events = (
                    q_trial_event & {'trial': trial['trial']}
                    & f'event_start >= {trial_duration}').fetch('KEY')
            if invalid_trial_events:
                trial['early_lick'] = 'early'

                trial_event_keys, trial_event_starts = (q_trial_event
                                                        & {'trial': trial['trial']}).fetch(
                    'KEY', 'event_start', order_by='event_start')

                previous_event_time = 0
                for e_key, e_start in zip(trial_event_keys, trial_event_starts.astype(float)):
                    if e_key in invalid_trial_events:
                        if e_key['trial_event_type'] == 'sample-start chirp':
                            e_start = 0
                            e_stop = e_start + 0.02
                        elif e_key['trial_event_type'] == 'sample':
                            e_start = previous_event_time + 0.15
                            e_stop = e_start + 0.7
                        elif e_key['trial_event_type'] == 'sample-end chirp':
                            e_start = previous_event_time + 0
                            e_stop = e_start + 0.02
                        elif e_key['trial_event_type'] == 'delay':
                            e_start = previous_event_time + 0.15
                            e_stop = e_start + 2
                        elif e_key['trial_event_type'] == 'go':
                            e_start = previous_event_time + 2
                            e_stop = e_start + 0.01
                        else:
                            raise ValueError(f'Unexpected event type: {e_key["trial_event_type"]}')

                        if e_start >= trial_duration:
                            e_start = trial_duration - 0.02
                            e_stop = e_start + 0.01

                        corrected_invalid_event_trials.append(trial['trial'])
                        corrected_invalid_event_types.append(e_key['trial_event_type'])
                        corrected_invalid_event_starts.append(e_start)
                        correceted_invalid_event_stops.append(e_stop)
                    else:
                        previous_event_time = e_start

                invalid_events.extend(invalid_trial_events)

            # create trial entries
            trial['id'] = trial['trial']  # rename 'trial_id' to 'id'
            [trial.pop(k) for k in skip_adding_columns]
            nwbfile.add_trial(**trial)

    # ===============================================================================
    # =============================== TRIAL EVENTS ==========================
    # ===============================================================================

    event_times, event_label_ind = [], []
    event_labels = OrderedDict()

    # ---- behavior events ----
    if invalid_events:
        q_trial_event = q_trial_event - (q_trial_event & invalid_events).proj()

    trials, event_types, event_starts, event_stops = q_trial_event.fetch(
        'trial', 'trial_event_type', 'event_start', 'event_stop', order_by='trial')

    if invalid_events:
        trials = np.concatenate([trials, corrected_invalid_event_trials])
        event_types = np.concatenate([event_types, corrected_invalid_event_types])
        event_starts = np.concatenate([event_starts.astype(float), corrected_invalid_event_starts])
        event_stops = np.concatenate([event_stops.astype(float), correceted_invalid_event_stops])

    trial_starts = [trial_times[tr][0] for tr in trials]
    event_starts = event_starts.astype(float) + trial_starts
    event_stops = event_stops.astype(float) + trial_starts

    for etype in set(event_types):
        event_labels[etype + '_start_times'] = len(event_labels)
        event_labels[etype + '_stop_times'] = len(event_labels)

    event_times.extend(event_starts)
    event_label_ind.extend([event_labels[etype + '_start_times'] for etype in event_types])
    event_times.extend(event_stops)
    event_label_ind.extend([event_labels[etype + '_stop_times'] for etype in event_types])

    # ---- action events ----
    q_action_event = (experiment.ActionEvent * q_ephys_event & session_key).proj(
        event_time='action_event_time - ephys_start')

    trials, event_types, event_starts = q_action_event.fetch(
        'trial', 'action_event_type', 'event_time', order_by='trial')
    trial_starts = [trial_times[tr][0] for tr in trials]
    event_starts = event_starts.astype(float) + trial_starts

    for etype in set(event_types):
        event_labels[etype] = len(event_labels)

    event_times.extend(event_starts)
    event_label_ind.extend([event_labels[etype] for etype in event_types])

    labeled_events = LabeledEvents(name='LabeledEvents',
                                   description='behavioral events of the experimental paradigm',
                                   timestamps=event_times,
                                   data=event_label_ind,
                                   labels=list(event_labels.keys()))
    nwbfile.add_acquisition(labeled_events)

    return nwbfile


def export_recording(session_keys, output_dir='./', overwrite=False):
    if not isinstance(session_keys, list):
        session_keys = [session_keys]

    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for session_key in session_keys:
        nwbfile = datajoint_to_nwb(session_key)
        # Write to .nwb
        save_file_name = ''.join([nwbfile.identifier, '.nwb'])
        output_fp = (output_dir / save_file_name).absolute()
        if overwrite or not output_fp.exists():
            with NWBHDF5IO(output_fp.as_posix(), mode='w') as io:
                io.write(nwbfile)
                print(f'\tWrite NWB 2.0 file: {save_file_name}')
