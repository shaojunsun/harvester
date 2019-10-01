#BOINC CONFIGRATION
boinc_project = {
    'url': 'http://atlasathome.cern.ch/Atlas-test/',
    'authenticator': 'FIXME',
    'app_name': 'ATLAS_backfill',
    'batch_name': 'manual:test',
    'input_template_filename': 'ATLAS_backfill_IN_2',
    'output_template_filename': 'ATLAS_backfill_OUT',
    'dest_log': '/data/boinc/logs/'
}

boinc_input_files = {
    'proxy_path': '/data/atlpilo1/x509up',
    'pilot_wrapper_path': '/data/atlpilo1/start_atlas.sh',
}
