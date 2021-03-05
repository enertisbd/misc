#########################################################################################
##
##  This module contains helper functions relate to YAML files.
##  The funtions in this module must not inherite any of the other local modules.
##
## Author: Lucas Viani
## Date  : 22.10.2018
##

import yaml

LOADER = yaml.Loader
FULL_LOADER = yaml.Loader

def dump_yaml (data, fpath):
    '''Dumps data to a yaml file'''
    with open(fpath, 'w') as outfile:
        yaml.dump(data, outfile, default_flow_style=False)

def load_yaml (fpath, loader=FULL_LOADER):
    '''Loads a yaml file and returns its content.'''
    with open(fpath, 'r') as stream:
        data = yaml.load(stream, Loader=loader)
    return data

def export_conf_file (path, title, asset_nm, in_folder, out_folder, out_fnm, file_patt, 
                      io_type_str, feat_time, layers, unique_fnm):
    '''Export the configuration file.'''
    dump_yaml ({'title'        : title,
                'asset_name'   : asset_nm,
                'input_folder' : in_folder,
                'output_folder': out_folder,
                "output_fnm"   : out_fnm,
                'file_pattern' : file_patt,
                'io_type'      : io_type_str,
                'feat_time'    : feat_time,
                'layers'       : layers,
                'unique_fnm'   : unique_fnm,
               }, path)
