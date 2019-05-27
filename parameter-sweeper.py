#!/usr/bin/env python3

from dask_mpi import initialize

initialize(interface='ib0', nthreads=16)

from dask.distributed import Client

client = Client()  # Connect this local process to remote workers

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch
import sys
import os
import argparse  # for command line parsing
import configparser  # for configuration file parsing
import itertools  # for iterating through each possible config combination
import tempfile  # For writing configuration files for each benchmark

import genomics_benchmarks


def product_params(inp):
    return (dict(zip(inp.keys(), itervalues)) for itervalues in itertools.product(*inp.values()))


def configparser_to_dict(config):
    dict = {}
    for section in config.sections():
        dict[section] = {}
        for key, value in config[section].items():
            dict[section][key] = value
    return dict


if __name__ == '__main__':
    # Extract arguments from CLI
    parser = argparse.ArgumentParser(description='A parameter sweeper for genomics-benchmarks exec mode.')
    parser.add_argument('--base_config', type=str, required=True,
                        help='Specifies the location for the base configuration file.')
    parser.add_argument('--sweep_config', type=str, required=True,
                        help='Specifies the location for all parameters to sweep through.')
    parser.add_argument('--section_name', type=str, required=True,
                        help='Specifies the [section] to use within the parameter sweep configuration file.')

    runtime_config = vars(parser.parse_args())

    # Read in the specified base config file
    base_config_location = runtime_config['base_config']
    base_config = configparser.ConfigParser()
    if os.path.isfile(base_config_location):
        base_config.read(base_config_location)
    else:
        print('Error: Base config file does not exist on filesystem. Exiting...')
        sys.exit(1)
    base_config_dict = configparser_to_dict(base_config)

    # Read in the specified parameter sweep config file
    sweep_config_location = runtime_config['sweep_config']
    sweep_config = configparser.ConfigParser()
    if os.path.isfile(sweep_config_location):
        sweep_config.read(sweep_config_location)
    else:
        print('Error: Sweep config file does not exist on filesystem. Exiting...')
        sys.exit(1)

    # Extract and display all parameters to sweep through
    sweep_section_name = runtime_config['section_name']
    sweep_params = {}
    print('Parameters to sweep through within section [{}]: '.format(sweep_section_name))
    for section in sweep_config:
        if section == sweep_section_name:
            print('[{}]'.format(section))
            for key in sweep_config[section]:
                # Extract all parameters for the specified key
                values = sweep_config[section][key].split(',')
                sweep_params[key] = values

                is_base_key_overridden = section in base_config and key in base_config[section]
                print('  {} {}: {}'.format('*' if is_base_key_overridden else ' ',
                                           key,
                                           values))
        else:
            print('(Ignoring section [{}])'.format(section))
    print('Note: Parameters preceded by an asterisk also exist in the base configuration file and will be ignored.')

    # Construct a list of all possible parameter combinations
    benchmark_params, benchmark_params_count = itertools.tee(product_params(sweep_params))
    benchmark_params_count = sum(1 for _ in benchmark_params_count)  # Get total number of parameter combinations
    print('Total number of benchmarks to run: {}'.format(benchmark_params_count))

    # Create a merged config file and run genomics-benchmarks for each parameter sweep set
    counter = 0
    for params in benchmark_params:
        print('Running benchmark {}/{}'.format(counter + 1, benchmark_params_count))

        # Create a new config file for this benchmark (merge sweep params with base config)
        merged_config = configparser.ConfigParser()
        merged_config.read_dict(base_config_dict)  # Read in base config parameters

        if sweep_section_name not in merged_config.sections():
            merged_config[sweep_section_name] = {}

        # Merge sweep params
        for key, value in params.items():
            merged_config[sweep_section_name][key] = value

        with tempfile.NamedTemporaryFile(mode='w', delete=False) as merged_config_file:
            print(merged_config_file.name)
            merged_config.write(merged_config_file)  # Write merged config data to temporary file
            merged_config_file.flush()

            # Run genomics-benchmarks
            benchmark_label = 'parametersweep_results'
            benchmark_args = ['genomics-benchmarks', 'exec',
                              '--config_file', merged_config_file.name,
                              '--label', benchmark_label]
            with patch.object(sys, 'argv', benchmark_args):
                genomics_benchmarks.main()

            # Close and remove the temporary configuration file
            merged_config_file.close()
            os.remove(merged_config_file.name)

        counter += 1

    print('Finished. Exiting...')
    sys.exit(0)
