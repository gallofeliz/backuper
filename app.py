#!/usr/bin/env python

import subprocess, threading, logging, sched, os
from shlex import quote

logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(message)s')

class CallResticError(Exception):
    def __init__(self, result):
        self.result = result
        self.message = 'Call Restic Error'
    def get_result(self):
        return self.result

def call_restic(cmd, args = []):
    log_template = '(RESTIC) (%s) - %s'
    cmd_parts = ["restic"] + [cmd] + args
    logging.debug(log_template, 'START', ' '.join(cmd_parts))
    proc = subprocess.Popen(
        cmd_parts,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )

    out=[] # only last to avoid memory boooom ?
    err=[] # only last to avoid memory boooom ?

    def log(stream, channel, stack):
        for rline in iter(stream.readline, ''):
            line = rline.rstrip()
            if line:
                logging.debug(log_template, channel, line)
                stack.append(line)


    threading.Thread(target=log, args=(proc.stdout, 'STDOUT', out,)).start()
    threading.Thread(target=log, args=(proc.stderr, 'STDERR', err,)).start()
    code = proc.wait()

    logging.debug(log_template, 'EXIT', str(code))

    result = {
        'code': code,
        'stdout': out,
        'stderr': err
    }

    if code > 0:
        raise CallResticError(result)

    return result

def convert_to_seconds(duration):
    seconds_per_unit = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
    return int(duration[:-1]) * seconds_per_unit[duration[-1]]

def load_config():
    config = {
        'backups': [],
        'check': {
            'frequency': os.environ['APP_CHECK_FREQUENCY']
        }
    }

    env_backups = {}

    for k, v in os.environ.items():
        if k[0:11] == 'APP_BACKUP_':
            name, *rest = k[11:].split('_')
            name = name.lower()
            rest = '_'.join(rest)

            if name not in env_backups:
                env_backups[name] = {}

            env_backups[name][rest] = v

    for name in env_backups:
        values = env_backups[name]
        backup_config = {
            'name': name,
            'paths': values['PATHS'].split(','),
            'excludes': values['EXCLUDES'].split(',') if 'EXCLUDES' in values else [],
            'frequency': values['FREQUENCY'],
            #'retryFrequency': values['RETRY_FREQUENCY'] if 'RETRY_FREQUENCY' in values else '30m'
        }
        config['backups'].append(backup_config)

    return config


# action target message
log_template = '(APP) (%s) (%s) - %s'
scheduler = sched.scheduler()
config = load_config()

logging.info(log_template, 'MAIN', 'GLOBAL', 'Starting APP with config ' + str(config))

def init():
    logging.info(log_template, 'INIT', 'GLOBAL', 'Starting repository initialization')
    try:
        call_restic('init')
        logging.info(log_template, 'INIT', 'GLOBAL', 'Initialization ended :)')
    except Exception as e:
        logging.info(log_template, 'INIT', 'GLOBAL', 'Unable to init :( ; skipping')

def schedule_backups():
    logging.info(log_template, 'BACKUP', 'GLOBAL', 'Scheduling backups')
    def backup(backup_config):
        logging.info(log_template, 'BACKUP', backup_config['name'], 'Starting backup')
        scheduler.enter(convert_to_seconds(backup_config['frequency']), 1, backup, (backup_config,))
        try:
            call_restic('backup', ['--tag', quote('backup-' + backup_config['name'])] + backup_config['paths'] + list(map(lambda exclude : '--exclude=' + quote(exclude), backup_config['excludes'])))
            logging.info(log_template, 'BACKUP', backup_config['name'], 'Backup ended :)')
            #scheduler.enter(convert_to_seconds(backup_config['frequency']), 1, backup, (backup_config,))
        except Exception as e:
            logging.exception(log_template, 'BACKUP', backup_config['name'], 'Backup failed :(')
            #logging.exception(log_template, 'BACKUP', backup_config['name'], 'Backup failed :( ; will retry later')
            #scheduler.enter(convert_to_seconds(backup_config['retryFrequency']), 1, backup, (backup_config,))
    for backup_config in config['backups']:
        backup(backup_config)

def schedule_check():
    logging.info(log_template, 'CHECK', 'GLOBAL', 'Scheduling check')
    def check():
        logging.info(log_template, 'CHECK', 'GLOBAL', 'Starting check')
        scheduler.enter(convert_to_seconds(config['check']['frequency']), 1, check)
        try:
            call_restic('check')
            logging.info(log_template, 'CHECK', 'GLOBAL', 'Check ended :)')
        except Exception as e:
            logging.exception(log_template, 'CHECK', 'GLOBAL', 'Check failed :(')
    check()


init()
schedule_backups()
schedule_check()
scheduler.run()

# def init():
#     logging.info(log_template, 'INIT', 'Starting repository initialization')
#     @retry(wait_fixed=convert_to_seconds('30m') * 1000)
#     def try_init():
#         try:
#             call_restic('init')
#         #except CallResticError as e:
#         except Exception as e:
#             logging.exception(log_template, 'INIT', 'Unable to init ; will retry later')
#             raise e
#     try_init()
#     logging.info(log_template, 'INIT', 'Initialization ended')


# init()
# import subprocess
# from subprocess import CalledProcessError
# from retrying import retry
# import time
# from time import sleep
# import sched
# import os
# from shlex import quote
# import requests
# import threading
# import logging

#


# def convert_to_mseconds(duration):
#     return convert_to_seconds(duration) * 1000

# class Restic():
#     def init(self):
#         self.__run('init')

#     def backup(self, paths, excludes = []):
#         args = paths + map(lambda exclude : '--exclude=' + quote(exclude), excludes)
#         self.__run('backup', args)

#     def check():
#         self.__run('check')

#     def is_init(self):
#         try:
#             self.__run('check', [], True)
#             return True
#         except CalledProcessError as e:
#             if e.stderr.find('conn.Object: Object Not Found') != -1:
#                 return False
#             raise e

#     # Need to retry everytime ? Only if a network problem, etc, but I can't identify it, so for now I comment retry
#     # @retry(wait_exponential_multiplier=convert_to_mseconds('5s'), wait_exponential_max=convert_to_mseconds('1m'), stop_max_attempt_number=5)
#     def __run(self, cmd, args = [], capture_output = False):
#         logging.debug('RESTIC START %s %s %s', cmd, 'with args', args)
#         try:
#             process_cmd_parts = ["restic"] + [cmd] + args
#             process = subprocess.run(process_cmd_parts, capture_output=capture_output, check=True, text=True)
#             logging.debug('RESTIC SUCCESS %s %s %s', process.returncode, process.stdout, process.stderr)
#             return process
#         except CalledProcessError as e:
#             logging.debug('RESTIC ERROR %s %s %s', e.returncode, e.stdout, e.stderr)
#             raise e

# logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(message)s')
# restic = Restic()
# scheduler = sched.scheduler()
# list_separator = ','
# backup_paths = os.environ['BACKUP_PATHS'].split(list_separator)
# backup_exclude = os.environ['BACKUP_EXCLUDE'].split(list_separator)
# backup_schedule = convert_to_seconds(os.environ['BACKUP_SCHEDULE'])
# check_schedule = convert_to_seconds(os.environ['CHECK_SCHEDULE'])

# @retry(wait_fixed=convert_to_mseconds('30m'))
# def init():
#     try:
#         logging.info('Initializing...')
#         if not restic.is_init():
#             restic.init()
#         logging.info('Initialized !')
#     except Exception as e:
#         logging.exception('Init error, will retry later')
#         raise e

# def schedule():
#     backup()
#     scheduler.enter(check_schedule, 1, check)
#     scheduler.run()

# @retry(wait_fixed=convert_to_mseconds('30m'))
# def check():
#     try:
#         logging.info('Checking...')
#         restic.check()
#         logging.info('Check done !')
#         scheduler.enter(check_schedule, 1, check)
#     except Exception as e:
#         logging.exception('Check error, will retry later')
#         raise e

# @retry(wait_fixed=convert_to_mseconds('30m'))
# def backup():
#     try:
#         logging.info('Backuping...')
#         restic.backup(backup_paths, excludes=backup_exclude)
#         logging.info('Backup done !')
#         scheduler.enter(backup_schedule, 1, backup)
#     except Exception as e:
#         logging.exception('Backup error, will retry later')
#         raise e

# init()
# schedule()
