import rpyc
import threading
from restic import kill_all_restics, call_restic
from gallocloud_utils.scheduling import schedule, create_scheduler
from gallocloud_utils.convertions import convert_to_seconds, convert_to_KiB
from gallocloud_utils.fswatcher import create_fswatch_callback
from gallocloud_utils.tasks import Task, TaskManager
from shlex import quote

class Daemon(rpyc.Service):
    def __init__(self, config, logger):
        self._config = config
        self._logger = logger
        self._started = False
        self._task_manager = TaskManager(self._logger)
        self._rpc = rpyc.ThreadedServer(service=self, port=18812, protocol_config={'allow_all_attrs': True, "allow_public_attrs":True})
        self._schedules = []
        self._fswatchers = []

    def start(self):
        if self._started:
            return

        self._logger.info('Starting Daemon', extra={
            'component': 'daemon',
            'action': 'start',
            'status': 'starting'
        })

        self._task_manager.run()
        threading.Thread(target=self._rpc.start).start()

        scheduler = create_scheduler()
        config = self._config

        for repository_name in config['repositories']:
            repository = config['repositories'][repository_name]
            self.init_repository(repository_name)

            if repository['check'].get('schedule'):
                self._schedules.append(
                    schedule(
                        repository['check']['schedule'],
                        self.check_repository,
                        kwargs={
                            'repository_name': repository_name
                        },
                        runAtBegin=True,
                        scheduler=scheduler,
                        on_error=self._logger.exception
                    )
                )

            for backup_name in config['backups']:
                backup = config['backups'][backup_name]

                if backup['schedule']:
                    self._schedules.append(
                        schedule(
                            backup['schedule'],
                            self.backup,
                            kwargs={
                                'backup_name': backup_name
                            },
                            runAtBegin=True,
                            scheduler=scheduler,
                            on_error=self._logger.exception
                        )
                    )

                if backup['watch']:
                    self._fswatchers.append(
                        create_fswatch_callback(**{
                            'paths':backup['paths'],
                            'ignore':backup['excludes'],
                            'fn': self.backup,
                            'kwargs': {
                                'backup_name': backup_name
                            },
                            'logger': self._logger,
                            'on_error': self._logger.exception,
                            **({ 'wait_min': convert_to_seconds(backup['watchwait'][0]), 'wait_max': convert_to_seconds(backup['watchwait'][1]) } if backup['watchwait'] else {})
                        })
                    )

        scheduler.run()

    def stop(self):
        if not self._started:
            return

        self._task_manager.stop()
        kill_all_restics()
        self._rpc.close()

        for schedule_stop in self._schedules:
            schedule_stop()

    def list_snapshots(self, repository_name, hostname, backup_name, priority='immediate'):
        self._logger.info('list_snapshots requested', extra={
            'component': 'daemon',
            'action': 'list_snapshots',
            'status': 'queuing'
        })

        def do_list_snapshots():
            self._logger.info('list_snapshots starting', extra={
                'component': 'daemon',
                'action': 'list_snapshots',
                'status': 'starting'
            })

            try:
                args=self._get_restic_global_opts()
                if backup_name:
                    args = args + ['--tag', 'backup-' + backup_name.lower()]
                if hostname:
                    args = args + ['--host', hostname.lower()]
                response = call_restic(cmd='snapshots', args=args, env=self._get_restic_repository_envs(self._config['repositories'][repository_name.lower()]), logger=self._logger, json=True)
                restic_snapshots = response['stdout']
                snapshots = []

                for raw_snapshot in restic_snapshots:
                    _backup_name = None
                    for tag in raw_snapshot['tags']:
                        if tag[0:7] == 'backup-':
                            _backup_name = tag[7:]
                    snapshots.append({
                        'Date': raw_snapshot['time'],
                        'Id': raw_snapshot['id'],
                        'Hostname': raw_snapshot['hostname'],
                        'Backup': _backup_name
                    })

                self._logger.info('list_snapshots success', extra={
                    'component': 'daemon',
                    'action': 'list_snapshots',
                    'status': 'success'
                })

                return snapshots
            except Exception as e:
                self._logger.info('list_snapshots failed', extra={
                    'component': 'daemon',
                    'action': 'list_snapshots',
                    'status': 'failure'
                })

                raise e

        return self._task_manager.add_task(
            task=Task(fn=do_list_snapshots, priority=priority),
            ignore_if_duplicate=False,
            get_result=True
        )

    def init_repository(self, repository_name, priority='next'):
        repository = self._config['repositories'][repository_name]

        self._logger.info('init_repository requested', extra={
            'component': 'daemon',
            'action': 'init_repository',
            'repository': repository['name'],
            'status': 'queuing'
        })

        def do_init_repository():
            self._logger.info('Starting repository initialization', extra={
                'component': 'daemon',
                'action': 'init_repository',
                'repository': repository['name'],
                'status': 'starting'
            })

            try:
                call_restic(cmd='init', args=self._get_restic_global_opts(), env=self._get_restic_repository_envs(repository), logger=self._logger)
                self._logger.info('Initialization ended :)', extra={
                    'component': 'daemon',
                    'action': 'init_repository',
                    'repository': repository['name'],
                    'status': 'success'
                })
            except Exception as e:
                self._logger.info('Unable to init ; probably already init else error ('+str(e)+')', extra={
                    'component': 'daemon',
                    'action': 'init_repository',
                    'repository': repository['name'],
                    'status': 'failure'
                })

        return self._task_manager.add_task(
            task=Task(fn=do_init_repository, priority=priority),
            ignore_if_duplicate=True,
            get_result=False
        )

    def check_repository(self, repository_name, priority=None):
        repository = self._config['repositories'][repository_name]

        self._logger.info('check_repository requested', extra={
            'component': 'daemon',
            'action': 'check_repository',
            'repository': repository['name'],
            'status': 'queuing'
        })

        if not priority:
            priority = repository.get('check', {}).get('priority', 'normal')

        def do_check_repository():
            self._logger.info('Starting check', extra={
                'component': 'daemon',
                'action': 'check_repository',
                'repository': repository['name'],
                'status': 'starting'
            })
            try:
                call_restic(cmd='check', args=self._get_restic_global_opts(), env=self._get_restic_repository_envs(repository), logger=self._logger)
                self._logger.info('Check ended :)', extra={
                    'component': 'daemon',
                    'action': 'check_repository',
                    'repository': repository['name'],
                    'status': 'success'
                })
            except Exception as e:
                self._logger.exception('Check failed :(', extra={
                    'component': 'daemon',
                    'action': 'check_repository',
                    'repository': repository['name'],
                    'status': 'failure'
                })

        self._task_manager.add_task(
            task=Task(fn=do_check_repository, priority=priority),
            ignore_if_duplicate=True,
            get_result=False
        )

    def restore_snapshot(self, repository_name, snapshot, target_path=None, priority='normal', get_result=False):
        if not target_path:
            target_path = '/'

        repository = self._config['repositories'][repository_name]

        self._logger.info('restore_snapshot requested', extra={
            'component': 'daemon',
            'action': 'restore_snapshot',
            'repository': repository['name'],
            'snapshot': snapshot,
            'status': 'queuing'
        })

        def do_restore_snapshot():
            self._logger.info('Starting restore', extra={
                'component': 'daemon',
                'action': 'restore_snapshot',
                'repository': repository_name,
                'snapshot': snapshot,
                'status': 'starting'
            })
            try:
                args = [snapshot]
                args = args + ['--target', target_path]
                args = args + self._get_restic_global_opts()
                call_restic(cmd='restore', args=args, env=self._get_restic_repository_envs(repository), logger=self._logger)
                self._logger.info('Restore ended', extra={
                    'component': 'daemon',
                    'action': 'restore_snapshot',
                    'repository': repository_name,
                    'snapshot': snapshot,
                    'status': 'success'
                })
            except Exception as e:
                self._logger.exception('Restore failed', extra={
                    'component': 'daemon',
                    'action': 'restore_snapshot',
                    'repository': repository_name,
                    'snapshot': snapshot,
                    'status': 'failure'
                })

        self._task_manager.add_task(
            task=Task(fn=do_restore_snapshot, priority=priority),
            ignore_if_duplicate=True,
            get_result=get_result
        )

    def backup(self, backup_name, priority=None):
        backup = self._config['backups'][backup_name]

        self._logger.info('backup requested', extra={
            'component': 'daemon',
            'action': 'backup',
            'backup': backup['name'],
            'status': 'queuing'
        })

        if not priority:
            priority = backup.get('priority', 'normal')

        def do_backup():
            self._logger.info('Starting backup', extra={
                'component': 'daemon',
                'action': 'backup',
                'backup': backup['name'],
                'status': 'starting'
            })
            success = True
            for repository in backup['repositories']:
                self._logger.info('Starting backup on repository', extra={
                    'component': 'daemon',
                    'action': 'backup',
                    'backup': backup['name'],
                    'repository': repository['name'],
                    'status': 'starting'
                })
                try:
                    options = ['--tag', quote('backup-' + backup['name']), '--host', self._config['hostname']] + self._get_restic_global_opts(backup)
                    args = backup['paths'] + list(map(lambda exclude : '--exclude=' + quote(exclude), backup['excludes']))
                    call_restic(cmd='backup', args=options + args, env=self._get_restic_repository_envs(repository), logger=self._logger)
                    self._logger.info('Backup on repository ended :)', extra={
                        'component': 'daemon',
                        'action': 'backup',
                        'backup': backup['name'],
                        'repository': repository['name'],
                        'status': 'success'
                    })
                except Exception as e:
                    self._logger.exception('Backup on repository failed :(', extra={
                        'component': 'daemon',
                        'action': 'backup',
                        'backup': backup['name'],
                        'repository': repository['name'],
                        'status': 'failure'
                    })
                    success = False

            if success:
                self._logger.info('Backup ended :)', extra={
                    'component': 'daemon',
                    'action': 'backup',
                    'backup': backup['name'],
                    'status': 'success'
                })
            else:
                self._logger.error('Backup failed (one or more backup in repository failed) :(', extra={
                    'component': 'daemon',
                    'action': 'backup',
                    'backup': backup['name'],
                    'status': 'failure'
                })

        self._task_manager.add_task(
            task=Task(fn=do_backup, priority=priority),
            ignore_if_duplicate=True,
            get_result=False
        )


    def _get_restic_repository_envs(self, repository):
        return {
            'RESTIC_REPOSITORY': repository['location'],
            'RESTIC_PASSWORD': repository['password'],
            **repository['providerEnv']
        }

    def _get_restic_global_opts(self, overrides = {}):
        options = []
        config = self._config

        uploadlimit = overrides.get('uploadlimit', config.get('uploadlimit'))
        downloadlimit = overrides.get('downloadlimit', config.get('downloadlimit'))

        if uploadlimit and uploadlimit != 'None':
            options.extend(['--limit-upload', str(convert_to_KiB(uploadlimit))])
        if downloadlimit and uploadlimit != 'None':
            options.extend(['--limit-download', str(convert_to_KiB(downloadlimit))])
        return options

