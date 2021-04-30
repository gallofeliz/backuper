#!/usr/bin/env python3

import rpyc
import click
from tabulate import tabulate

def get_remote():
    conn = rpyc.connect("localhost", 18812, config={'allow_all_attrs': True, "allow_public_attrs":True, "sync_request_timeout": None})
    return conn.root

@click.group()
def cli():
    pass

@click.command(name='list-snapshots')
@click.argument('repository')
@click.option('-b', '--backup')
@click.option('-h', '--hostname')
def list_snapshots(repository, backup, hostname):
    snapshots = get_remote().list_snapshots(
        repository_name=repository.lower(),
        backup_name=backup.lower() if backup else backup,
        hostname=hostname.lower() if hostname else hostname
    )
    if not len(snapshots):
        click.echo('No snapshots')
        return
    click.echo(tabulate(snapshots, headers="keys", tablefmt="github"))

@click.command(name='restore-snapshot')
@click.argument('repository')
@click.argument('snapshot')
@click.option('-d', '--target-path')
@click.option('-p', '--priority', type=click.Choice(['normal', 'next', 'immediate']), default='normal')
@click.option('-w', '--wait-done/--no-wait-done', default=False)
#@click.option('--force-run-on-timeout')
def restore_snapshot(repository, snapshot, target_path, priority, wait_done):
    get_remote().restore_snapshot(
        repository_name=repository.lower(),
        snapshot=snapshot,
        target_path=target_path,
        priority=priority,
        get_result=wait_done
    )
    if wait_done:
        click.echo('Restore ended')
    else:
        click.echo('Restore requested')

@click.command(name='check-repository')
@click.argument('repository')
@click.option('-p', '--priority', type=click.Choice(['normal', 'next', 'immediate']), default='normal')
def check_repository(repository, priority):
    get_remote().check_repository(
        repository_name=repository.lower(),
        priority=priority
    )

    click.echo('Check requested')

def download_snapshot():
    #restic -r /srv/restic-repo dump latest /home/other/work > restore.tar
    pass

cli.add_command(list_snapshots)
cli.add_command(restore_snapshot)
cli.add_command(check_repository)
cli()
