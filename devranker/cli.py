
"""This module contains cli commands to get Developer Ranking data"""

import sys
import click

from .gitlanding import create_components
from .gitlatest import get_latest_commits
from .gituserdata import get_user_data


@click.group()
#@click.command()
def cli():
    #click.echo("Hello World")
    pass



@cli.command()
@click.argument('git_url')
@click.option('--es_url', '-es', default='', help='Enter the URL for Elasticsearch instance')
@click.option('--local_dir', '-lr', default='', help='Path to clone to')
def fulldata(git_url,local_dir,es_url):
    comp = create_components(git_url, local_dir,es_url)
    es_index = comp[0]
    commit_index = comp[1]
    blame_index = comp[2]
    data = get_latest_commits(es_index, commit_index, blame_index)
    print(data)
    
    
        
@cli.command()
@click.argument('git_url')
@click.option('--es_url', '-es', default='', help='Enter the URL for Elasticsearch instance')
@click.option('--local_dir', '-lr', default='', help='Path to clone to')
def devscores(git_url,local_dir,es_url): 
    comp = create_components(git_url, local_dir,es_url)
    es_index = comp[0]
    commit_index = comp[1]
    blame_index = comp[2]
    data = get_user_data(es_index,commit_index,blame_index)
    print(data)
            




