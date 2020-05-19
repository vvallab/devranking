import elasticsearch
import git
import os
from git import Repo
import sys
from .gitdataload import store_commit_data
def create_components(git_url,esurl,localdir):
    # if no url supplied for Elastic, assume the localhost
    if esurl =='':
        try:
            es = elasticsearch.Elasticsearch(['http://localhost:9200/'])
        except:
            print('Elasticsearch not running at localhost:9200')
            sys.exit(1)
    else:
        try:
            es = elasticsearch.Elasticsearch([esurl])
        except:
            print('Elasticsearch not running at the given URL. For default localhost, do not provide the argument')
            sys.exit(1)
        
    # Get the default commit index name
    es_index_raw = str.split(git_url,'/')[-1].split('.')[0]+'_'+'index'
    # Get the default blame index name
    es_blame_index_raw = str.split(git_url,'/')[-1].split('.')[0]+'_'+'blame'+'_'+'index'
    es_index = es_index_raw.lower()
    es_blame_index = es_blame_index_raw.lower()
    # Create elasticsearch instance
    
    # If local Repo path is not supplied, create default path in '/tmp'
    if localdir == '':
        if sys.platform == 'linux':
            local_dir ='/tmp/'+str.split(git_url,'/')[-1].split('.')[0]
        else:
            local_dir ='C:\\Downloads'+str.split(git_url,'/')[-1].split('.')[0]
    else:
        local_dir = localdir
    # Check if the local Repo already exists
    if os.path.isdir(local_dir):
        # Load the local Repo
        try:
            repo = git.Repo(local_dir)   
        # Get the latest commit object in the local Repo
            local_commit = repo.commit()     
        except:
            print('No valid Repo found at the location. If unsure, remove the directory and try without local dir argument')
            sys.exit(1)
                       # latest local commit 
        
        # Get the latest commit object in the remote Repo
        remote = git.remote.Remote(repo, 'origin')      # remote repo
        info = remote.fetch()[0]                        # fetch changes
        remote_commit = info.commit  
        
        # If latest commit in local and remote differ refresh the local Repo
        if (local_commit.hexsha == remote_commit.hexsha ):
            print('No changes in the Repo...')
        else:    
            repo = git.Repo(local_dir) 
            o = repo.remotes.origin
            o.pull()
            # Analyse and store additional commit data
            store_commit_data(local_dir,es,es_index,es_blame_index,local_commit.hexsha,remote_commit.hexsha )
    else:
        # If no local Repo exists, clone the Repo
        try:
            if sys.platform == 'linux':
                git.Git('/tmp').clone(git_url)
            else:
                git.Git('C:\\Downloads').clone(git_url)
        except:
            print('Not able to clone the Repo. If there is a non Git directory with the  same name, delete it and then try')
            sys.exit(1)
        # Delete the elastic indices, if exist
        es.indices.delete(index=es_index, ignore=[400, 404])
        es.indices.delete(index=es_blame_index, ignore=[400, 404])
        # Create new elastic indices
        es.indices.create(es_index)
        es.indices.create(es_blame_index)
        # Call the function to store the necessary commit data
        store_commit_data(local_dir,es,es_index,es_blame_index,'None','None')

    return es,es_index,es_blame_index
