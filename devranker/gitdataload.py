import elasticsearch
from elasticsearch_dsl import Search,Q
import pandas as pd
import git
import re
from pydriller import RepositoryMining
from elasticsearch import helpers
from git import Repo
def store_commit_data(local_dir,es,es_index,es_blame_index,local_commit,remote_commit):
    
    repo = Repo(local_dir)
    # Creating empty lists for carrying commit data
    doclist = []
    blamelist =[]
    # If the Repo has just been cloned, the program will traverse the whole Repo
    if(local_commit == 'None'):
        # using PyDriller's API.
        for commit in RepositoryMining(local_dir).traverse_commits():
               for mod in commit.modifications:
                    commit_data = {'hash':commit.hash,'Author':commit.author.name,'Email':commit.author.email,
                                       'message':commit.msg,'authored_date':commit.author_date,
                                       'Committer':commit.committer.name,'committed_date':commit.committer_date,
                                       'no._of_branches':len(commit.branches),'merge_commit?':commit.merge,
                                       'no._of_mod_files':len(commit.modifications),'dmm_unit_size':commit.dmm_unit_size,
                                       'dmm_unit_complexity':commit.dmm_unit_complexity,'dmm_unit_interfacing':commit.dmm_unit_interfacing,
                                       'file_name':mod.filename, 'file_path':mod.new_path, 'complexity': mod.complexity, 'functions': len(mod.methods),
                                       'lines_added':mod.added,'lines_removed': mod.removed,'loc':mod.nloc,'size': 0 if mod.source_code is None else len(mod.source_code.splitlines()),'tokens':mod.token_count
                                       
                                        }
                    # loading each commit and it's modified components into the list
                    doclist.append(commit_data)
                    alines = mod.diff_parsed['added']
                    addedlines = [x[1] if len(alines)>0 else 'None' for x in alines ]
                    
                    blines = mod.diff_parsed['deleted']
                    deletedlines = [x[1] if len(blines)>0 else 'None' for x in blines]
                    # For bug fix commits, retrieving the blame data
                    if len(re.findall(r"\bbug\b|\bBug\b|\bFix\b|\bfix\b",commit.msg))>0:# & len(addedlines)>0:
                        for eachline in addedlines:
                            
                            # Reading blame log for each modified line in a bugfix commit
                            repo_blame = repo.blame(commit.hash,mod.new_path,eachline)
                            for blame_record in repo_blame:
                                prev_record = ''
                                
                                
                                if str(commit.hash) !=str(blame_record[0]) and (str(blame_record[0]) != prev_record):

                                    blame_doc = {'orig_hash':commit.hash,'blame_hash':str(blame_record[0]),
                                                'file':mod.new_path}    
                                    # Loading blame data into the list
                                    blamelist.append(blame_doc)
                                    prev_record = blame_record[0]
                        
                   

    else:
        #if Repo has been refreshed, only delta commits are processed
        for commit in RepositoryMining(local_dir,from_commit = local_commit, to_commit = remote_commit ).traverse_commits():
            for mod in commit.modifications:
               
                commit_data = {'hash':commit.hash,'Author':commit.author.name,'Email':commit.author.email,
                               'message':commit.msg,'authored_date':commit.author_date,
                               'Committer':commit.committer.name,'committed_date':commit.committer_date,
                               'no._of_branches':len(commit.branches),'merge_commit?':commit.merge,
                               'no._of_mod_files':len(commit.modifications),'dmm_unit_size':commit.dmm_unit_size,
                               'dmm_unit_complexity':commit.dmm_unit_complexity,'dmm_unit_interfacing':commit.dmm_unit_interfacing,
                               'file_name':mod.filename, 'file_path':mod.new_path, 'complexity': mod.complexity, 'functions': len(mod.methods),
                               'lines_added':mod.added,'lines_removed': mod.removed,'loc':mod.nloc,'size': 0 if mod.source_code is None else len(mod.source_code.splitlines()),'tokens':mod.token_count
                               
                              }
                doclist.append(commit_data)
                alines = mod.diff_parsed['added']
                addedlines = [x[1] if len(alines)>0 else 'None' for x in alines ]
                
                blines = mod.diff_parsed['deleted']
                deletedlines = [x[1] if len(blines)>0 else 'None' for x in blines]
                if len(re.findall(r"\bbug\b|\bBug\b|\bFix\b|\bfix\b",commit.msg))>0:# & len(addedlines)>0:
                    for eachline in addedlines:
                        
                        repo_blame = repo.blame(commit.hash,mod.new_path,eachline)
                        for blame_record in repo_blame:
                            prev_record = ''
                            
                            if str(commit.hash) !=str(blame_record[0]) and (str(blame_record[0]) != prev_record):
                                
                                blame_doc = {'orig_hash':commit.hash,'blame_hash':str(blame_record[0]),
                                            'file':mod.new_path}                                     
                                blamelist.append(blame_doc)
                                prev_record = blame_record[0]
                        
                        
    # using elasticsearch.py's helper tools to bulk load into elasticsearch's commit index           
    helpers.bulk(es,doclist,index=es_index,doc_type ='commit_data',request_timeout = 2000)
    # Since Git Blame produces duplicate data, getting only unique records
    blamelist_fil = [i for n, i in enumerate(blamelist) if i not in blamelist[n + 1:]]
    # using elasticsearch.py's helper tools to bulk load into elasticsearch's blame index
    helpers.bulk(es,blamelist_fil,index=es_blame_index,doc_type ='blame',request_timeout = 2000)# -*- coding: utf-8 -*-

