# -*- coding: utf-8 -*-

import elasticsearch
from elasticsearch_dsl import Search,Q
import pandas as pd

def get_latest_commits(es_instance,commit_index,blame_index):
        es = es_instance
        es_ma_index = commit_index
        es_bl_index = blame_index
        blame_es_data = Search(using=es, index=es_bl_index)
        blame_dict = [hit.to_dict() for hit in blame_es_data.scan()]
        commit_es_data = Search(using=es, index=es_ma_index)
        commit_dict = [hit.to_dict() for hit in commit_es_data.scan()]

        commit_frame = pd.DataFrame(commit_dict)
        blame_frame = pd.DataFrame(blame_dict)
        blame_frame['type'] = 'Buggy'
        comb_frame = pd.merge(commit_frame,blame_frame,how='left',left_on = ['hash','file_path'],right_on = ['blame_hash','file'])
        comb_frame['type'] = comb_frame['type'].fillna('Clean')
        comb_frame_refined = comb_frame[['hash', 'Author', 'Email', 'message',                               'committed_date', 'no._of_branches', 'merge_commit?',
                                        'no._of_mod_files', 'dmm_unit_size', 'dmm_unit_complexity','dmm_unit_interfacing',
                                        'file_path', 'complexity','functions', 'lines_added', 'lines_removed', 'size', 'tokens',
                                        'type']]
        comb_frame_refined['hash'] = comb_frame_refined['hash'].str.slice(0,10)

        commit_frame['committed_date'] = commit_frame['committed_date'].astype('str').apply(lambda x: pd.to_datetime(x).tz_convert('US/Pacific'))

        comb_frame_refined = comb_frame_refined.sort_values('committed_date', ascending=False).reset_index().drop(columns = 'index')
        return comb_frame_refined.head(50)
