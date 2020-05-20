import elasticsearch
from elasticsearch_dsl import Search,Q
import pandas as pd

def get_user_data(es_instance,commit_index,blame_index):
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
        comb_frame_refined = comb_frame[['hash', 'Author','Committer', 'Email', 'message',                               'committed_date', 'no._of_branches', 'merge_commit?',
                                        'no._of_mod_files', 'dmm_unit_size', 'dmm_unit_complexity','dmm_unit_interfacing',
                                        'file_path', 'complexity','functions', 'lines_added', 'lines_removed', 'size', 'tokens',
                                        'type']]
        comb_frame_refined['hash'] = comb_frame_refined['hash'].str.slice(0,10)

        
        commit_frame['committed_date'] = commit_frame['committed_date'].astype('str').apply(lambda x: pd.to_datetime(x).tz_convert('US/Pacific'))

        comb_frame_refined = comb_frame_refined.sort_values('committed_date', ascending=False)
        comb_frame_refined = comb_frame_refined.fillna(0)
        comb_frame_refined['commit_count'] = comb_frame_refined.groupby('Author').hash.transform('nunique')
        comb_frame_refined['riskfreescore'] = comb_frame_refined['dmm_unit_size']+comb_frame_refined['dmm_unit_complexity']+comb_frame_refined['dmm_unit_interfacing']
        comb_frame_refined['lineschanged'] = comb_frame_refined['lines_added']+comb_frame_refined['lines_removed']
        comb_frame_refined['score'] = comb_frame_refined.apply(lambda x:((x.riskfreescore)*10+(x.lineschanged)+(x.complexity)*10)/10,axis =1)
        dev_score = comb_frame_refined[['Author','commit_count','score','type']]
        dev_score['scaled_score'] = dev_score.apply(lambda x: x.score if x.type == 'Clean' else x.score/2,axis=1)
        dev_score_refined = dev_score[['Author','commit_count','scaled_score']]
        dev_score_refined['average_score'] = dev_score_refined.groupby('Author').scaled_score.transform('mean').round()
        dev_score_final = dev_score_refined[['Author','commit_count','average_score']].drop_duplicates().sort_values('commit_count', ascending=False).reset_index().drop(columns = 'index') 
        return dev_score_final
