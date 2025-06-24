import pandas as pd

def extract_commit_type(message):
    if pd.isna(message):
        return 'other'
    message = str(message).lower()
    if message.startswith('feat'):
        return 'feat'
    elif message.startswith('fix'):
        return 'fix'
    elif message.startswith('docs'):
        return 'docs'
    elif message.startswith('style'):
        return 'style'
    elif message.startswith('refactor'):
        return 'refactor'
    elif message.startswith('test'):
        return 'test'
    elif message.startswith('chore'):
        return 'chore'
    else:
        return 'other'

def get_commit_type_display_names():
    return {
        'feat': 'Features',
        'fix': 'Correções',
        'docs': 'Documentação',
        'style': 'Estilo',
        'refactor': 'Refatoração',
        'test': 'Testes',
        'chore': 'Tarefas',
        'other': 'Outros'
    }

def process_commits_data(commits_df):
    """Process commits dataframe by adding date columns and commit types"""
    if 'date' in commits_df.columns:
        commits_df['date'] = pd.to_datetime(commits_df['date'])
        commits_df['date_only'] = commits_df['date'].dt.date
    
    if 'message' in commits_df.columns:
        commits_df['commit_type'] = commits_df['message'].apply(extract_commit_type)
    
    return commits_df

def filter_commits_by_date(commits_df, start_date, end_date):
    """Filter commits by date range"""
    if 'date_only' in commits_df.columns:
        return commits_df[
            (commits_df['date_only'] >= start_date) & 
            (commits_df['date_only'] <= end_date)
        ].copy()
    return commits_df.copy()