import streamlit as st
import polars as pl
import psutil
from supabase import create_client
import os


pl.Config.set_tbl_rows(100).set_fmt_str_lengths(100)
pl.enable_string_cache() #for Categoricals


pattern_yyyy_mm_dd = r'\b(?:19|20)\d{2}[-/.](?:0[1-9]|1[0-2])[-/.](?:0[1-9]|[12]\d|3[01])\b'
pattern_yyyy_dd_mm = r'\b(?:19|20)\d{2}[-/.](?:0[1-9]|[12]\d|3[01])[-/.](?:0[1-9]|1[0-2])\b'
pattern_dd_mm_yyyy = r'\b(?:0[1-9]|[12]\d|3[01])[-/.](?:0[1-9]|1[0-2])[-/.](?:19|20)\d{2}\b'
pattern_mm_dd_yyyy = r'\b(?:0[1-9]|1[0-2])[-/.](?:0[1-9]|[12]\d|3[01])[-/.](?:19|20)\d{2}\b'

pattern_yy_mm_dd = r'\b\d{2}[-/.](?:0[1-9]|1[0-2])[-/.](?:0[1-9]|[12]\d|3[01])\b'
pattern_yy_dd_mm = r'\b\d{2}[-/.](?:0[1-9]|[12]\d|3[01])[-/.](?:0[1-9]|1[0-2])\b'
pattern_dd_mm_yy = r'\b(?:0[1-9]|[12]\d|3[01])[-/.](?:0[1-9]|1[0-2])[-/.]\d{2}\b'
pattern_mm_dd_yy = r'\b(?:0[1-9]|1[0-2])[-/.](?:0[1-9]|[12]\d|3[01])[-/.]\d{2}\b'

pattern_dd_MMM_yyyy = r'\b(?:0[1-9]|[12]\d|3[01])[-/. ]?(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[-/. ]?(?:19|20)\d{2}\b'
pattern_MMM_dd_yyyy = r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[-/. ]?(?:0[1-9]|[12]\d|3[01])[-/. ]?(?:19|20)\d{2}\b'
pattern_yyyy_MMM_dd = r'\b(?:19|20)\d{2}[-/. ]?(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[-/. ]?(?:0[1-9]|[12]\d|3[01])\b'
pattern_yyyy_dd_MMM = r'\b(?:19|20)\d{2}[-/. ]?(?:0[1-9]|[12]\d|3[01])[-/. ]?(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b'

pattern_dd_MMM_yy = r'\b(?:0[1-9]|[12]\d|3[01])[-/. ]?(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[-/. ]?\d{2}\b'
pattern_MMM_dd_yy = r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[-/. ]?(?:0[1-9]|[12]\d|3[01])[-/. ]?\d{2}\b'
pattern_yy_MMM_dd = r'\b\d{2}[-/. ]?(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[-/. ]?(?:0[1-9]|[12]\d|3[01])\b'
pattern_yy_dd_MMM = r'\b\d{2}[-/. ]?(?:0[1-9]|[12]\d|3[01])[-/. ]?(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b'

pattern_mm_yy = r'\b(?:0[1-9]|1[0-2])[-/. ]\d{2}\b'
pattern_dd_mm = r'\b(?:0[1-9]|[12]\d|3[01])[-/. ](?:0[1-9]|1[0-2])\b'
pattern_yy_mm = r'\b\d{2}[-/. ](?:0[1-9]|1[0-2])\b'
pattern_mm_dd = r'\b(?:0[1-9]|1[0-2])[-/. ](?:0[1-9]|[12]\d|3[01])\b'

pattern_yyyy = r'\b(19\d{2}|20\d{2})\b' # 1999 or 2025
pattern_apos_yy = r"'\d{2}\b" # '98 or '25

pattern_month_year_or_reversed = r"\b(?:(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{4}|\d{4} (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*)\b"

yt_scrapings = pl.scan_csv('routine_videos.csv')
fabio_efforts = pl.scan_csv('Fabio_Routine_Archive.csv').rename({'Link':'url'})

def just_a_peek(df_):
        '''just peeks at the df where it is'''
        st.write(df_.schema)
        return df_
    
@st.cache_resource #makes it so streamlit doesn't have to reload for every sesson.
def load_routine_data():
        return (pl.concat([yt_scrapings, 
                           fabio_efforts
                           ], 
                          how='diagonal_relaxed')
                # .with_row_index(offset=1)
                # .with_columns(
                #             #   extracted_date = pl.concat_list(pl.col('Title').str.extract_all(pattern_yyyy_mm_dd),
                #             #                                 pl.col('Title').str.extract_all(pattern_yyyy_dd_mm),
                #             #                                 pl.col('Title').str.extract_all(pattern_dd_mm_yyyy),
                #             #                                 pl.col('Title').str.extract_all(pattern_mm_dd_yyyy),

                #             #                                 pl.col('Title').str.extract_all(pattern_yy_mm_dd),
                #             #                                 pl.col('Title').str.extract_all(pattern_yy_dd_mm),
                #             #                                 pl.col('Title').str.extract_all(pattern_dd_mm_yy),
                #             #                                 pl.col('Title').str.extract_all(pattern_mm_dd_yy),

                #             #                                 pl.col('Title').str.extract_all(pattern_dd_MMM_yyyy),
                #             #                                 pl.col('Title').str.extract_all(pattern_MMM_dd_yyyy),
                #             #                                 pl.col('Title').str.extract_all(pattern_yyyy_MMM_dd),
                #             #                                 pl.col('Title').str.extract_all(pattern_yyyy_dd_MMM),

                #             #                                 pl.col('Title').str.extract_all(pattern_dd_MMM_yy),
                #             #                                 pl.col('Title').str.extract_all(pattern_yy_MMM_dd),
                #             #                                 # pl.col('Title').str.extract_all(pattern_MMM_dd_yy), #matches on Jul 2024 as a date :(
                #             #                                 # pl.col('Title').str.extract_all(pattern_yy_dd_MMM),  #matches on 2024 Jul as a date :(

                #             #                                 # pl.col('Title').str.extract_all(pattern_mm_yy),
                #             #                                 # pl.col('Title').str.extract_all(pattern_dd_mm),
                #             #                                 # pl.col('Title').str.extract_all(pattern_yy_mm),
                #             #                                 # pl.col('Title').str.extract_all(pattern_mm_dd),
                #             #                                 )
                #             #                     .list.unique()
                #             #                     .list.drop_nulls(),
                #                 extracted_year = pl.concat_list(pl.col(pl.String).str.extract_all(pattern_yyyy),
                #                                                 pl.col(pl.String).str.extract_all(pattern_apos_yy),
                #                                                 )
                                                
                                
                #                 )
                )


df = load_routine_data()

video_txt_search = st.text_input("Routine title search:").lower().split(',')


routine_vids = (df
                .filter(pl.any(pl.String).str.contains_any(video_txt_search, ascii_case_insensitive=True),
                      )
                .with_columns(search_terms = pl.col('Title')
                                                .str.to_lowercase()
                                                .str.extract_all('|'.join(video_txt_search))
                                                .list.unique()
                                                .list.drop_nulls()
                                                .list.sort(),
                              terms_count = pl.col('Title')
                                                .str.to_lowercase()
                                                .str.extract_all('|'.join(video_txt_search))
                                                .list.unique()
                                                .list.drop_nulls()
                                                .list.len(),
                              )
                .sort(pl.col('terms_count'), descending=True)
                
                )

st.dataframe(routine_vids, 
             column_config={"url": st.column_config.LinkColumn()})


# df.pipe(just_a_peek)
# yt_scrapings.pipe(just_a_peek)
# fabio_efforts.pipe(just_a_peek)