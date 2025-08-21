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

pattern_placements = r'\b([1-9]|1[0-9]|20)(st|nd|rd|th)\b' #1st 2nd etc
pattern_placements2 = r'(?i)\b([1-9]|1[0-9]|20)(st|nd|rd|th)\s*place\b' #1st place

pattern_month_year_or_reversed = r"\b(?:(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{4}|\d{4} (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*)\b"

yt_scrapings = pl.scan_csv('routine_videos.csv')
fabio_efforts = pl.scan_csv('Fabio_Routine_Archive.csv').rename({'Link':'url',
                                                                 'Year':'year'})

categories = ['cabaret',
              'routine',
            'classic',
            'honorable mention',
            'juniors routine',
            'masters',
            'pro am', 'proam',
            'rising star',
            'showcase',
            'team routine',
            'team',
            'young adult',]

wsdc_events = ['4th of july convention', '4th of july swing bash', '5280 swing dance championships', 
          '5280 westival', 'all star swingjam', 'american lindy hop championships', 'americano dance camp', 
          'americas classic', 'anchor festival', 'anti valentines', 'arizona dance classic', 
          'arizona dance classic (cancelled due to covid-19)', 'asia wcs open - 10th anniversary', 
          'asia wcs open xi', 'asia west coast swing open', 'asian wcs open swingvitation', 'atlanta swing classic', 
          'austin rocks', 'austin swing dance championships', 'austin swing dance championships (asdc)', 'australasian wcs & zouk champs', 
          'australian open swing dance championships', 'austrian open', 'austrian wcs spectacle', 'autumn swing challenge', 
          'avignon city swing', 'baltic swing', 'bavarian open', 'bavarian open wcs', 'bavarian open west coast swing championships', 
          'bay swingers', 'berlin swing revolution', 'best of the best', 'best of the best wcs', 'big apple dance festival', 
          'big apple dance festival/world hustle championships', 'boogie & blues', 'boogie by the bay', 'boston dance challenge', 
          'boston dance revolution', 'boston tea party', "brandin' iron dance festival", 'bridgetown swing', 'bridgetown swing boogie', 
          'british columbia dance challenge', 'bto open', 'budafest open wcs championships', 'budafest', 'c.a.s.h. bash', 'c.a.s.h. bash weekend', 
          'calgary dance stampede', 'california state championships', 'canadian swing championships', 'canadian swing championships (csc)', 
          'capital swing convention', 'capital swing dance convention', "capital swing dancers' president's day", 
          "capital swing presidents' day convention", 'carolina groove', 'cash bash', 'champions weekend (ma)', 'charlotte westie fest', 
          'charlotte westiefest', 'chicago classic', 'chicago dance challenge', 'chicagoland', 'chicagoland country & swing dance festival', 
          'chicagoland country and swing dance festival', 'chicagoland dance festival', 'chico dance sensation', 'citadel swing', 
          'citadel swing (cancelled due to covid-19)', 'city of angels', 'city of angels swing event', 'colorado country classic', 
          'come rain come shine', 'countdown swing boston', 'country boogie', 'country dance world championships', 'd-town swing', 
          'd-townswing', 'da dance camp', 'dallas d.a.n.c.e.', 'dallas dance festival', 'dance america seattle', 'dance camp chicago', 
          'dance mardi gras', 'dance n play', 'dancers fund/moover & groover/swingexpo', 'dc swing experience', 'dc swing experience  (dcsx)', 'dcsx',
          'dc swing experience (dcsx)', 'derby city swing', 'desert city swing', 'desert city swing dance championships', 'detonation dance', 
          'dfw pro am jam', 'dowcs', 'dutch open west coast swing', 'dutch open west coast swing 2024', 'easter swing', 
          'eastern/washington dance challenge', 'european swing challenge', 'fall fall in swing', 'finnfest', 'floor play swing vacation', 
          'floorplay new years swing vacation', 'florida dance magic', 'florida westie fest', 'freedom swing dance challenge', 'fowcs',
          'french open wcs', 'french open west coast swing', 'frezno dance classic', 'ft. lauderdale swing & shag beach bash', 
          'german open', 'german open championships', 'german open wcs championships', 'global grand prix - west coast swing reunion', 
          'go west swing fest', 'golden state classic', 'grand prix of swing', 'halloween in harrisburg & swingzing', 'halloween swingthing', 
          'hawaii dance sensation', 'high desert dance classic', 'holy land open', 'honey fest', 'hungarian open', 'hustlemania', 
          'indy dance explosion', 'indy swing classic', 'inland valley dance festival', 'international hustle & salsa congress', 
          'italian open', 'j&j national championships', "j&j o'rama", "jack & jill o'rama", 'kazan el fest', 'kc swing challenge and heartland dance festival', 
          'king swing', 'kiwi fest', 'korea westival', 'korean open swing dance championships', 'korean open wcs championsips', 
          'labor day swing dance festival', 'las vegas swing expo', 'liberty swing dance championships', 'liberty swing', 'london swingvitational', 
          'lone star invitational', 'lonestar invitational', 'los angeles premiere dance classic', 'madjam (mid atlantic dance jam)', 'madjam',
          'madmac', 'meet me in st louis', 'meet me in st louis swing dance championships', 'meet me in st. louis swing dance championships', 
          'michigan classic', 'michigan dance classic', 'michigan swing dance champs', 'mid atlantic dance jam (madjam)', 'mid usa jack & jill', 
          'mid-american dance championships', 'mid-atlantic dance jam', 'midatlantic dance classic', 'midland swing open', 'midnight madness', 
          'midwest westie fest', 'milan modern swing', 'monster mash', 'monterey swing fest', 'monterey swing fest 2024', 'monterey swingfest', 
          'monterey swingfest (cancelled due to covid-19)', 'montreal wcs fest', 'montreal westie fest', 'moscow westie dance fest', 
          'moscow westie fest', 'moscow xmas dance camp', 'motor city jam', 'motown dance championships', 'mountain magic', 'mountain magic dance convention', 
          'municorn swing', 'music city swing dance championships', 'nashville swing & shag dance classic', 'neverland swing', 'neverlandswing', 
          'new england dance festival', 'new england swing jam 1', 'new mexico dance fiesta', 'new orleans dance mardi gras', "new year's dance camp", 
          "new year's dance championships", "new year's dance extravaganza", "new year's dancin' eve", "new year's swing fling", 'new years dance camp', 
          'new zealand open', 'new zealand open swing dance championships', 'nz open', 'nordic wcs championships', 'north atlantic swing dance championships', 
          'northwest regional', 'norway westie fest', 'novice invitational', 'nsw west coast swing dance championships', 'odyssey west coast swing', 
          'old town swing', 'orange blossom dance festival', 'pacific rim dance classic', 'palm springs new year', 
          'palm springs new years swing dance classic', 'palm springs summer dance camp classic', 'palm springs summer dance classic', 
          'palm springs swing dance classic', 'paradise country & swing dance festival', 'paradise country and swing dance festival', 
          'paradise country dance festival', 'paradise dance festival', 'paris swing classic', 'paris westie fest', 'philly swing classic', 
          'phoenix 4th of july', 'phoenix dance festival', 'portland dance festival', 'providance swing in the city', 'reno dance sensation', 
          'riga summer swing', 'rising star - chicago', 'rising star - dallas', 'river city dance festival', 'river city swing', 'rock the barn', 
          'rocky mountain 5 dance', 'rocky mountain five dance (rm5)', 'rolling swing', 'rose city swing', 'russian open wcs championship', 'russian open',
          'russian open wcs championships', 'sacramento all swing', 'saint petersburg wcs nights', 'san diego dance festival', 'san francisco dance sensation', 
          'sao paulo swing dance championships', 'scandinavian open', 'scandinavian open wcs', 'scandinavian open wcs "snow"', 'ru open', 'ruopen',
          'scandinavian open wcs 2022', 'scotland swing classic', 'scottish wcs dance championships', 'sea sun & swing camp', 'sea sun and swing', 
          'sea to sky', 'sea to sky - seattle', 'sea to sky seattle wcs', "seattle's easter swing", 'shakedown swing', 'shooba dooba swing', 
          'show me showdown', 'show-me showdown', 'simply adelaide west coast swing', 'simply adelaide west coast swing 2022', 
          'simply adelaide west coast swing 2023', 'sincity swing', 'slingshot swing', 'soswing', 'soswing 2022', 'south bay cw dance festival', 
          'south bay dance fling', 'spotlight dance challenge', "spotlight new year's celebration", 'spring fling', 'spring swing', 
          "st. patrick's day swing", 'st.petersburg wcs nights', 'summer dance festival', 'summer hummer', 'summer swing classic', 
          'summer swing jam', 'sundance labor day swing dance festival', 'sunny side dance camp', 'sunshine state dance challenge', 
          'sweden westie gala', 'swedish swing summer camp', 'sweetheart swing classic', 'swing & snow', 'swing challenge', 'swing city chicago', 
          'swing dance america', 'swing dance america (sda)', 'swing diego', 'swing escape', 'swing expo', 'swing fiction', 'swing fiction 2024', 'swing fling', 
          'swing generation', 'swingesota', 'swing in capital', "swing it like it's hot", 'swing jam', 'swing niagara dance championships', 'swing open kazan', 
          'swing over', 'swing over orlando', 'swing resolution', 'swing trilogy', 'swing&snow', 'swingapalooza', 'swingcouver', 
          'swingcouver 2020 - the 10th episode', 'swingdiego', 'swingdiego (the superbowl of swing)', 'swingin in the valley', "swingin' into spring", 
          "swingin' new england", "swingin' new england dance festival", 'swinging in the northland', 'swingover', 'swingsation', 'swingsation 2024', 
          'swingtacular', 'swingtacular: the galactic open', 'swingtacular: the galactic open 2022', 'swingtacular: the galactic open 2023', 'swingtimate', 
          'swingtimate 2020', 'swingtime', 'swingtime denver', 'swingtime in the rockies', 'swingtzerland', 'swingvasion', 'swingvester', 'swustlicious', 
          'tampa bay classic', 'texas classic', 'the after party', 'the after party (tap)', 'the after party - tap', 'the after party “tap”',
          'the boston tea party', 'the brazilian open', 'the brazilian open championships', 'the brazilian open championships (cancelled due to coronavirus)', 
          'the challenge', 'the chicago classic', 'the city of light', 'the german open wcs championships', 'the new zealand open', 
          'the texas classic', 'tlv swingfest', 'toronto international swing dance', 'toronto open swing & hustle championships', 
          'toronto open swing and hustle championships', 'swing and snow', 'trilogy swing', 'tulsa fall fling', 'tulsa spring swing', 'twin city swing challenge', 
          'u.k. & european wcs championships', 'ucwdc country dance world championships', 'uk & european wcs championships', 
          'uk wcs dance championships', 'ukrainian open', 'upstate dance challenge', 'uptown swing', 'us national dance championships', 'u s open', 'us open', 
          'the open', 'us open swing dance championships', 'usa grand national dance championships', 'usa grand nationals', 'usa grand nationals dance championship', 
          'usa grand nationals dance championships', 'grand nationals', 'usa jack & jill championships', 'valentine swing', 'valley dancefest', 'vancouver vibrations', 
          'vermont swing dance championships', 'virginia state open', 'waltz across texas', 'warsaw halloween swing', 'warsaw swing', 'wcs festival', 
          'wcs helsinki', 'wcs@idb', 'west coast dance challenge', 'west in lyon', 'westcoast swing dance championships', 'westeroz swing', 'westie gala', 
          'westie pink city', 'westie spring thing', "westie's angels", 'westies on the water', 'westy nantes', 'wild wild westie', 'windy city', 'wcs international rally',
          'winter coast swing', 'winter white', 'winter white wcs', 'winter white west coast swing', 'wisconsin dance challenge/midwest area swing dance challenge', 
          'world hustle dance championships', 'world swing dance championships', 'world swing masters', 'worlds ucwdc', 'xanadu - midwinter dance celebration',
          'euro dance festival', 'eurodance festival', 'ggp', 'global grand prix', 'choreo camp', 'capital swing classic', 'capital swing', 'awcso', 'pfw', 'csc', 
          'uk champs', 'austin swing dance championships', 'csdc', 'kiwifest', 'dutch open', 'wotp', 'rcs', 
          'raw con',
          ]


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
                .with_row_index(offset=1)
                # .with_columns(extracted_date = pl.concat_list(pl.col('Title').str.extract_all(pattern_yyyy_mm_dd),
                #                                             pl.col('Title').str.extract_all(pattern_yyyy_dd_mm),
                #                                             pl.col('Title').str.extract_all(pattern_dd_mm_yyyy),
                #                                             pl.col('Title').str.extract_all(pattern_mm_dd_yyyy),

                #                                             pl.col('Title').str.extract_all(pattern_yy_mm_dd),
                #                                             pl.col('Title').str.extract_all(pattern_yy_dd_mm),
                #                                             pl.col('Title').str.extract_all(pattern_dd_mm_yy),
                #                                             pl.col('Title').str.extract_all(pattern_mm_dd_yy),

                #                                             pl.col('Title').str.extract_all(pattern_dd_MMM_yyyy),
                #                                             pl.col('Title').str.extract_all(pattern_MMM_dd_yyyy),
                #                                             pl.col('Title').str.extract_all(pattern_yyyy_MMM_dd),
                #                                             pl.col('Title').str.extract_all(pattern_yyyy_dd_MMM),

                #                                             pl.col('Title').str.extract_all(pattern_dd_MMM_yy),
                #                                             pl.col('Title').str.extract_all(pattern_yy_MMM_dd),
                #                                             # pl.col('Title').str.extract_all(pattern_MMM_dd_yy), #matches on Jul 2024 as a date :(
                #                                             # pl.col('Title').str.extract_all(pattern_yy_dd_MMM),  #matches on 2024 Jul as a date :(

                #                                             # pl.col('Title').str.extract_all(pattern_mm_yy),
                #                                             # pl.col('Title').str.extract_all(pattern_dd_mm),
                #                                             # pl.col('Title').str.extract_all(pattern_yy_mm),
                #                                             # pl.col('Title').str.extract_all(pattern_mm_dd),
                #                                             )
                #                                 .list.unique()
                #                                 .list.drop_nulls(),
                #                 extracted_year = pl.concat_list(pl.col(pl.String).str.extract_all(pattern_yyyy),
                #                                                 pl.col(pl.String).str.extract_all(pattern_apos_yy),
                #                                                 )
                                                
                                
                #                 )
                )


df = load_routine_data()

video_txt_search = st.text_input("Routine title search:").lower().split(',')


routine_vids = (df
                .filter(pl.concat_str(pl.all(), separator=' ', ignore_nulls=True).str.contains_any(video_txt_search, ascii_case_insensitive=True),
                       )
                .with_columns( 
                            #   pl.when((pl.col('year').is_null()
                            #            | pl.col('year').eq('')
                            #            | pl.col('year').eq(' ')
                            #            ))
                            #     .then(pl.lit('unk')),
                                
                              search_terms = pl.concat_str(pl.all(), separator=' ', ignore_nulls=True)
                                                .str.to_lowercase()
                                                .str.extract_all('|'.join(video_txt_search))
                                                .list.unique()
                                                .list.drop_nulls()
                                                .list.sort(),
                              terms_count = pl.concat_str(pl.all(), separator=' ', ignore_nulls=True)
                                                .str.to_lowercase()
                                                .str.extract_all('|'.join(video_txt_search))
                                                .list.unique()
                                                .list.drop_nulls()
                                                .list.len(),
                             category = pl.concat_str(pl.all(), separator=' ', ignore_nulls=True)
                                                .str.to_lowercase()
                                                .str.extract_all('|'.join(categories))
                                                .list.unique()
                                                .list.drop_nulls(),
                             placements = pl.concat_str(pl.all(), separator=' ', ignore_nulls=True)
                                                .str.to_lowercase()
                                                .str.extract_all(pattern_placements2)
                                                .list.unique()
                                                .list.drop_nulls(),
                            events = pl.concat_str(pl.all(), separator=' ', ignore_nulls=True)
                                                .str.to_lowercase()
                                                .str.extract_all('|'.join(wsdc_events))
                                                .list.unique()
                                                .list.drop_nulls(),
                              )
                # .with_columns(pl.col('Category'))
                .sort(pl.col('terms_count'), descending=True)
                
                )

st.dataframe(routine_vids, 
             column_config={"url": st.column_config.LinkColumn(),
                            'Account': st.column_config.LinkColumn()
                            }
             )


# df.pipe(just_a_peek)
# yt_scrapings.pipe(just_a_peek)
# fabio_efforts.pipe(just_a_peek)