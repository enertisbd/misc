#########################################################################################
##
##  This module contains helper functions.
##  The funtions in this module must not inherite any of the other local modules.
##
## Author: Lucas Viani
## Date  : 22.10.2018
##

import os, re, logging

from utils import global_defs  as glb
if glb.USE_MODIN: import modin.pandas as pd
else:             import pandas       as pd

logger = logging.getLogger() 

#########################################################################################
#
# Properties.............................................................................
#
rt_type = {'none':None, 'path': 0, 'df':1}

#########################################################################################
#
# Functions..............................................................................
#


def df_or_path (item, col_as_object=True):
    '''
    Check if 'item' is a path or a dataframe.
    If item is a path, it will open the CSV file located at the path.
    
    Parameters
    ----------
    item : string or pd.DataFrame
        The path to a CSV file or a dataframe.
        
    Return
    ------
        Returns the type provided and the pandas DataFrame if avaialable.
        It will return the input type and the DataFrame.
        If the input is not a string neither a DataFrame, or it it was NOT successful loading the CSV file, it returns (None, None).
    '''
    if isinstance(item['df'], str): 
        # Open the CSV file.
        try:
            logger.debug('Loading CSV file: ' + item['df'])
            # Is the file empty?
            if os.path.getsize(item['df']) > 0:
                col_types = object if col_as_object else None
                df        = pd.read_csv(item['df'], dtype=col_types, sep=';')
                return rt_type['path'], df
            else:
                logger.warning('File is empty! Skipping it.')
                return rt_type['none'], rt_type['none']
                
        except Exception as e:            
            logger.warning(e)
            return rt_type['none'], rt_type['none']
        
    elif isinstance(item['df'], pd.DataFrame):         
          return rt_type['df']  , item['df'].copy(deep=False) # Perform a shallow copy to keep the the reference to the original content
    else: return rt_type['none'], rt_type['none']


def export_or_append (item_type, item, df_info, prefix, output_folder, lst_out, io_mode, na_rep=None):
    ''' Auxiliary function intend to be used in the typical function archtecture when 
        a file path or a dataframe is passed to be handled.

        Parameters
        ----------
        item_type : rt_type
            The type of item provided (Path, DataFrame, etc). It is described in the dictionary rt_type. 
        item : str or DataFrame
            The item of the type provided in 'item_type'.
        df_info : Pandas DataFrame or dict
            The DataFrame/s modified by the calling function. It will basically replace the DataFrame/s store in 'item' 
            if the 'item_type' is DataFrame.
        prefix : str
            String prefix to be added to the file name. It can be used to avoid a possible overwrite of the input file.
        output_folder : string
            The output folder to dump the CSV files. If None, the dataframes are returned and no file is exported.
        lst_out : list
            The list to be filled with the DataFrame provided 'df_info' in case of DataFrame mode.
        io_mode : glb.IO_TYPE
            The IO type to be considered. Examples are glb.IO_TYPE['FILE'], glb.IO_TYPE['DF'] to export the output tables as files or dataframes.
        na_rep : str, default ''
            Missing data representation argument in to_csv functions of Pandas.
    '''
    # Setting the default arguments =====================================================
    if na_rep is None: na_rep = ''

    if isinstance(df_info, pd.DataFrame):
        df_info = {'df': df_info, 'label': '', 'output_fnm': ''}
    else:
        df_info['output_fnm'] = df_info['output_fnm'] if 'output_fnm' in df_info.keys() else ''

    # Handling the function =============================================================
    # Check if to export the DataFrame to a file or append in the 'list_out'.............
    exp_file = False
    if io_mode is None:
        if item_type == rt_type['path']: 
            exp_file = True
    elif io_mode == glb.IO_TYPE['FILE']:
        assert item_type == rt_type['path'], 'File export requested, but no path provied!'
        exp_file = True        
     
     # Performing the action.............................................................
    if exp_file:
        fname = os.path.basename(item['fnm']) + '_' if df_info['label'] else ''
        fpath = output_folder if output_folder is not None else os.path.dirname (item['fnm'])
        logger.info('Exporting file: ' + fpath)
        logger.warning('All files are exported under the same filename. Pending to implement a way to label the outputs differently')
        df_info['df'].to_csv(fpath+'/'+prefix+fname+df_info['label'], sep=';', index=False, na_rep=na_rep )
    else:
        item.update({'df':df_info['df'], 'label': df_info['label'], 'output_fnm': df_info['output_fnm']})        
        lst_out.append(item)


def reduce_remove_merge_dfs (lst_df, func_args):
    '''Merges the DataFrame in a list and clears the object to save memory.'''
    if lst_df:
        if len(lst_df) > 1:
            for df2 in lst_df[1:]:
                lst_df[0] = pd.merge(lst_df[0],df2, **func_args)
                del df2
        return lst_df[0]
    return None
    

def to_numeric_col(col, comma_as_thousand=True, fillna=None):
    '''Converts the data type of a column to numeric.'''            
    # Replacing the decimal separator
    col_str = col.astype(str)

    # Check if commas and dots are found in the numbers =================================    
    has_comma = False if col_str.str.contains(',').sum() == 0 else True
    has_dot   = False if col_str.str.contains('.').sum() == 0 else True
    
    # If commas and dots are found, we adjust the column to endup with
    # dots as decimal separators 
    if has_comma and has_dot:
        if comma_as_thousand: 
            col_str = col_str.str.replace(',','')
        else:                 
            col_str = col_str.str.replace('.','')
            col_str = col_str.str.replace(',','.')
    elif has_comma:
        col_str = col_str.str.replace(',','.')

    # Converting to numeric =============================================================
    col = pd.to_numeric(col_str, errors='coerce').astype(float)
    # Handling the NA if requested
    if fillna is not None:
        col = col.fillna(fillna)
    return col


def get_date(dt_str, tz=0, fmt=None, zero_hour=None):
    '''
    Convert a date from a string shape to date using the standard definition of the project (UTC).
    dt_str : str
        The date in the string form.
    tz : int
        The timezone offset in UTC.
    fmt : str
        The date/time format represented by dt_str.
    zero_hour : bool, default False
        Whether to reset the hour to zero. IT is useful when timezones offset the hour.
    '''
    # Setting the date-time..............................................................    
    if format != None: dt = pd.to_datetime(dt_str)            
    else:              dt = pd.to_datetime(dt_str, format=fmt)
    
    # Ensure that no shift in the hours after tz_localize
    dt = (dt + pd.Timedelta(hours=-tz)).tz_localize('UTC').tz_convert(tz*3600)

    # Reset the hour.....................................................................
    if zero_hour:        
        dt = dt.replace(hour=0)
    return dt


def get_sec_of_freq (freq):
    '''Returns the total number of seconds corresponding to the frequency alias provided.
 
        Parameters
        ----------
        freq : str
            The frequency alias of interest.
 
        Return
        ------
            Returns the total number of seconds.
    '''
    r      = re.compile("([0-9]+)([a-zA-Z]+)")
    re_frq = r.match(freq)
 
    # Handle 'M' (month) freq -- pd.to_timedelta() does not support 'M' freq and has a bug adding some hours to the month. Timedelta
    # functions are used for fixed time periods (H, T, D, W) not variable ones (1 month could have 28 to 31 days)
    if freq[-1] == 'M':
        if re_frq is not None: secs = int(re_frq.group(1))*31.0*24.0*3600.0
        else:                  secs = 31.0*24.0*3600.0
    else:
        if re_frq is not None: secs = pd.to_timedelta(int(re_frq.group(1)), unit=re_frq.group(2)).total_seconds()
        else:                  secs = pd.to_timedelta(                   1, unit=freq           ).total_seconds()
    
    return secs


def infer_freq(df, date_col=None, date_fmt=None, n_rows=10):
    ''' Infers the frequency from a Pandas DataFrame using the first 10 rows. If it fails,
        tries again with the next 10 rows, and so on.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame which frequency is to be inferred.
        date_col : None, str
            Name of the column that contains the timestamps. If None, the index will be used 
        date_fmt : str
            Datetime format.
        nrows : int
            Number of rows to be taken into account to try to infer the frequency 

        Return
        ------
            Returns the total number of seconds.
    '''
    dates = df.index if date_col is None else df[date_col]
    if date_fmt:
        dates = pd.to_datetime(dates, format=date_fmt).astype(str)
    freq = pd.infer_freq(dates[0:n_rows])
    if freq is None:
        logger.debug('Frequency could not be inferred from first <{}> values. Trying next date ranges...'.format(n_rows))
        for try_n in range(0, 21):
            logger.debug('   .Try #{}'.format(try_n))
            try:
                freq = pd.infer_freq(dates[try_n*n_rows+1:try_n*n_rows+1+n_rows])
                if freq:
                    logger.debug('   .Success!')
                    break
            except Exception as e:            
                logger.warning(e)

    return freq


def remove_colums_by_type (df, ctypes_rmv):
    '''
        Keep all columns with type not in the list provided.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to be handled.
        ctypes_rmv : list or set
            The standard column types to be remove from 'df'.

        Return
        ------
        Returns the Dataframe filtered.
    '''
    cType_service = glb.id_col_std(df.columns)
    valid_cols    = [c for c in df.columns if cType_service[c] not in ctypes_rmv]    
    return df[ valid_cols ]
