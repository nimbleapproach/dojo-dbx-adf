def fillnas(df: DataFrame) ->DataFrame: 
    
    for col in df.columns:
        if  dict(df.dtypes)[col] =='string':
            df = (df.fillna(value= 'NoValue',subset=[col])
                .replace('', 'NoValue', col))
            
        elif dict(df.dtypes)[col] =='timestamp':
            df = (df.fillna(value= "1900-01-01", subset=[col]))
        else:
            df = (df.fillna(value= -float('Inf'), subset=[col]))
    
    return df