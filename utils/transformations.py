from pyspark.sql.functions import *

class Utilities:
    def dropCols(self,df,columns):
        df=df.drop(*columns)
        return df