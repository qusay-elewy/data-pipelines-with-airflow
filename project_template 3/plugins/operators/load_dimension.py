from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    """
    Extracts data from stagging tables and loads it into the dimenssion tables
    
        Parameters:
            redshift_conn_id: Redshift conection id
            table: Destination table where data will be loaded
            sql: Select statment that will read data from a data source
            truncate_insert: Defines whether old data should be removed or kept before loadin new data to the dimenssion table. This option is set to True by default
            
        Returns:
            None
    """
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 truncate_insert=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table=table
        self.sql=sql
        self.truncate_insert=truncate_insert

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        #Check if the old data should be deleted before loading the exctracted data 
        if truncate_insert:
            self.log.info(f"Clearing data from {self.table} table")
            redshift_hook.run(f"DELETE FROM {self.table}")

        #Loading the extracted data into the dimenssion table
        self.log.info("Inserting data into {self.table} table")
        redshift_hook.run(f"""INSERT INTO {self.table}
                                        {self.sql}
                        """)
