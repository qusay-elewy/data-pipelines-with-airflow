from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    """
    Extracts data from stagging tables and loads it into the fact table
    
        Parameters:
            redshift_conn_id: Redshift conection id
            table: Destination table where data will be loaded
            sql: Select statment that will read data from a data source
            
        Returns:
            None
    """
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table=table
        self.sql=sql

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from fact table")
        redshift_hook.run(f"DELETE FROM {self.table}")

        self.log.info("Inserting data into fact table")
        formatted_sql=f"""INSERT INTO {self.table}
                                        {self.sql}
                        """
        
        redshift_hook.run(formatted_sql)
