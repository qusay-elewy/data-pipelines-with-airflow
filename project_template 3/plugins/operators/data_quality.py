from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Performs data quality checks on processed data. This operator takes in a sql query and a value to see if they match.
    
        Parameters:
            redshift_conn_id: Redshift conection id
            sql: sql query presenting a test case to be evaluated
            expected_result: the result expected from running the passed test case. The output of the sql query will be compard to this value to check whether the check passes or not.
            
        Returns:
            None
    """
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 db_checks="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.db_checks=db_checks

    def execute(self, context):
        self.log.info('Performing data quality checks')
        reshift_hook = PostgressHook(self.redshift_conn_id)
        
        for index in range(len(self.db_checks)):
            result = 0
            expected_result = 0
            
            for key in self.db_checks[index]:
                if key == 'check_sql':
                    result = redshift_hook.get_records(self.db_checks[index][key])
                if key == 'expected_result':
                    expected_result = self.db_checks[index][key]
                if(result != expected_result):
                    raise ValueError(f"Data quality check on failed on {self.table} table")

                    
                
                #print(self.db_checks[index][key])

        
        #records = redshift_hook.get_records(self.sql)
        #num_records=records[0][0]
        
        #Checking if the returned value matches the expected value
        #if num_records != self.expected_result:
        #    raise ValueError(f"Data quality check on failed. {self.table} has {num_records} unmaching records")
        self.log.info(f"Data quality checks on {self.table} passed with {num_records} records")
