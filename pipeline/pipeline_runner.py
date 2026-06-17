# pipeline/pipeline_runner.py
from pipeline.extractor   import WeatherExtractor
from pipeline.transformer import WeatherTransformer
from pipeline.loader      import WeatherLoader
from pipeline.utils.logger import get_logger
 
logger = get_logger(__name__)
 
 
class WeatherPipelineRunner:
    """
    Reusable, configurable ETL pipeline class.
    Encapsulates the full extract -> transform -> load workflow.
    Can be called standalone or invoked by the Airflow DAG.
    """
 
    def __init__(self):
        self.extractor   = WeatherExtractor()
        self.transformer = WeatherTransformer()
        self.loader      = WeatherLoader()
 
    def run(self) -> dict:
        """
        Execute the full ETL pipeline.
        Returns: {'extracted': N, 'transformed': N, 'loaded': N}
        """
        logger.info('=== WeatherPipelineRunner START ===')
        summary = {}
 
        raw_df = self.extractor.extract_all()
        summary['extracted'] = len(raw_df)
 
        clean_df = self.transformer.transform(raw_df)
        summary['transformed'] = len(clean_df)
 
        self.loader.load(clean_df)
        summary['loaded'] = len(clean_df)
 
        logger.info(f'=== Pipeline complete. Summary: {summary} ===')
        return summary
 
 
if __name__ == '__main__':
    runner = WeatherPipelineRunner()
    result = runner.run()
    print(result)
