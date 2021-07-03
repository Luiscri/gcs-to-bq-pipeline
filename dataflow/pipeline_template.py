import json

import apache_beam as beam
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import SetupOptions

class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments
        parser.add_value_provider_argument(
            '--input_file',
            help='Path of the file to read from')
        parser.add_value_provider_argument(
            '--output_prefix',
            help='Output prefix of the file to write results to.')
        parser.add_argument(
            '--dataflow_bucket',
            default='dataflow-files',
            help='Bucket name for staging and temp files.')
        parser.add_argument(
            '--bq_sink',
            default='mydataset.stats',
            help='BigQuery sink with project:dataset.table OR dataset.table format.')

options = PipelineOptions()
args = options.view_as(CustomOptions)
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'myproject'
google_cloud_options.region = 'europe-west1'
google_cloud_options.job_name = 'process-raw'
google_cloud_options.staging_location = 'gs://{}/staging/'.format(args.dataflow_bucket)
google_cloud_options.temp_location = 'gs://{}/temp/'.format(args.dataflow_bucket)
#options.view_as(StandardOptions).runner = 'DirectRunner'  # use this for debugging
options.view_as(StandardOptions).runner = 'DataFlowRunner'
options.view_as(SetupOptions).save_main_session = True
options.view_as(SetupOptions).setup_file = './setup.py'

output_suffix = '.csv'
output_header = 'Name,Total,HP,Attack,Defence,Sp_attack,Sp_defence,Speed,Average'
output_format = 'STRING,{}'.format('FLOAT64,'*8)[:-1]

def run(source_file, output_prefix):
    with beam.Pipeline(options=options) as p:
        cleaned_data = (
            p
            | "Read from Cloud Storage" >> beam.io.ReadFromText(source_file, skip_header_lines=1)
            | "Split columns" >> beam.Map(lambda x: x.split(','))
            | "Cleanup entries" >> beam.ParDo(ElementCleanup())
            | "Calculate average stats" >> beam.Map(calculate_average)
        )

        (
            cleaned_data
            | "Format CSV" >> beam.Map(format_csv)
            | "Write to Cloud Storage" >> beam.io.WriteToText(
                                            file_path_prefix=output_prefix,
                                            file_name_suffix=output_suffix,
                                            header=output_header)
        )

        table_schema = parse_table_schema_from_json(create_schema())
        sink = args.bq_sink # The dataset needs to already exist
        (
            cleaned_data
            | "Format BigQuery row" >> beam.Map(format_bq_row)
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                                        sink,
                                        schema=table_schema,
                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                        additional_bq_parameters={
                                            'timePartitioning' : {
                                                #'field': 'Date',
                                                'type': 'MONTH'
                                            }
                                        })
        )

class ElementCleanup(beam.DoFn):
    def __init__(self):
        self.transforms = self.map_transforms()

    def map_transforms(self):
        return [
            [self.trim, self.to_lowercase],   # Name
            [self.trim, self.to_float],		  # Total
            [self.trim, self.to_float],		  # HP
            [self.trim, self.to_float], 	  # Attack
            [self.trim, self.to_float],		  # Defence
            [self.trim, self.to_float],		  # Sp_attack
            [self.trim, self.to_float],		  # Sp_defence
            [self.trim, self.to_float]		  # Speed
        ]

    def process(self, row):
        return [self.clean_row(row, self.transforms)]

    def clean_row(self, row, transforms):
        cleaned = []
        for idx, col in enumerate(row):
            for func in transforms[idx]:
                col = func(col)
            cleaned.append(col)
        return cleaned

    def to_lowercase(self, col:str):
        return col.lower()

    def trim(self, col:str):
        return col.strip()

    def to_float(self, col:str):
        return (float(col) if col != None else None)

def calculate_average(row):
    average = round(sum(row[2:]) / len(row[2:]), 2)
    row.append(average)
    return row

def format_csv(row):
	row = [str(col) for col in row]
	return ','.join(row)

def format_bq_row(row):
    fields = output_header.split(',')
    return { field : col for field, col in zip(fields, row) }

def create_schema():
    names = output_header.split(',')
    types = output_format.split(',')
    fields = [ {"name": name, "type": ftype, "mode": "REQUIRED"} for name, ftype in zip(names, types) ]
    return json.JSONEncoder().encode({"fields": fields})

run(args.input_file, args.output_prefix)