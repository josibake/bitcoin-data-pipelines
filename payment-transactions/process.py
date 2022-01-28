import apache_beam as beam
import json

from apache_beam.io import ReadFromText
from apache_beam.io import ReadFromBigQuery
from apache_beam.io import WriteToBigQuery
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

#options = PipelineOptions(experiments=['use_unsupported_python_version'])
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = "bitcoin-data-analysis-320014"
google_cloud_options.job_name = "parse-bitcoin-transactions"
google_cloud_options.region = "us-east1"
google_cloud_options.staging_location = "gs://josibake-testing/staging"
google_cloud_options.temp_location = "gs://josibake-testing/temp"

# options.view_as(StandardOptions).runner = "DirectRunner"  # use this for debugging
options.view_as(StandardOptions).runner = "DataFlowRunner"

# see here for bigquery docs https://beam.apache.org/documentation/io/built-in/google-bigquery/
#source_table_spec = bigquery.TableReference(
#    projectId="pascalwhoop", datasetId="phone_sensors", tableId="heartbeat"
#)
sink_table_spec = bigquery.TableReference(
    projectId="bitcoin-data-analysis-320014", datasetId="transaction_analysis", tableId="tx_output_test"
)

def make_sink_schema():
    mapping = {
        "tx_hash":          "STRING", 
        "block_timestamp": "TIMESTAMP", 
        "block_number": "INTEGER", 
        "input_count": "INTEGER",
        "output_count": "INTEGER", 
        "input_type": "STRING",
        "output_type": "STRING",
        "input_value": "FLOAT", 
        "output_value": "FLOAT", 
        "payment_value": "FLOAT",
        "change_value": "FLOAT",
        "mining_fee": "FLOAT",
    }
    mapping_list =  [{"mode": "NULLABLE", "name": k, "type": mapping[k]} for k in mapping.keys()]
    return json.JSONEncoder(sort_keys=True).encode({"fields": mapping_list})

table_schema = parse_table_schema_from_json(make_sink_schema())

source = ReadFromBigQuery(query="""
        SELECT `hash` as tx_hash
             , block_timestamp
             , block_number
             , input_count
             , output_count
             , inputs
             , outputs
             , input_value
             , output_value
             , input_value - output_value as mining_fee
          FROM `bigquery-public-data.crypto_bitcoin.transactions`
         WHERE TRUE
           AND is_coinbase IS FALSE
           AND date(block_timestamp_month) = '2021-06-01'
           AND output_count < 3
        """, use_standard_sql=True)  # you can also use SQL queries
# source = BigQuerySource(source_table_spec)
target = WriteToBigQuery(sink_table_spec, schema=table_schema)


def run():
    with beam.Pipeline(options=options) as p:
        raw_values = (
            p 
            | "ReadTable" >> beam.io.Read(source) 
            | "cleanup" >> beam.ParDo(ElementCleanup())
            | "writeTable" >> beam.io.Write(target)
            )
        # pipeline
        # parDo for all values in PCollection: process
        # each element: define a target datatype and a set of cleanup functions for each


class ElementCleanup(beam.DoFn):
    exclude = ['inputs','outputs']
    
    def process(self, row):
        #process receives the object and (must) return an iterable (in case of breaking objects up into several)
        return [self.identify_change_address(self.label_inputs_and_outputs(row))]


    def label_inputs_and_outputs(self, row):
        tx_type = {
        'witness_v0_keyhash': 0,
        'witness_v0_scripthash': 0,
        'pubkeyhash': 0,
        'scripthash': 0,
        'nonstandard': 0,
        'multisig': 0,
        'pubkey':0,
        }
        # verify inputs are all the same
        for tx_input in row['inputs']:
            tx_type[tx_input['type']] += 1

        for k,v in tx_type.items():
            if k == 'nonstandard' and v > 0:
                row['input_type'] = k
                break
            elif v == row['input_count']:
                row['input_type'] = k
                break
            else:
                row['input_type'] = 'mixed_inputs'

        tx_type = tx_type.fromkeys(tx_type, 0)
        # check out types
        for tx_output in row['outputs']:
            tx_type[tx_output['type']] += 1
        for k,v in tx_type.items():
            if k == 'nonstandard' and v > 0:
                row['output_type'] = k
                break
            elif v == row['output_count']:
                row['output_type'] = k
                break
            else:
                row['output_type'] = 'mixed_outputs'
        return row

    def identify_change_address(self, row):
        if row['input_count'] == 1 and row['output_count'] == 1:
            row['change_value'] = 0
        elif row['output_type'] == 'nonstandard':
            for output in row['outputs']:
                if output['type'] != 'nonstandard':
                    row['change_value'] = 0
        elif row['output_type'] == 'mixed_outputs' and row['output_count'] > 1:
            for output in row['outputs']:
                if output['type'] == row['input_type']:
                    row['change_value'] = output['value']
        elif row['output_count'] > 1 and row['input_count'] > 1:
            min_input = min(row['inputs'], key=lambda x: x['value'])['value']
            change_value = list(filter(lambda x: x['value'] <= min_input, row['outputs']))
            if len(change_value) == 1:
                # what about when it's more??
                row['change_value'] = change_value[0]['value']
            elif len(change_value) > 1:
                min_input = min(row['inputs'], key=lambda x: x['value'])['value'] - row['mining_fee']
                change_value = list(filter(lambda x: x['value'] <= min_input, row['outputs']))
        elif row['input_count'] > 1 and row['output_count'] == 1:
            row['change_value'] = 0
        elif row['input_count'] == 1:
            for output in row['outputs']:
                if output['addresses'][0] == row['inputs'][0]['addresses'][0]:
                    row['change_value'] = output['value']
        if 'change_value' in row:
            row['payment_value'] = row['output_value'] - row['change_value']
        return {k: row[k] for k in row.keys() if k not in self.exclude}


if __name__ == "__main__":
    run()
