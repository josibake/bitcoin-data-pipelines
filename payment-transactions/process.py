#!/usr/bin/env python3

import apache_beam as beam
import json
import argparse

from apache_beam.io import ReadFromText
from apache_beam.io import ReadFromBigQuery
from apache_beam.io import WriteToBigQuery
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

parser = argparse.ArgumentParser("pipeline for processing transactions with <= 2 outputs")
parser.add_argument('-r', '--runner', default='DataFlowRunner', help='run locally with DirectRunner')
parser.add_argument('-s', '--start_date', help='start date for processing', required=True)
parser.add_argument('-e', '--end_date', help='end date for processing (inclusive)', required=True)

args = parser.parse_args()

options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = "bitcoin-data-analysis-320014"
google_cloud_options.job_name = "parse-bitcoin-transactions"
google_cloud_options.region = "us-east1"
google_cloud_options.staging_location = "gs://josibake-testing/staging"
google_cloud_options.temp_location = "gs://josibake-testing/temp"

options.view_as(StandardOptions).runner = args.runner

# see here for bigquery docs https://beam.apache.org/documentation/io/built-in/google-bigquery/
#source_table_spec = bigquery.TableReference(
#    projectId="pascalwhoop", datasetId="phone_sensors", tableId="heartbeat"
#)
sink_table_spec = bigquery.TableReference(
    projectId="bitcoin-data-analysis-320014", datasetId="transaction_analysis", tableId="txs_two_or_less_outputs"
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
        "input_value": "INTEGER", 
        "output_value": "INTEGER", 
        "payment_value": "INTEGER",
        "change_value": "INTEGER",
        "mining_fee": "INTEGER",
        "mining_fee_rate": "FLOAT"
    }
    mapping_list =  [{"mode": "NULLABLE", "name": k, "type": mapping[k]} for k in mapping.keys()]
    return json.JSONEncoder(sort_keys=True).encode({"fields": mapping_list})

table_schema = parse_table_schema_from_json(make_sink_schema())

source = ReadFromBigQuery(query=f"""
        SELECT `hash` as tx_hash
             , block_timestamp
             , block_number
             , input_count
             , output_count
             , inputs
             , outputs
             , input_value
             , output_value
             , fee AS mining_fee
             , fee / virtual_size AS mining_fee_rate
          FROM `bigquery-public-data.crypto_bitcoin.transactions`
         WHERE TRUE
           AND is_coinbase IS FALSE
           AND date(block_timestamp) BETWEEN '{args.start_date}' AND '{args.end_date}'
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
    convert_to_int = ['input_value','output_value','change_value','payment_value', 'mining_fee']
    
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
        # if there is only one input and one output, there cannot be a change address
        # unlikely that these are actual payments but we are keeping them in the dataset
        # for now. they might yield something interesting when studied on their own
        if row['input_count'] == 1 and row['output_count'] == 1:
            row['change_value'] = 0

        # for nonstandard outputs, choose the output which is not non-standard as the
        # payment value.
        # these also are unlikely to be regular payments, excluding for now
        elif row['output_type'] == 'nonstandard':
            for output in row['outputs']:
                if output['type'] != 'nonstandard':
                    row['change_value'] = 0

        # for mixed outputs, we want to pick the output that is the same as the inputs as
        # the change address. this is generally, not always, true
        elif row['output_type'] == 'mixed_outputs' and row['output_count'] > 1:
            for output in row['outputs']:
                if output['type'] == row['input_type']:
                    row['change_value'] = output['value']
        # if we have multiple inputs and two outputs, the change value should be the output which
        # is less than or equal to the minimum of the inputs
        elif row['output_count'] > 1 and row['input_count'] > 1:
            min_input = min(row['inputs'], key=lambda x: x['value'])['value']
            change_value = list(filter(lambda x: x['value'] <= min_input, row['outputs']))
            if len(change_value) == 1:
                # what about when it's more??
                row['change_value'] = change_value[0]['value']
            elif len(change_value) > 1:
                min_input = min(row['inputs'], key=lambda x: x['value'])['value'] - row['mining_fee']
                change_value = list(filter(lambda x: x['value'] <= min_input, row['outputs']))
        # if there are many inputs to one output, there is no change address
        # difficult to say if these are payments or not. its possible this is UTXO consolidation by
        # a user, but it could also be a wallets that are optimizing for shrinking the UTXO set by combining
        # inputs into exactly one output
        elif row['input_count'] > 1 and row['output_count'] == 1:
            row['change_value'] = 0
        elif row['input_count'] == 1:
            for output in row['outputs']:
                if output['addresses'][0] == row['inputs'][0]['addresses'][0]:
                    row['change_value'] = output['value']
        # only calculate a change and payment value if we have something for change,
        # even if it is zero. if we have nothing, both change and payment will be null
        # which means we weren't able to make an educated guess
        if 'change_value' in row:
            row['payment_value'] = row['output_value'] - row['change_value']
        converted_row = {k: int(row[k]) if k in self.convert_to_int else row[k] for k in row.keys()}
        return {k: converted_row[k] for k in converted_row.keys() if k not in self.exclude}


if __name__ == "__main__":
    run()
