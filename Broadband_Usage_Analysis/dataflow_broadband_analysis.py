import argparse
import time
import logging
import typing
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.runners import DataflowRunner, DirectRunner

# ### functions and classes

class UlRecord(typing.NamedTuple):
  SERVICE_ID : str
  USAGE_DATE : str
  PEAK_UL_KBPS : int
  OFFPEAK_UL_KBPS : int
  MB_USAGE_00 : int
  MB_USAGE_01 : int
  MB_USAGE_02 : int
  MB_USAGE_03 : int
  MB_USAGE_04 : int
  MB_USAGE_05 : int
  MB_USAGE_06 : int
  MB_USAGE_07 : int
  MB_USAGE_08 : int
  MB_USAGE_09 : int
  MB_USAGE_10 : int
  MB_USAGE_11 : int
  MB_USAGE_12 : int
  MB_USAGE_13 : int
  MB_USAGE_14 : int
  MB_USAGE_15 : int
  MB_USAGE_16 : int
  MB_USAGE_17 : int
  MB_USAGE_18 : int
  MB_USAGE_19 : int
  MB_USAGE_20 : int
  MB_USAGE_21 : int
  MB_USAGE_22 : int
  MB_USAGE_23 : int

class DlRecord(typing.NamedTuple):
  SERVICE_ID : str
  USAGE_DATE : str
  PEAK_DL_KBPS : int
  OFFPEAK_DL_KBPS : int
  MB_USAGE_00 : int
  MB_USAGE_01 : int
  MB_USAGE_02 : int
  MB_USAGE_03 : int
  MB_USAGE_04 : int
  MB_USAGE_05 : int
  MB_USAGE_06 : int
  MB_USAGE_07 : int
  MB_USAGE_08 : int
  MB_USAGE_09 : int
  MB_USAGE_10 : int
  MB_USAGE_11 : int
  MB_USAGE_12 : int
  MB_USAGE_13 : int
  MB_USAGE_14 : int
  MB_USAGE_15 : int
  MB_USAGE_16 : int
  MB_USAGE_17 : int
  MB_USAGE_18 : int
  MB_USAGE_19 : int
  MB_USAGE_20 : int
  MB_USAGE_21 : int
  MB_USAGE_22 : int
  MB_USAGE_23 : int

def read_ul_csv(File):
  row=File.split(",")
  lst=[]
  lst.append(row[0])
  lst.append(row[1])
  for i in row[7:]:
    lst.append(float(i))
  key=["SERVICE_ID","USAGE_DATE","PEAK_UL_KBPS","OFFPEAK_UL_KBPS","MB_USAGE_00","MB_USAGE_01","MB_USAGE_02","MB_USAGE_03","MB_USAGE_04","MB_USAGE_05","MB_USAGE_06","MB_USAGE_07","MB_USAGE_08","MB_USAGE_09","MB_USAGE_10",
       "MB_USAGE_11","MB_USAGE_12","MB_USAGE_13","MB_USAGE_14","MB_USAGE_15","MB_USAGE_16",
       "MB_USAGE_17","MB_USAGE_18","MB_USAGE_19","MB_USAGE_20","MB_USAGE_21","MB_USAGE_22","MB_USAGE_23"]
  dict1=dict(zip(key,lst))
  return UlRecord(**dict1)

def read_dl_csv(File):
  row=File.split(",")
  lst=[]
  lst.append(row[0])
  lst.append(row[1])
  lst.append(float(row[5]))
  lst.append(float(row[6]))
  for i in row[9:]:
    lst.append(float(i))
  key=["SERVICE_ID","USAGE_DATE","PEAK_DL_KBPS","OFFPEAK_DL_KBPS","MB_USAGE_00","MB_USAGE_01","MB_USAGE_02","MB_USAGE_03","MB_USAGE_04","MB_USAGE_05","MB_USAGE_06","MB_USAGE_07","MB_USAGE_08","MB_USAGE_09","MB_USAGE_10",
       "MB_USAGE_11","MB_USAGE_12","MB_USAGE_13","MB_USAGE_14","MB_USAGE_15","MB_USAGE_16",
       "MB_USAGE_17","MB_USAGE_18","MB_USAGE_19","MB_USAGE_20","MB_USAGE_21","MB_USAGE_22","MB_USAGE_23"]
  dict1=dict(zip(key,lst))
  return DlRecord(**dict1)

class FileSelect(beam.DoFn):
  def process(self, File):
    if input_path.split("_")[2]=="UL":
      return [beam.pvalue.TaggedOutput('UL',File)]
    else:
      return [beam.pvalue.TaggedOutput('DL',File)]


# ### main

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load CSV files into GCS Bucket')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--staging_location', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--temp_location', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
    parser.add_argument('--input_path', required=True, help='Path to csv file')
    parser.add_argument('--output_path', required=True, help='Path to output')

    opts = parser.parse_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions(save_main_session=True)
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.staging_location
    options.view_as(GoogleCloudOptions).temp_location = opts.temp_location
    options.view_as(GoogleCloudOptions).job_name = '{0}'.format('batch-broadband-usage-daily')
    options.view_as(StandardOptions).runner = opts.runner

    input_path = opts.input_path
    output_path = opts.output_path



    # Create the pipeline
    p = beam.Pipeline(options=options)

    FileSelect = (
            p
            | "Read File" >> beam.io.ReadFromText(input_path, skip_header_lines=True)
            | "Ul or DL"  >>beam.ParDo(FileSelect()).with_outputs('UL', 'DL')

    )

    (
            FileSelect.UL
            | "Filter UL File" >> beam.Map(read_ul_csv).with_output_types(UlRecord)
            | "Transform UL File" >>beam.GroupBy("USAGE_DATE")
                                        .aggregate_field('SERVICE_ID', CountCombineFn(), 'TOTAL_CUSTOMERS')
                                        .aggregate_field('PEAK_UL_KBPS', MeanCombineFn(), 'PEAK_UL_AVG_SPEED')
                                        .aggregate_field('OFFPEAK_UL_KBPS', MeanCombineFn(), 'OFFPEAK_UL_AVG_SPEED')
                                        .aggregate_field('PEAK_UL_KBPS', min, 'PEAK_UL_MIN_SPEED')
                                        .aggregate_field('PEAK_UL_KBPS', max, 'PEAK_UL_MAX_SPEED')
                                        .aggregate_field('OFFPEAK_UL_KBPS', min, 'OFFPEAK_UL_MIN_SPEED')
                                        .aggregate_field('OFFPEAK_UL_KBPS', max, 'OFFPEAK_UL_MAX_SPEED')
                                        .aggregate_field('MB_USAGE_00', sum, 'TOTAL_VOLUME_00')
                                        .aggregate_field('MB_USAGE_01', sum, 'TOTAL_VOLUME_01')
                                        .aggregate_field('MB_USAGE_02', sum, 'TOTAL_VOLUME_02')
                                        .aggregate_field('MB_USAGE_03', sum, 'TOTAL_VOLUME_03')
                                        .aggregate_field('MB_USAGE_04', sum, 'TOTAL_VOLUME_04')
                                        .aggregate_field('MB_USAGE_05', sum, 'TOTAL_VOLUME_05')
                                        .aggregate_field('MB_USAGE_06', sum, 'TOTAL_VOLUME_06')
                                        .aggregate_field('MB_USAGE_07', sum, 'TOTAL_VOLUME_07')
                                        .aggregate_field('MB_USAGE_08', sum, 'TOTAL_VOLUME_08')
                                        .aggregate_field('MB_USAGE_09', sum, 'TOTAL_VOLUME_09')
                                        .aggregate_field('MB_USAGE_10', sum, 'TOTAL_VOLUME_10')
                                        .aggregate_field('MB_USAGE_11', sum, 'TOTAL_VOLUME_11')
                                        .aggregate_field('MB_USAGE_12', sum, 'TOTAL_VOLUME_12')
                                        .aggregate_field('MB_USAGE_13', sum, 'TOTAL_VOLUME_13')
                                        .aggregate_field('MB_USAGE_14', sum, 'TOTAL_VOLUME_14')
                                        .aggregate_field('MB_USAGE_15', sum, 'TOTAL_VOLUME_15')
                                        .aggregate_field('MB_USAGE_16', sum, 'TOTAL_VOLUME_16')
                                        .aggregate_field('MB_USAGE_17', sum, 'TOTAL_VOLUME_17')
                                        .aggregate_field('MB_USAGE_18', sum, 'TOTAL_VOLUME_18')
                                        .aggregate_field('MB_USAGE_19', sum, 'TOTAL_VOLUME_19')
                                        .aggregate_field('MB_USAGE_20', sum, 'TOTAL_VOLUME_20')
                                        .aggregate_field('MB_USAGE_21', sum, 'TOTAL_VOLUME_21')
                                        .aggregate_field('MB_USAGE_22', sum, 'TOTAL_VOLUME_22')
                                        .aggregate_field('MB_USAGE_23', sum, 'TOTAL_VOLUME_23')
            | "Create UL Dictionary" >> beam.Map(lambda x: x._asdict())
            | "Write To Ul File"     >> beam.io.WriteToText(output_path)

    )

    (
            FileSelect.DL
            | "Filter DL File" >> beam.Map(read_dl_csv).with_output_types(DlRecord)
            | "Tansform DL File" >> beam.GroupBy("USAGE_DATE")
                                        .aggregate_field('SERVICE_ID', CountCombineFn(), 'TOTAL_CUSTOMERS')
                                        .aggregate_field('PEAK_DL_KBPS', MeanCombineFn(), 'PEAK_DL_AVG_SPEED')
                                        .aggregate_field('OFFPEAK_DL_KBPS', MeanCombineFn(), 'OFFPEAK_DL_AVG_SPEED')
                                        .aggregate_field('PEAK_DL_KBPS', min, 'PEAK_DL_MIN_SPEED')
                                        .aggregate_field('PEAK_DL_KBPS', max, 'PEAK_DL_MAX_SPEED')
                                        .aggregate_field('OFFPEAK_DL_KBPS', min, 'OFFPEAK_DL_MIN_SPEED')
                                        .aggregate_field('OFFPEAK_DL_KBPS', max, 'OFFPEAK_DL_MAX_SPEED')
                                        .aggregate_field('MB_USAGE_00', sum, 'TOTAL_VOLUME_00')
                                        .aggregate_field('MB_USAGE_01', sum, 'TOTAL_VOLUME_01')
                                        .aggregate_field('MB_USAGE_02', sum, 'TOTAL_VOLUME_02')
                                        .aggregate_field('MB_USAGE_03', sum, 'TOTAL_VOLUME_03')
                                        .aggregate_field('MB_USAGE_04', sum, 'TOTAL_VOLUME_04')
                                        .aggregate_field('MB_USAGE_05', sum, 'TOTAL_VOLUME_05')
                                        .aggregate_field('MB_USAGE_06', sum, 'TOTAL_VOLUME_06')
                                        .aggregate_field('MB_USAGE_07', sum, 'TOTAL_VOLUME_07')
                                        .aggregate_field('MB_USAGE_08', sum, 'TOTAL_VOLUME_08')
                                        .aggregate_field('MB_USAGE_09', sum, 'TOTAL_VOLUME_09')
                                        .aggregate_field('MB_USAGE_10', sum, 'TOTAL_VOLUME_10')
                                        .aggregate_field('MB_USAGE_11', sum, 'TOTAL_VOLUME_11')
                                        .aggregate_field('MB_USAGE_12', sum, 'TOTAL_VOLUME_12')
                                        .aggregate_field('MB_USAGE_13', sum, 'TOTAL_VOLUME_13')
                                        .aggregate_field('MB_USAGE_14', sum, 'TOTAL_VOLUME_14')
                                        .aggregate_field('MB_USAGE_15', sum, 'TOTAL_VOLUME_15')
                                        .aggregate_field('MB_USAGE_16', sum, 'TOTAL_VOLUME_16')
                                        .aggregate_field('MB_USAGE_17', sum, 'TOTAL_VOLUME_17')
                                        .aggregate_field('MB_USAGE_18', sum, 'TOTAL_VOLUME_18')
                                        .aggregate_field('MB_USAGE_19', sum, 'TOTAL_VOLUME_19')
                                        .aggregate_field('MB_USAGE_20', sum, 'TOTAL_VOLUME_20')
                                        .aggregate_field('MB_USAGE_21', sum, 'TOTAL_VOLUME_21')
                                        .aggregate_field('MB_USAGE_22', sum, 'TOTAL_VOLUME_22')
                                        .aggregate_field('MB_USAGE_23', sum, 'TOTAL_VOLUME_23')
            | "Create UL Dictionary" >> beam.Map(lambda x: x._asdict())
            | "Write To Dl File" >> beam.io.WriteToText(output_path)
                    #sample output- {'USAGE_DATE': '21/01/2022', 'TOTAL_CUSTOMERS': 20, 'PEAK_DL_AVG_SPEED': 2788.55, 'OFFPEAK_DL_AVG_SPEED': 2303.0, 'PEAK_DL_MIN_SPEED': 479.0, 'PEAK_DL_MAX_SPEED': 4425.0, 'OFFPEAK_DL_MIN_SPEED': 224.0, 'OFFPEAK_DL_MAX_SPEED': 4749.0, 'TOTAL_VOLUME_00': 1022.18, 'TOTAL_VOLUME_01': 960.08}
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run()

if __name__ == '__main__':
  run()