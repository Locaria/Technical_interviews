import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class RemovePositiveNumbers(beam.DoFn):
    def process(self, element):
        if element['value'] >= 0:
            yield element

class ComputeSquare(beam.DoFn):
    def process(self, element):
        element['value'] = element['value'] * element['value']
        yield element

def run(argv=None):
    pipeline_options = PipelineOptions()
    p = beam.Pipeline(options=pipeline_options)

    input_collection = (
        p
        | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(
            query='SELECT id, value FROM `project.dataset.table`',
            use_standard_sql=True
        )
    )

    final_variable = input_collection | 'RemovePositiveNumbers' >> beam.ParDo(RemovePositiveNumbers())

    ultimate_variable = final_variable | 'SquareNumbers' >> beam.ParDo(ComputeSquare())

    ultimate_variable | 'WriteOutput' >> beam.io.WriteToText('output.txt')

    p.run().wait_until_finish()

if __name__ == '__main__':
    run()











