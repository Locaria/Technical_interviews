## no comments
## -- No Docstring, no explanation of what the code does, no inline comments

## no error handling 
## -- No try-except block, no logging, no error handling
## -- If there's an error, the pipeline will fail without any error message

## no edge-case handling 
## -- No handling for edge cases where 'value' might not be present or not an integer
## -- If the input data is not as expected, the pipeline will fail without any error message

## input is hard-coded in the pipeline instead of being passed as an argument (i.e. via PipelineOptions)
## -- not parameterized for flexibility and reusability

## output is hard-coded just as file name, without any path. (also not as an argument)
## -- not parameterized for flexibility and reusability

## variable names (i.e. final_variable, ultimate_variable) are not descriptive
## -- not following best practices for variable naming
## -- makes the code harder to read and understand


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io import ReadFromBigQuery, WriteToText

class RemovePositiveNumbers(beam.DoFn):
    def process(self, element):
        # Ensure the 'value' key exists and is an integer
        if 'value' in element and isinstance(element['value'], int):
            if element['value'] < 0:
                yield element
        else:
            # Handle edge cases where 'value' might not be present or not an integer
            logging.warning(f"Skipping element with invalid data: {element}")

class ComputeSquare(beam.DoFn):
    def process(self, element):
        # Ensure the 'value' key exists and is an integer
        if 'value' in element and isinstance(element['value'], int):
            element['value'] = element['value'] * element['value']
            yield element
        else:
            # Handle edge cases where 'value' might not be present or not an integer
            logging.warning(f"Skipping element with invalid data: {element}")

def run(input_query, output_path, pipeline_options):
    p = beam.Pipeline(options=pipeline_options)

    # Read data from BigQuery
    input_data = (
        p
        | 'ReadFromBigQuery' >> ReadFromBigQuery(
            query=input_query,
            use_standard_sql=True
        )
    )

    # Remove positive numbers from the dataset
    negative_numbers = input_data | 'RemovePositiveNumbers' >> beam.ParDo(RemovePositiveNumbers())

    # Compute the square of each number
    squared_numbers = negative_numbers | 'ComputeSquare' >> beam.ParDo(ComputeSquare())

    # Write the results to the specified output path
    squared_numbers | 'WriteOutput' >> WriteToText(output_path)

    p.run().wait_until_finish()

if __name__ == '__main__':
    # Set up PipelineOptions
    pipeline_options = PipelineOptions()
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    # Parameterize these variables for flexibility and reusability
    google_cloud_options.project = 'your-project-id'  # Replace with your GCP project ID
    input_query = 'SELECT id, value FROM `project.dataset.table`'  # Replace with your BigQuery query
    output_path = 'gs://your-bucket/output/output.txt'  # Replace with your​⬤