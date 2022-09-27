import sys
import uuid
from pymongo import MongoClient

# Print Warning Message with Different Colors
# Write Warning Message in Red Then Reset Font Color Back to White
RESET = '\033[0;0m'
WARNING = '\033[1;31m'

def write_warning_message(msg):
    sys.stdout.write(WARNING)
    print(f'\n{msg}\n')
    sys.stdout.write(RESET)

# Request User Input Until Valid Input Provided
# Display Warning Message if Input is Invalid
# Return User Input When Valid Input Provided
def get_valid_input(prompt_msg, valid_inputs, warning_message):
    req_input = input(prompt_msg).strip()

    # Check if User Input is In Array of Valid Inputs
    while req_input not in valid_inputs:
        write_warning_message(warning_message)
        req_input = input(prompt_msg).strip()

    return int(req_input)

def get_valid_num_input(prompt_msg, warning_message):
    isValid = False
    while not isValid:
        try:
            # Request Input And Cast to Int in Try Catch Block
            req_input = int(input(prompt_msg).strip())

            # Check if User Input is Number Greater Than 0
            if req_input > 0:
                isValid = True
            else:
                write_warning_message(warning_message) 
        except ValueError:
            write_warning_message(warning_message)

    return req_input

# Get Request Data
def make_request():
    # Generate Unique Request Id Using Universally Unique Identifier (UUID)
    # UUID 4th Gen Generates a 36 Character Id
    rfm_id = str(uuid.uuid4())

    # Benchmark Type - Specify Data Sample (File)
    benchmark_type_prompt = 'Enter Benchmark Type As 1 (DVD) Or 2 (NDBench): '
    benchmark_type_warning = 'Invalid Benchmark Type! Must Enter 2!'
    benchmark_type_values =[ '2', '2']
    
    benchmark_type = get_valid_input(benchmark_type_prompt, benchmark_type_values, benchmark_type_warning)

    # Workload Metric - Specify Metric to MapReduce (Column)
    workload_metric_prompt = 'Enter Workload Metric As 1 (CPU), 2 (Network In),\n 3 (Network Out) Or 4 (Memory): '
    workload_metric_warning = 'Invalid Workload Metric! Must Enter Either 1, 2, 3, or 4!'
    workload_metric_values = ['1', '2', '3', '4']
    
    workload_metric = get_valid_input(workload_metric_prompt, workload_metric_values, workload_metric_warning)

    # Batch Unit - Specify Number of Samples in Each Batch, ie Num Maps
    bach_unit_prompt = 'Enter Batch Unit: '
    bach_unit_warning = 'Invalid Batch Unit! Must Enter Greater than 0!'
    
    batch_unit = get_valid_num_input(bach_unit_prompt, bach_unit_warning)

    # Batch Id - Specifies Starting Batch Id
    bach_id_prompt = 'Enter Batch Id: '
    bach_id_warning = 'Invalid Batch Id! Must Enter Greater than 0!'
    
    batch_id = get_valid_num_input(bach_id_prompt, bach_id_warning)

    # Batch Size - Specify Number of Batches to Run, ie Num Reduces
    bach_size_prompt = 'Enter Batch Size: '
    bach_size_warning = 'Invalid Batch Size! Must Enter Greater than 0!'
    
    batch_size = get_valid_num_input(bach_size_prompt, bach_size_warning)

    # Data Type - Specifies Testing or Training Data Sample (File)
    data_type_prompt = 'Enter Data Type As 1 (Training) Or 2 (Testing): '
    data_type_warning = 'Invalid Data Type! Must Enter Either 1 or 2!'
    data_type_values = ['1', '2']
    
    data_type = get_valid_input(data_type_prompt, data_type_values, data_type_warning)

    return rfm_id, benchmark_type, workload_metric, batch_unit, batch_id, batch_size, data_type
    
# Get File Name from Request
def get_file_name(benchmark_type, data_type): 
    # Benchmark and Data Types Array Specifying the File
    benchmark_types = ['DVD', 'NDBench']
    data_types = ['training', 'testing']

    file_prefix = benchmark_types[benchmark_type - 1]
    file_suffix = data_types[data_type - 1]

    return f'{file_prefix}-{file_suffix}'

# Get Column Name from Request
def get_metric(workload_metric): 
    # Benchmark Types Array Specifying Columns
    workload_metrics = ['CPUUtilization_Average', 'NetworkIn_Average', 'NetworkOut_Average', 'MemoryUtilization_Average']
    return workload_metrics[workload_metric - 1]

# Script Starting Point
if __name__ == '__main__':
    # Connect to MongoDB and Access Data Cluster 
    client = MongoClient(f'mongodb+srv://base:base@cluster0.jtkf3.mongodb.net/data?retryWrites=true&w=majority')
    db = client['data']

    # Get User Inputs for Request
    rfm_id, benchmark_type, workload_metric, batch_unit, batch_id, batch_size, data_type = make_request()

    # Get File Name, ie Collection Name in MongoDB
    collection_name = get_file_name(benchmark_type, data_type)
    collection = db[collection_name]

    # Get Column Name, ie Metric to Evaluate
    metric = get_metric(workload_metric)

    first_id = batch_id * batch_unit
    last_id = first_id + batch_size * batch_unit - 1

    # Get Data from Collection using Aggregate
    # Define Pipeline to Get Specific Values from DB
    pipeline = [
        {
            '$match': {
                'Line': {
                    '$gte': first_id,
                    '$lte': last_id
                }
            }
        }, {
            '$group': {
                '_id': {
                    '$ceil': {
                        '$divide': [
                            {
                                '$add': ['$Line', 1, -first_id]
                            },
                            batch_unit,
                        ]
                    }
                },
                metric: {
                    '$push': f'${metric}'
                }
            }
        }, {
            '$sort': { f'{metric}': 1 }
        }, {
            '$project': {
                '_id': 0,
                'batch_id': '$_id',
                'min': {
                    '$min': f'${metric}'
                },                
                'max': {
                    '$max': f'${metric}'
                },
                'median': {
                    '$cond': {
                        'if': { '$eq': [{ '$mod': [{ '$size': [f'${metric}'] }, 2] }, 0] },
                        'then': {
                            '$divide': [{
                                '$add': [ 
                                    {                            
                                    '$arrayElemAt': [
                                        f'${metric}', { '$divide': [{ '$size': [f'${metric}'] }, 2] }]
                                    },
                                    {                        
                                    '$arrayElemAt': [
                                        f'${metric}',  { '$subtract': [{ '$divide': [{ '$size': [f'${metric}'] }, 2]}, 1]}]
                                    }
                                ]
                            }, 2]
                        },
                        'else': {
                            '$arrayElemAt': [
                                f'${metric}', { '$floor': { '$divide': [{ '$size': [f'${metric}'] }, 2] }}
                            ]
                        }
                    }
                },
                'std_dev': {
                    '$stdDevSamp': f'${metric}'
                }
            }
        }, {
            '$sort': { 'batch_id': 1 }
        }
    ]

    # Print Request and Response
    print("\nRequest:")
    rfm = {"rfm_id": rfm_id,  "benchmark_type": benchmark_type, "workload_metric": workload_metric, "batch_unit": batch_unit, "batch_id": batch_id, "batch_size": batch_size, "data_type": data_type}
    print(rfm)

    print("\nResponse:")
    cursor = collection.aggregate(pipeline)
    
    for cur in cursor:
        print(cur)
