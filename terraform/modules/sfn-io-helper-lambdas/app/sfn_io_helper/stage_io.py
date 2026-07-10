import os
import re
import json
import logging
from typing import List
from datetime import datetime
from uuid import uuid4

from botocore import xform_name
from botocore.exceptions import ClientError  # type: ignore

from . import s3, s3_object, sqs

logger = logging.getLogger()


def get_input_uri_key(stage):
    return f"{xform_name(stage).upper()}_INPUT_URI"


def get_output_uri_key(stage):
    return f"{xform_name(stage).upper()}_OUTPUT_URI"


def get_output_path(sfn_state):
    """
    The output path is ``<OutputPrefix>/<workflow_name>`` with the ``vX.Y.Z`` suffix normalized to just ``X``

    Example:
        ``OutputPrefix=s3://idseq-samples/samples/1/19/11, workflow_name=short-read-mngs-v8.3.15`` -> ``s3://idseq-samples/samples/1/19/11/short-read-mngs-8``
    """
    output_prefix = sfn_state["OutputPrefix"]
    assert output_prefix.startswith("s3://")
    sub_path = re.sub(r"v(\d+)\..+", r"\1", get_workflow_name(sfn_state))
    return f"{output_prefix.rstrip('/')}/{sub_path}"


def get_stage_input(sfn_state, stage):
    input_uri = sfn_state[get_input_uri_key(stage)]
    return json.loads(s3_object(input_uri).get()["Body"].read().decode().strip() or '{}')


def put_stage_input(sfn_state, stage, stage_input):
    input_uri = sfn_state[get_input_uri_key(stage)]
    s3_object(input_uri).put(Body=json.dumps(stage_input).encode())


def get_stage_output(sfn_state, stage):
    output_uri = sfn_state[get_output_uri_key(stage)]
    try:
        return json.loads(s3_object(output_uri).get()["Body"].read().decode().strip() or '{}')
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return {}
        else:
            raise e


def read_state_from_s3(sfn_state, current_state):
    stage = current_state.replace("ReadOutput", "")
    sfn_state.setdefault("Result", {})
    stage_output = get_stage_output(sfn_state, stage)

    # Extract Batch job error, if any, and drop error metadata to avoid overrunning the Step Functions state size limit
    batch_job_error = sfn_state.pop("BatchJobError", {})
    # If the stage succeeded, don't throw an error
    if not sfn_state.get("BatchJobDetails", {}).get(stage):
        if batch_job_error and next(iter(batch_job_error)).startswith(stage):
            error_type = type(stage_output["error"], (Exception,), dict())
            raise error_type(stage_output.get("cause", stage_output.get("message")))

    # HACK: don't include list outputs due to the SFN state size limit
    sfn_state["Result"].update({k: v for k, v in stage_output.items() if not isinstance(v, list)})

    return sfn_state


def trim_batch_job_details(sfn_state):
    """
    Remove large redundant batch job description items from Step Function state to avoid overrunning the Step Functions
    state size limit.
    """
    sfn_state["BatchJobDetails"] = {k: {} for k in sfn_state["BatchJobDetails"]}
    return sfn_state


def segment_path(path: str) -> List[str]:
    _path = path
    segments: List[str] = []
    while _path:
        _path, segment = os.path.split(_path)
        segments.insert(0, segment)
    return segments


def get_workflow_name(sfn_state):
    """
    The workflow name is extracted from any ``*_WDL_URI`` entry in the SFN state.

    Example:
        ``HOST_FILTER_WDL_URI=s3://seqtoid-workflows-staging-030998640247/short-read-mngs-v8.3.15/host_filter.wdl`` -> ``short-read-mngs-v8.3.15``
    """
    for k, v in sfn_state.items():
        if k.endswith("_WDL_URI"):
            segments = [s for s in segment_path(v) if re.match(r".*-v(\d+)", s)]
            name = segments[0] if segments else os.path.basename(str(v))
            return os.path.splitext(name)[0]
    raise ValueError("Could not find workflow name")


def link_outputs(sfn_state):
    if len(list(sfn_state["Input"])) == 0:
        return

    stages_json_uri = sfn_state.get("STAGES_IO_MAP_JSON")
    stage_io_dict = {}
    if stages_json_uri:
        stage_io_dict = json.loads(s3_object(stages_json_uri).get()["Body"].read().decode())

    stripped_result = {k.split(".")[1]: v for k, v in sfn_state.get("Result", {}).items()}

    for stage in sfn_state["Input"].keys():
        stage_input = sfn_state["Input"][stage]
        for input_name, source in stage_io_dict.get(stage, {}).items():
            if isinstance(source, list):
                stage_input[input_name] = sfn_state["Input"].get(source[0], {}).get(source[1])
            elif source in stripped_result:
                stage_input[input_name] = stripped_result[source]
        put_stage_input(sfn_state=sfn_state, stage=stage, stage_input=stage_input)


def preprocess_sfn_input(sfn_state, aws_region, aws_account_id, state_machine_name):
    # TODO: add input validation assertions here (use JSON schema?)
    output_path = get_output_path(sfn_state)

    for stage in sfn_state["Input"].keys():
        sfn_state[get_input_uri_key(stage)] = os.path.join(output_path, f"{xform_name(stage)}_input.json")
        sfn_state[get_output_uri_key(stage)] = os.path.join(output_path, f"{xform_name(stage)}_output.json")
        for compute_env in "SPOT", "EC2":
            memory_key = stage + compute_env + "Memory"
            memory_default_key = memory_key + "Default"
            if memory_default_key in os.environ:
                sfn_state.setdefault(memory_key, int(os.environ[memory_default_key]))
            vcpu_key = stage + compute_env + "Vcpu"
            vcpu_default_key = vcpu_key + "Default"
            if vcpu_default_key in os.environ:
                sfn_state.setdefault(vcpu_key, int(os.environ[vcpu_key + "Default"]))

    link_outputs(sfn_state)

    return sfn_state


def broadcast_stage_complete(execution_id: str, stage: str):
    if not os.environ.get("SQS_QUEUE_URLS"):
        return

    sqs_queue_urls = os.environ["SQS_QUEUE_URLS"].split(",")

    assert len(execution_id.split(":")) == 8
    _, _, _, aws_region, aws_account_id, _, state_machine_name, execution_name = execution_id.split(":")

    state_machine_arn = f"arn:aws:states:{aws_region}:{aws_account_id}:stateMachine:{state_machine_name}"

    body = json.dumps({
        "version": "0",
        "id": str(uuid4()),
        "detail-type": "Step Functions Execution Status Change",
        "source": "aws.states",
        "account": aws_account_id,
        "time": datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
        "region": aws_region,
        "resources": [execution_id],
        "detail": {
            "executionArn": execution_id,
            "stateMachineArn": state_machine_arn,
            "name": execution_name,
            "status": "RUNNING",
            "lastCompletedStage": re.sub(r'(?<!^)(?=[A-Z])', '_', stage).lower(),
            # We don't set this because it isn't used yet and we don't have this
            #   field in the lambda, but it is part of the schema for these
            #   messages so we may need to add it.
            # "startDate": 1551225271984,
            "stopDate": None,
            "input": "{}",
            "inputDetails": {
                "included": None
            },
            "output": None,
            "outputDetails": None
        }
    })

    for squs_que_url in sqs_queue_urls:
        sqs.send_message(
            QueueUrl=squs_que_url,
            MessageBody=body,
        )


def delete_restricted_intermediate_files(sfn_state):
    """
    Delete all files listed in ``restricted_intermediate_files`` from the workflow's S3 output directory.

    Deletion errors are logged but never raised, so that a missing file does not stop this cleanup or impact the caller.
    """

    restricted_regexes = {
        re.compile(r".*bowtie2_ercc_filtered\d+\.fastq$"),
        re.compile(r".*bowtie2_host\.bam$"),
        re.compile(r".*bowtie2_host_filtered\d+\.fastq$"),
        re.compile(r".*bowtie2_human_filtered\d+\.fastq$"),
        re.compile(r".*fastp\d+\.fastq$"),
        re.compile(r".*hisat2_host_filtered\d+\.fastq$"),
        re.compile(r".*sample_validated\.fastq$"),
        re.compile(r".*sample_quality_filtered\.fastq$"),
        re.compile(r".*sample\.hostfiltered\.fastq$"),
        re.compile(r".*sample\.hostfiltered\.bam$"),
        re.compile(r".*sample\.humanfiltered\.bam$"),
        re.compile(r".*sample\.humanfiltered\.fastq$"),
        re.compile(r".*valid_input\d+\.fastq$"),
        re.compile(r".*validated_\d+\.fastq\.gz$"),
    }

    output_path = get_output_path(sfn_state)
    bucket_name, prefix = output_path.split("/", 3)[2:]
    prefix = f"{prefix}/"

    logger.info(
        "Scanning for restricted intermediate files in %s (bucket=%s prefix=%s)",
        output_path,
        bucket_name,
        prefix
    )
    # We use the legacy paginator because it allows for retrieving only the files in a given directory, instead of recursing to all files
    paginator = s3.meta.client.get_paginator("list_objects_v2")
    objects_to_delete = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter="/"):
        if 'Contents' in page:
            for page_contents in page['Contents']:
                s3_key = page_contents['Key']
                if s3_key != prefix:
                    logger.debug("Trying to match S3 object s3://%s/%s", bucket_name, s3_key)
                    for restricted_regex in restricted_regexes:
                        if restricted_regex.fullmatch(s3_key):
                            objects_to_delete.append({"Key": s3_key})
                            break

    if objects_to_delete:
        logger.info("Deleting restricted intermediate files: %s", json.dumps(objects_to_delete))
        try:
            s3.Bucket(bucket_name).delete_objects(Delete={"Objects": objects_to_delete})
        except Exception as e:
            logger.warning("Error deleting restricted intermediate files: %s", e)
    else:
        logger.info("No restricted intermediate files to delete")


def delete_sample_files(sfn_state):
    """
    Delete all files listed in the workflow's S3 sample directory.

    Deletion errors are logged but never raised so that a missing file does not impact the caller.
    """

    output_prefix = sfn_state["OutputPrefix"]
    assert output_prefix.startswith("s3://")

    # Remove the last part of the path and replace it with "fastqs/", including a terminating backslash
    # IE: s3://idseq-samples/samples/1/19/11 -> bucket=idseq-samples prefix=samples/1/19/fastqs/
    path_as_array = output_prefix.rstrip("/").split("/")[2:]
    bucket_name = path_as_array[0]
    prefix = "/".join([
        *path_as_array[1:-1],
        "fastqs",
        ""
    ])
    s3_uri = f"s3://{bucket_name}/{prefix}"

    logger.info("Deleting all files in %s (bucket=%s prefix=%s)", s3_uri, bucket_name, prefix)
    try:
        responses = s3.Bucket(bucket_name).objects.filter(Prefix=prefix).delete()
        logger.info("Deleted sample files: %s", json.dumps(responses))
    except Exception as e:
        logger.warning("Unexpected error deleting sample files in %s: %s", s3_uri, e)
