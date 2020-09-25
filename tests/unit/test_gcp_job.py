from train.gcp_job import (
    build_job_config,
    build_remote_model_dir,
    build_remote_data_fname,
    GoogleAIPlatformJob,
    ModelDefn
)
from train import gcp_job


def test_build_job_config_simple(monkeypatch):
    monkeypatch.setenv('GOOGLE_AI_PLATFORM_DOCKER_IMAGE_URI',
                       'gcr.io/blah/myimage')
    monkeypatch.setenv('GOOGLE_AI_PLATFORM_BUCKET', 'gs://blah')

    expected = '''
labels:
  type: "production"
  owner: "alchemy"
  version: "1"
trainingInput:
  scaleTier: CUSTOM
  masterType: n1-standard-4
  args:
    - "--dirs"
    - gs://my_bucket/model_dir
    - "--data-dir"
    - gs://gs://blah/data
    - "--eval-batch-size"
    - '16'
  region: us-central1
  masterConfig:
    acceleratorConfig:
      count: '1'
      type: NVIDIA_TESLA_P100
    imageUri: "gcr.io/blah/myimage"
    '''

    result = build_job_config(
        model_dirs=['gs://my_bucket/model_dir'], version='1')
    print(result)
    assert expected.strip() == result.strip()


def test_build_job_config_infer_1(monkeypatch):
    monkeypatch.setenv('GOOGLE_AI_PLATFORM_DOCKER_IMAGE_URI',
                       'gcr.io/blah/myimage')
    monkeypatch.setenv('GOOGLE_AI_PLATFORM_BUCKET', 'gs://blah')

    expected = '''
labels:
  type: "production"
  owner: "alchemy"
  version: "1"
trainingInput:
  scaleTier: CUSTOM
  masterType: n1-standard-4
  args:
    - "--dirs"
    - gs://my_bucket/model_dir
    - "--infer"
    - gs://my_bucket/data/spring_jan_2020.jsonl
    - "--data-dir"
    - gs://gs://blah/data
    - "--eval-batch-size"
    - '16'
  region: us-central1
  masterConfig:
    acceleratorConfig:
      count: '1'
      type: NVIDIA_TESLA_P100
    imageUri: "gcr.io/blah/myimage"
    '''

    result = build_job_config(model_dirs=['gs://my_bucket/model_dir'],
                              files_for_inference=[
                                  'gs://my_bucket/data/spring_jan_2020.jsonl'],
                              version='1')
    assert expected.strip() == result.strip()


def test_build_job_config_infer_2(monkeypatch):
    monkeypatch.setenv('GOOGLE_AI_PLATFORM_DOCKER_IMAGE_URI',
                       'gcr.io/blah/myimage')
    monkeypatch.setenv('GOOGLE_AI_PLATFORM_BUCKET', 'gs://blah')

    expected = '''
labels:
  type: "production"
  owner: "alchemy"
  version: "1"
trainingInput:
  scaleTier: CUSTOM
  masterType: n1-standard-4
  args:
    - "--dirs"
    - gs://my_bucket/model_dir
    - "--infer"
    - gs://my_bucket/data/spring_jan_2020.jsonl
    - gs://my_bucket/data/spring_jan_2021.jsonl
    - "--data-dir"
    - gs://gs://blah/data
    - "--eval-batch-size"
    - '16'
  region: us-central1
  masterConfig:
    acceleratorConfig:
      count: '1'
      type: NVIDIA_TESLA_P100
    imageUri: "gcr.io/blah/myimage"
    '''

    result = build_job_config(model_dirs=['gs://my_bucket/model_dir'],
                              files_for_inference=[
                                  'gs://my_bucket/data/spring_jan_2020.jsonl', 'gs://my_bucket/data/spring_jan_2021.jsonl'],
                              version='1')

    assert expected.strip() == result.strip()


def test_build_job_config_infer_3(monkeypatch):
    monkeypatch.setenv('GOOGLE_AI_PLATFORM_DOCKER_IMAGE_URI',
                       'gcr.io/blah/myimage')
    monkeypatch.setenv('GOOGLE_AI_PLATFORM_BUCKET', 'gs://blah')

    expected = '''
labels:
  type: "production"
  owner: "alchemy"
  version: "1"
trainingInput:
  scaleTier: CUSTOM
  masterType: n1-standard-4
  args:
    - "--dirs"
    - gs://my_bucket/model_dir1
    - gs://my_bucket/model_dir2
    - "--infer"
    - gs://my_bucket/data/spring_jan_2020.jsonl
    - gs://my_bucket/data/spring_jan_2021.jsonl
    - "--data-dir"
    - gs://gs://blah/data
    - "--eval-batch-size"
    - '16'
  region: us-central1
  masterConfig:
    acceleratorConfig:
      count: '1'
      type: NVIDIA_TESLA_P100
    imageUri: "gcr.io/blah/myimage"
    '''

    result = build_job_config(model_dirs=['gs://my_bucket/model_dir1', 'gs://my_bucket/model_dir2'],
                              files_for_inference=[
                                  'gs://my_bucket/data/spring_jan_2020.jsonl', 'gs://my_bucket/data/spring_jan_2021.jsonl'],
                              version='1')

    assert expected.strip() == result.strip()


def test_build_remote_model_dir(monkeypatch):
    monkeypatch.setenv('GOOGLE_AI_PLATFORM_BUCKET', 'mybucket')
    assert build_remote_model_dir('abc', 2) == \
        'gs://mybucket/tasks/abc/models/2'


def test_build_remote_data_fname(monkeypatch):
    monkeypatch.setenv('GOOGLE_AI_PLATFORM_BUCKET', 'mybucket')
    assert build_remote_data_fname('blah.jsonl') == \
        'gs://mybucket/data/blah.jsonl'


def test_build_remote_data_fname_long(monkeypatch):
    monkeypatch.setenv('GOOGLE_AI_PLATFORM_BUCKET', 'mybucket')
    assert build_remote_data_fname('/tmp/foo/blah.jsonl') == \
        'gs://mybucket/data/blah.jsonl'


def test_get_job_status_v1(monkeypatch):
    def mock_describe(job_id):
        return {
            "createTime": "2020-04-02T23:24:18Z",
            "etag": "2TFzYuw9IIA=",
            "jobId": "t_99e5cb31_8343_4ec3_8b5e_c6cdedfb7e3d_v_5",
            "labels": {
                    "owner": "alchemy",
                    "type": "production",
                    "version": "1"
            },
            "startTime": "2020-04-02T23:27:09Z",
            "state": "RUNNING",
            "trainingInput": {
                "args": [
                    "--dir",
                    "gs://_REDACTED_/tasks/8b5e-c6cdedfb7e3d/models/5",
                    "--infer",
                    "gs://_REDACTED_/data/spring_jan_2020.jsonl",
                    "gs://_REDACTED_/data/spring_feb_2020.jsonl",
                    "--eval-batch-size",
                    "16"
                ],
                "masterConfig": {
                    "acceleratorConfig": {
                        "count": "1",
                        "type": "NVIDIA_TESLA_P100"
                    },
                    "imageUri": "gcr.io/_REDACTED_"
                },
                "masterType": "n1-standard-4",
                "region": "us-central1",
                "scaleTier": "CUSTOM"
            },
            "trainingOutput": {}
        }
    monkeypatch.setattr(gcp_job, 'describe_ai_platform_job', mock_describe)

    job = GoogleAIPlatformJob(123)
    assert job.get_state() == 'RUNNING'
    assert job.get_model_defns() == [ModelDefn('8b5e-c6cdedfb7e3d', '5')]


def test_get_job_status_v2(monkeypatch):
    def mock_describe(job_id):
        return {
            "createTime": "2020-04-02T23:24:18Z",
            "etag": "2TFzYuw9IIA=",
            "jobId": "t_99e5cb31_8343_4ec3_8b5e_c6cdedfb7e3d_v_5",
            "labels": {
                    "owner": "alchemy",
                    "type": "production",
                    "version": "2"
            },
            "startTime": "2020-04-02T23:27:09Z",
            "state": "RUNNING",
            "trainingInput": {
                "args": [
                    "--dirs",
                    "gs://_REDACTED_/tasks/8b5e-c6cdedfb7e3d/models/6",
                    "gs://_REDACTED_/tasks/8b5e-c6cdedfb7e3d/models/7",
                    "--infer",
                    "gs://_REDACTED_/data/spring_jan_2020.jsonl",
                    "gs://_REDACTED_/data/spring_feb_2020.jsonl",
                    "--eval-batch-size",
                    "16"
                ],
                "masterConfig": {
                    "acceleratorConfig": {
                        "count": "1",
                        "type": "NVIDIA_TESLA_P100"
                    },
                    "imageUri": "gcr.io/_REDACTED_"
                },
                "masterType": "n1-standard-4",
                "region": "us-central1",
                "scaleTier": "CUSTOM"
            },
            "trainingOutput": {}
        }
    monkeypatch.setattr(gcp_job, 'describe_ai_platform_job', mock_describe)

    job = GoogleAIPlatformJob(123)
    assert job.get_state() == 'RUNNING'
    assert job.get_model_defns() == [ModelDefn('8b5e-c6cdedfb7e3d', '6'),
                                     ModelDefn('8b5e-c6cdedfb7e3d', '7')]


def test_get_job_status_invalid(monkeypatch):
    def mock_describe(job_id):
        return None
    monkeypatch.setattr(gcp_job, 'describe_ai_platform_job', mock_describe)

    job = GoogleAIPlatformJob(None)
    assert job.get_state() is None
    assert job.get_model_defns() == []


def test_get_job_status_exception(monkeypatch):
    def mock_describe(job_id):
        raise Exception("Testing")
    monkeypatch.setattr(gcp_job, 'describe_ai_platform_job', mock_describe)

    job = GoogleAIPlatformJob(None)
    assert job.get_state() is None
    assert job.get_model_defns() == []
