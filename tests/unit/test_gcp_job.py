from train.gcp_job import (
    build_job_config,
    build_remote_model_dir,
    build_remote_data_fname
)


def test_build_job_config_simple(monkeypatch):
    monkeypatch.setenv('GOOGLE_AI_PLATFORM_DOCKER_IMAGE_URI',
                       'gcr.io/blah/myimage')

    expected = '''
labels:
  type: "production"
  owner: "alchemy"
  version: "1"
trainingInput:
  scaleTier: CUSTOM
  masterType: n1-standard-4
  args:
    - "--dir"
    - gs://my_bucket/model_dir
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
        model_dir='gs://my_bucket/model_dir', version='1')

    assert expected.strip() == result.strip()


def test_build_job_config_infer_1(monkeypatch):
    monkeypatch.setenv('GOOGLE_AI_PLATFORM_DOCKER_IMAGE_URI',
                       'gcr.io/blah/myimage')

    expected = '''
labels:
  type: "production"
  owner: "alchemy"
  version: "1"
trainingInput:
  scaleTier: CUSTOM
  masterType: n1-standard-4
  args:
    - "--dir"
    - gs://my_bucket/model_dir
    - "--infer"
    - gs://my_bucket/data/spring_jan_2020.jsonl
    - "--eval-batch-size"
    - '16'
  region: us-central1
  masterConfig:
    acceleratorConfig:
      count: '1'
      type: NVIDIA_TESLA_P100
    imageUri: "gcr.io/blah/myimage"
    '''

    result = build_job_config(model_dir='gs://my_bucket/model_dir',
                              infer_filenames=[
                                  'gs://my_bucket/data/spring_jan_2020.jsonl'],
                              version='1')
    assert expected.strip() == result.strip()


def test_build_job_config_infer_2(monkeypatch):
    monkeypatch.setenv('GOOGLE_AI_PLATFORM_DOCKER_IMAGE_URI',
                       'gcr.io/blah/myimage')

    expected = '''
labels:
  type: "production"
  owner: "alchemy"
  version: "1"
trainingInput:
  scaleTier: CUSTOM
  masterType: n1-standard-4
  args:
    - "--dir"
    - gs://my_bucket/model_dir
    - "--infer"
    - gs://my_bucket/data/spring_jan_2020.jsonl
    - gs://my_bucket/data/spring_jan_2021.jsonl
    - "--eval-batch-size"
    - '16'
  region: us-central1
  masterConfig:
    acceleratorConfig:
      count: '1'
      type: NVIDIA_TESLA_P100
    imageUri: "gcr.io/blah/myimage"
    '''

    result = build_job_config(model_dir='gs://my_bucket/model_dir',
                              infer_filenames=[
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
