from ..version import __version__

ASSUME_ROLE_POLICY_DOCUMENT = """{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
"""

BUCKET_POLICY_DOCUMENT_TEMPLATE = """{{
    "Version": "2012-10-17",
    "Statement": [
        {{
            "Effect": "Allow",
            "Principal": {{
                "AWS": "{role_arn}"
            }},
            "Action": "*",
            "Resource": [
                "{bucket_arn}",
                "{bucket_arn}/*"
            ]
        }}
    ]
}}
"""

PYTHON_DEPENDENCIES = [
    'boto3',
    'git+ssh://git@github.com/dagster-io/dagster.git'
    '@{version}#egg=dagma&subdirectory=python_modules/dagma'.format(version=__version__),
]
