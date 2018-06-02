# lambda-go-kinesis-streams-to-firehose
AWS Lambda Function by Go that syncs data from Kinesis Data Streams to Kinesis Data Firehose.

## Requirements

- For dependency management, [dep](https://github.com/golang/dep).
- This function requires a minimum version of Go 1.9.


## Create lambda function archive

```
$ dep ensure
$ cd main
$ go build -pkgdir ../vendor/ -o ../syncKinesisStreamstoFirehose
$ cd ../
$ chmod +x syncKinesisStreamstoFirehose
$ zip -j syncKinesisStreamtoFirehose.zip syncKinesisStreamtoFirehose
```

## Parameters

- This function uses below Lambda Environment Variables.

| Name | Desc | Example |
|------|------|---------|
| Region | Firehose AWS Region | ap-northeast-1 |
| DeliveryStream | Put target DeliveryStream(for fixed define.) | fixed-stream |
| DefaultStream | Put target DeliveryStream(for default.) | default-stream |
| TargetColumn | Base column for generatting Firehose DeliveryStream.<br>(Now this function extract ltsv format column only.) | tag |
| RemovePrefix | Common remove prefix for generatting Firehose DeliveryStream. | remove_prefix. |
| AddPrefix | Common additional prefix for generatting Firehose DeliveryStream. | header. |
| ReplacePattern | Replace pattern for generatting Firehose DeliveryStream.<br>Replace before pattern are separeted `/`.<br>Multiple pattern are separeted `,`.  | httpd/firehose |

- Target DeliveryStream generate flow.
  - If `DeliveryStream` specified use `DeliveryStream`.
  - Extract `TargetColumn` value from received record(LTSV format).
  - Remove `RemovePrefix` from `TargetColumn` value.
  - ReplacePattern are applied.
  - Add `AddPrefix` to `TargetColumn` value.

## Deploy command example

```
$ aws iam create-role --role-name sync-lambda-role --assume-role-policy-document file://deploy_config/lambda-assume-role-policy.json
$ aws iam put-role-policy --role-name sync-lambda-role --policy-name sync-lambda-policy --policy-document file://deploy_config/sync-lambda-policy.json
$ aws lambda create-function --function-name sync-kinesis-streams-to-firehose --runtime go1.x --role arn:aws:iam::[AWS Account ID]:role/sync-lambda-role --handler syncKinesisStreamstoFirehose --zip-file fileb://syncKinesisStreamtoFirehose.zip
$ aws lambda update-function-configuration --function-name hl-dev-tagged_ltsv --environment "Variables={AddPrefix=AAA,Region=BBB,RemovePrefix=CCC,ReplacePattern=DDD,TargetColumn=EEE,DeliveryStream=FFF,DefaultStream=GGG}"
$ aws lambda publish-version --function-name sync-kinesis-streams-to-firehose
$ aws lambda create-alias --function-name sync-kinesis-streams-to-firehose --name Active --function-version 1
$ aws lambda create-event-source-mapping --function-name arn:aws:lambda:ap-northeast-1:[AWS Account ID]:function:sync-kinesis-streams-to-firehose:Active --event-source arn:aws:kinesis:ap-northeast-1:[AWS Account ID]:stream/sync-target --starting-position LATEST
```

### License

- License: Apache License, Version 2.0