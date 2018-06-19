package main

import (
	"context"
	"os"
	"sync"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/kimutansk/go-kinesis-deaggregation"
	"github.com/kimutansk/lambda-go-kinesis-streams-to-firehose/converter"
	"github.com/kimutansk/lambda-go-kinesis-streams-to-firehose/put"
	"go.uber.org/zap"
)

var sugar *zap.SugaredLogger
var once sync.Once

func main() {
	lambda.Start(syncKinesisStreamstoFirehose)
}

func syncKinesisStreamstoFirehose(ctx context.Context, kinesisEvent events.KinesisEvent) error {
	once.Do(func() { initialize() })

	recordTexts := []string{}
	for _, record := range kinesisEvent.Records {
		kinesisRecord := record.Kinesis
		dataBytes := kinesisRecord.Data
		if deaggregation.IsAggregatedRecord(dataBytes) {
			recordDatas, err := deaggregation.ExtractRecordDatas(dataBytes)
			if err != nil {
				sugar.Warnw("Failed deserialize Received Event.", "EventName", record.EventName, "DataBytes", dataBytes)
				continue
			}

			for _, recordData := range recordDatas {
				recordTexts = append(recordTexts, string(recordData))
			}
		} else {
			recordTexts = append(recordTexts, string(dataBytes))
		}
	}

	converter := converter.FirehoseConverter{
		DeliveryStream:  os.Getenv("DeliveryStream"),
		DefaultStream:   os.Getenv("DefaultStream"),
		TargetColumn:    os.Getenv("TargetColumn"),
		RemovePrefix:    os.Getenv("RemovePrefix"),
		AddPrefix:       os.Getenv("AddPrefix"),
		ReplacePatterns: converter.CreateReplacePatterns(os.Getenv("ReplacePattern")),
	}

	putDatas := converter.ConvertToFirehoseDatas(recordTexts)

	client := put.FirehosePutter{
		Region:        os.Getenv("Region"),
		DefaultStream: os.Getenv("DefaultStream"),
	}

	client.ParallelPut(putDatas)
	return nil
}

func initialize() {
	logger, _ := zap.NewDevelopment()
	sugar = logger.Sugar()
	sugar.Infow("Initializing lambda with Environment.",
		"Region", os.Getenv("Region"),
		"DeliveryStream", os.Getenv("DeliveryStream"),
		"DefaultStream", os.Getenv("DefaultStream"),
		"TargetColumn", os.Getenv("TargetColumn"),
		"RemovePrefix", os.Getenv("RemovePrefix"),
		"AddPrefix", os.Getenv("AddPrefix"),
		"ReplacePattern", os.Getenv("ReplacePattern"))
}
