package put

import (
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"go.uber.org/zap"
)

// MaxRetry Max retry for failure.
const MaxRetry = 5

// RetryIntervalMillis Retry interval for failure.
const RetryIntervalMillis = 500

// MaxRecordsPerRequest Max record per request.
// See https://docs.aws.amazon.com/firehose/latest/APIReference/API_PutRecordBatch.html
// Each PutRecordBatch request supports up to 500 records.
// Each record in the request can be as large as 1,000 KB (before 64-bit encoding), up to a limit of 4 MB for the entire request.
// These limits cannot be changed.
const MaxRecordsPerRequest = 500

// FirehosePutter Put recordDatas to Kinesis Firehose.
type FirehosePutter struct {
	Region        string
	DefaultStream string
}

var sugar *zap.SugaredLogger
var once sync.Once

// ParallelPut puts PutData to Firehose Parallel.
func (putter *FirehosePutter) ParallelPut(putDatas map[string][]string) {
	once.Do(func() { initialize() })

	batchRequests := []*firehose.PutRecordBatchInput{}
	for key, putDataSlice := range putDatas {
		batchRequests = append(batchRequests, createPutRecordBatchInputs(key, putDataSlice)...)
	}

	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		panic("unable to load SDK config, " + err.Error())
	}

	cfg.Region = putter.Region
	client := firehose.New(cfg)

	// Put Data to Firehose concurrently.
	wg := &sync.WaitGroup{}
	for _, batchRequest := range batchRequests {
		wg.Add(1)
		batchRequestLocalScope := batchRequest
		go func() {
			defer wg.Done()
			putter.putToFirehose(client, batchRequestLocalScope, 0)
		}()
	}
	wg.Wait()
}

func (putter *FirehosePutter) putToFirehose(client *firehose.Firehose, batchRequest *firehose.PutRecordBatchInput, retryCount int) {

	req := client.PutRecordBatchRequest(batchRequest)
	resp, err := req.Send()
	if err != nil {
		sugar.Warnw("PutRecordBatchInputRequest gets Error Status.", "DeliveryStream", *batchRequest.DeliveryStreamName, "FailedCount", retryCount+1, "Error", err.Error())

		if strings.Contains(err.Error(), "ResourceNotFound") {
			batchRequest.DeliveryStreamName = &putter.DefaultStream
		}
		judgeRetryCountout(*batchRequest.DeliveryStreamName, retryCount)

		time.Sleep(RetryIntervalMillis * time.Millisecond)
		putter.putToFirehose(client, batchRequest, retryCount+1)
	} else {
		failedCount := *resp.FailedPutCount
		if failedCount != 0 {
			sugar.Warnw("PutRecordBatchOutput contains FailedPut.", "DeliveryStream", *batchRequest.DeliveryStreamName, "FailedPutCount", failedCount)

			retryRequest := &firehose.PutRecordBatchInput{}
			retryRecords := []firehose.Record{}
			retryRequest.DeliveryStreamName = batchRequest.DeliveryStreamName

			for index, response := range resp.RequestResponses {
				if *response.ErrorCode != "" || *response.ErrorMessage != "" || *response.RecordId == "" {
					sugar.Warnw("PutRecordBatch failed.", "DeliveryStream", *batchRequest.DeliveryStreamName, "FailedRecord", string(batchRequest.Records[index].Data), "ErrorCode", *response.ErrorCode, "ErrorMessage", *response.ErrorMessage)

					// BatchRequest contains only one DeliveryStream, so any record contains ResourceNotFound, set DefaultStream for all requests.
					if strings.Contains(*response.ErrorMessage, "ResourceNotFound") {
						retryRequest.DeliveryStreamName = &putter.DefaultStream
					}

					retryRecords = append(retryRecords, batchRequest.Records[index])
				}
			}
			judgeRetryCountout(*batchRequest.DeliveryStreamName, retryCount)

			retryRequest.Records = retryRecords
			time.Sleep(RetryIntervalMillis * time.Millisecond)
			putter.putToFirehose(client, retryRequest, retryCount+1)
		} else {
			// For Confirm.
			// sugar.Infow("PutRecordBatch succeed.", "DeliveryStream", *batchRequest.DeliveryStreamName)
		}
	}
}

func createPutRecordBatchInputs(deliveryStream string, putDataSlice []string) []*firehose.PutRecordBatchInput {
	resultRequests := []*firehose.PutRecordBatchInput{}
	resultRecords := []firehose.Record{}
	recordInRequest := 0

	for _, putData := range putDataSlice {
		if putData != "" {
			record := firehose.Record{}
			if recordInRequest >= MaxRecordsPerRequest {
				createInput := &firehose.PutRecordBatchInput{}
				createInput.DeliveryStreamName = &deliveryStream
				createInput.Records = resultRecords
				resultRequests = append(resultRequests, createInput)
				resultRecords = []firehose.Record{}
				recordInRequest = 0
			}
			// Firehose not add lineseparator between Records, so add lineseparator to each Records.
			record.Data = []byte(putData + "\n")
			resultRecords = append(resultRecords, record)
			recordInRequest++
		}
	}

	createInput := &firehose.PutRecordBatchInput{}
	createInput.DeliveryStreamName = &deliveryStream
	createInput.Records = resultRecords
	resultRequests = append(resultRequests, createInput)
	return resultRequests
}

func judgeRetryCountout(deliveryStream string, retryCount int) {
	if MaxRetry <= retryCount {
		sugar.Errorw("PutToFirehose count out.", "DeliveryStream", deliveryStream, "FailedCount", retryCount+1)
		panic("PutToFirehose count out.")
	}
}

func initialize() {
	logger, _ := zap.NewDevelopment()
	sugar = logger.Sugar()
}
