package converter

import (
	"strings"
)

// FirehoseConverter KinesisStream AggregatedRecordDatas to FirehosePutDataMap converter
type FirehoseConverter struct {
	DeliveryStream  string
	DefaultStream   string
	TargetColumn    string
	RemovePrefix    string
	AddPrefix       string
	ReplacePatterns [][2]string
}

// ConvertToFirehoseDatas converts KinesisStream AggregatedRecordDatas to FirehosePutDataMap
// MapStructure DeliveryStream -> PutDatas
func (converter *FirehoseConverter) ConvertToFirehoseDatas(recordDatas []string) map[string][]string {
	result := map[string][]string{}
	if len(recordDatas) == 0 {
		return result
	}

	if converter.DeliveryStream != "" {
		result[converter.DeliveryStream] = recordDatas
		return result
	}

	for _, recordData := range recordDatas {
		targetValue := extractValueFromLtsvLine(recordData, converter.TargetColumn)
		if targetValue == "" {
			addMapValue(result, converter.DefaultStream, recordData)
		} else {
			putTarget := converter.convertToDeliveryStream(targetValue)
			addMapValue(result, putTarget, recordData)
		}
	}

	return result
}

// CreateReplacePatterns converts string setting to structured ReplacePatterns.
func CreateReplacePatterns(replaceConfig string) [][2]string {
	result := [][2]string{}
	replaceSettings := strings.Split(replaceConfig, ",")

	for _, replaceSetting := range replaceSettings {
		setting := strings.SplitN(replaceSetting, "/", 2)
		if len(setting) < 2 {
			continue
		}
		replacePattern := [2]string{setting[0], setting[1]}
		result = append(result, replacePattern)
	}
	return result
}

func (converter *FirehoseConverter) convertToDeliveryStream(targetValue string) string {
	var result string
	if strings.HasPrefix(targetValue, converter.RemovePrefix) {
		result = strings.Replace(targetValue, converter.RemovePrefix, "", 1)
	} else {
		result = targetValue
	}

	for _, replacePattern := range converter.ReplacePatterns {
		result = strings.Replace(result, replacePattern[0], replacePattern[1], 1)
	}

	result = converter.AddPrefix + result

	return result
}

func addMapValue(targetMap map[string][]string, key string, value string) {
	targetList, exists := targetMap[key]
	if exists == false {
		targetList = []string{}
	}
	targetList = append(targetList, value)
	targetMap[key] = targetList
}

func extractValueFromLtsvLine(targetLine string, targetLabel string) string {
	columns := strings.Split(targetLine, "\t")
	for _, column := range columns {
		lv := strings.SplitN(column, ":", 2)
		if len(lv) < 2 {
			continue
		}
		if lv[0] == targetLabel {
			return lv[1]
		}
	}
	return ""
}
