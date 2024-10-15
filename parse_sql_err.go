package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strings"
)

type GaliPaiDC struct {
	ID        string `json:"id"`     //用户id，或者设备id
	Locale    string `json:"locale"` //国家
	Type      string `json:"type"`   //打点类型
	TimeStamp int64  `json:"time"`   //打点时间戳-13位-ms级别
	Data      string `json:"data"`   //业务打点数据，大json
	//Business  string `json:"business"` //header中获取
}

// ParseJSONStrToSQLField 解析 JSON 字符串并生成 SQL 插入语句
func ParseJSONStrToSQLField(tableName, jsonStr string) (map[string]interface{}, string, error) {
	var data map[string]interface{}
	retData := make(map[string]interface{})
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		log.Fatalf("JSON解析失败: %v", err)
		return nil, "", errors.New("body data is not json format")
	}

	fields := []string{}
	values := []string{}

	// 递归解析 JSON
	parseNestedJSON(retData, data, "", &fields, &values)

	fieldStr := strings.Join(fields, ", ")
	valueStr := strings.Join(values, ", ")
	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", tableName, fieldStr, valueStr)

	return retData, sql, nil
}

// parseNestedJSON 递归解析 JSON 对象，将字段名按层级展开
func parseNestedJSON(retData, data map[string]interface{}, prefix string, fields *[]string, values *[]string) {
	for key, value := range data {
		fullKey := key
		if prefix != "" {
			fullKey = prefix + "_" + key
		}

		for {
			if nestedMap, err := tryParseJSONString(value); err == nil {
				value = nestedMap // 更新 value 为解析后的 map
			} else {
				break
			}
		}

		v := reflect.ValueOf(value)
		switch v.Kind() {
		case reflect.Map:
			nestedData, ok := value.(map[string]interface{})
			if ok {
				parseNestedJSON(retData, nestedData, fullKey, fields, values)
			}
		default:
			valueType := formatValue(value)
			if valueType != "NULL" {
				*fields = append(*fields, fullKey)
				*values = append(*values, valueType)
				retData[fullKey] = value
			}

		}
	}
}

// tryParseJSONString 尝试解析字符串形式的 JSON
func tryParseJSONString(value interface{}) (map[string]interface{}, error) {
	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("不是 JSON 字符串: %v", value)
	}
	var result map[string]interface{}
	err := json.Unmarshal([]byte(str), &result)
	return result, err
}

// formatValue 格式化值为 SQL 插入语句中的形式
func formatValue(value interface{}) string {
	switch v := value.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''")) // 转义单引号
	case float64, int, int64:
		return fmt.Sprintf("%v", v)
	default:
		return "NULL"
	}
}

// TableNotExistSQLErr = "Table with name t does not exist!"
// FiledNotExistOnTableSQLErr = "Table t does not have a column with name c"
const (
	TableNotExistPattern        = "Table with name (?<t>\\w+) does not exist!"
	FiledNotExistOnTablePattern = `Table "(?P<t2>[^"]+)" .* name "(?P<c>[^"]+)`
)

// 解析字符串，是否可以匹配到上面的字符串，如果可以匹配到的话，将其中的变量获取
func ParseSqlErrType(errStr string) (string, string, string) {
	patterns := []struct {
		Pattern string
		Table   string
		Field   string
	}{
		{TableNotExistPattern, "t", ""},
		{FiledNotExistOnTablePattern, "t2", "c"},
	}

	for _, p := range patterns {
		re := regexp.MustCompile(p.Pattern)
		match := re.FindStringSubmatch(errStr)

		if match != nil {
			var tValue, cValue string
			if p.Table != "" {
				tValue = match[re.SubexpIndex(p.Table)]
			}
			if p.Field != "" {
				cValue = match[re.SubexpIndex(p.Field)]
			}
			return tValue, cValue, p.Pattern
		}
	}
	return "", "", "" // 如果没有匹配，返回空值
}

var CreateTableSentence = "CREATE TABLE %s (%s)"
var CreateFieldSentence = "ALTER TABLE %s ADD COLUMN %s %s"

func ProduceCreateSql(retData map[string]interface{}, tableName, field string) (string, error) {
	if tableName == "" {
		return "", errors.New("table name is empty")
	}

	if field == "" {
		partSql := ""
		for fieldName, value := range retData {
			fType := getTypeFromValue(value)
			if fType == "NULL" {
				continue
			}
			partSql += fmt.Sprintf("%s %s,", fieldName, fType)
		}

		partSql = partSql[:len(partSql)-1]
		createTableSql := fmt.Sprintf(CreateTableSentence, tableName, partSql)
		return createTableSql, nil
	}

	//创建表字段
	fType := getTypeFromValue(retData[field])
	createFieldSql := fmt.Sprintf(CreateFieldSentence, tableName, field, fType)
	return createFieldSql, nil

}

func getTypeFromValue(value interface{}) string {
	if value == nil {
		return "NULL"
	}

	switch v := value.(type) {
	case string:
		return "VARCHAR"
	case bool:
		return "BOOLEAN"
	case float32, float64:
		return "DOUBLE"
	case int, int8, int16, int32, int64:
		return "BIGINT"
	default:
		// 使用反射获取类型信息，方便调试
		fmt.Printf("Unsupported type: %v\n", reflect.TypeOf(v))
		return "NULL"
	}
}
