package main

import "testing"

func TestParseJSONStrToSQLField(t *testing.T) {
	jsonStr := `{"id":"90122","locale":"es","type":"ad-click","data":"{\"ad_unit_id\":\"9721975108ab97ff\",\"revenue\":0.00115,\"revenue_precision\":\"exact\",\"network_name\":\"Mintegral\",\"dsp_name\":\"\",\"timestamp\":1727654398538,\"os\":\"android\",\"report_id\":\"a804827e-17f4-45b9-8704-188e381efaff\",\"ab\":{\"younger\":\"111\"},\"plan\":null,\"source\":\"official\",\"apk_source\":\"official\",\"backup_id\":\"f4860e46-480f-4a70-b096-69aaa28f7456\"}","time":1727654400028}`
	retData, sql, _ := ParseJSONStrToSQLField("ttt", jsonStr)
	t.Log(retData)
	t.Log("(------------------------)")
	t.Log(sql)

	// errStr := "Table with name story_statistic does not exist!"
	// errStr = `Binder Error: Table "story" does not have a column with name "data_plan"`
	// tableName, field, _ := ParseSqlErrType(errStr)
	// t.Log("tableName--", tableName)
	// t.Log("field--", field)

	// newSql, err := ProduceCreateSql(retData, tableName, field)
	// if err != nil {
	// 	t.Error(err)
	// 	return
	// }

	// t.Log("new sql ---", newSql)
}

func TestParseSqlErrType(t *testing.T) {
	errStr := "Table with name t does not exist!"
	errStr = "Table hhh does not have a column with name xxxx"
	t.Log(ParseSqlErrType(errStr))
}
