package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"

	"github.com/spiceai/gospice"
)

// Make sure Datasource implements required interfaces. This is important to do
// since otherwise we will only get a not implemented error response from plugin in
// runtime. In this example datasource instance implements backend.QueryDataHandler,
// backend.CheckHealthHandler interfaces. Plugin should not implement all these
// interfaces- only those which are required for a particular task.
var (
	_ backend.QueryDataHandler      = (*Datasource)(nil)
	_ backend.CheckHealthHandler    = (*Datasource)(nil)
	_ instancemgmt.InstanceDisposer = (*Datasource)(nil)
)

func getSpiceClient(flightAddress string) *gospice.SpiceClient {
	if flightAddress != "" {
		return gospice.NewSpiceClientWithAddress(flightAddress)
	} else {
		return gospice.NewSpiceClient()
	}
}

// NewDatasource creates a new datasource instance.
func NewDatasource(settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	apiKey := settings.DecryptedSecureJSONData["apiKey"]

	config := &spiceSettings{}
	json.Unmarshal(settings.JSONData, &config)

	spice := getSpiceClient(config.FlightAddress)

	if apiKey == "" {
		return nil, fmt.Errorf("missing Spice AI apiKey")
	}

	if err := spice.Init(apiKey); err != nil {
		return nil, fmt.Errorf("failed to initizlize Spice AI client: %w", err)
	}

	return &Datasource{
		spice:    *spice,
		settings: settings,
	}, nil
}

// Datasource is an example datasource which can respond to data queries, reports
// its health and has streaming skills.
type Datasource struct {
	spice    gospice.SpiceClient
	settings backend.DataSourceInstanceSettings
}

// Dispose here tells plugin SDK that plugin wants to clean up resources when a new instance
// created. As soon as datasource settings change detected by SDK old datasource instance will
// be disposed and a new one will be created using NewSampleDatasource factory function.
func (d *Datasource) Dispose() {
	d.spice.Close()
}

func arrowColumnToArray(field arrow.Field, columnType arrow.Type, column arrow.Array) interface{} {
	length := column.Len()

	switch columnType {
	case arrow.BOOL:
		arr := make([]bool, length)
		for i := 0; i < column.Len(); i++ {
			arr[i] = column.(*array.Boolean).Value(i)
		}
		return arr

	case arrow.UINT8:
		arr := make([]uint8, length)
		for i := 0; i < column.Len(); i++ {
			arr[i] = column.(*array.Uint8).Value(i)
		}
		return arr

	case arrow.UINT16:
		arr := make([]uint16, length)
		for i := 0; i < column.Len(); i++ {
			arr[i] = column.(*array.Uint16).Value(i)
		}
		return arr

	case arrow.UINT32:
		arr := make([]uint32, length)
		for i := 0; i < column.Len(); i++ {
			arr[i] = column.(*array.Uint32).Value(i)
		}
		return arr

	case arrow.UINT64:
		arr := make([]uint64, length)
		for i := 0; i < column.Len(); i++ {
			arr[i] = column.(*array.Uint64).Value(i)
		}
		return arr

	case arrow.INT8:
		arr := make([]int8, length)
		for i := 0; i < column.Len(); i++ {
			arr[i] = column.(*array.Int8).Value(i)
		}
		return arr

	case arrow.INT16:
		arr := make([]int16, length)
		for i := 0; i < column.Len(); i++ {
			arr[i] = column.(*array.Int16).Value(i)
		}
		return arr

	case arrow.INT32:
		arr := make([]int32, length)
		for i := 0; i < column.Len(); i++ {
			arr[i] = column.(*array.Int32).Value(i)
		}
		return arr

	case arrow.INT64:
		arr := make([]int64, length)
		for i := 0; i < column.Len(); i++ {
			arr[i] = column.(*array.Int64).Value(i)
		}
		return arr

	case arrow.FLOAT32:
		arr := make([]float32, length)
		for i := 0; i < column.Len(); i++ {
			arr[i] = column.(*array.Float32).Value(i)
		}
		return arr

	case arrow.FLOAT64:
		arr := make([]float64, length)
		for i := 0; i < column.Len(); i++ {
			arr[i] = column.(*array.Float64).Value(i)
		}
		return arr

	case arrow.DECIMAL128:
		arr := make([]float64, column.Len())
		for i := 0; i < column.Len(); i++ {
			arr[i] = column.(*array.Decimal128).Value(i).ToFloat64(1)
		}
		return arr

	case arrow.STRING:
		arr := make([]string, length)
		for i := 0; i < column.Len(); i++ {
			arr[i] = column.(*array.String).Value(i)
		}
		return arr

	case arrow.TIMESTAMP:
		arr := make([]time.Time, length)
		timeUnit := field.Type.(*arrow.TimestampType).Unit
		for i := 0; i < column.Len(); i++ {
			arr[i] = column.(*array.Timestamp).Value(i).ToTime(timeUnit)
		}
		return arr

	case arrow.LIST:
		arr := make([]string, length)
		for i := 0; i < column.Len(); i++ {
			list := column.(*array.List)
			arr[i] = ""

			listType := list.DataType().ID()

			for j := 0; j < list.Len(); j++ {
				if j > 0 {
					arr[i] += ","
				}

				switch listType {
				case arrow.STRING:
					arr[i] += fmt.Sprintf("%v", list.ListValues().(*array.String).Value(j))
				case arrow.INT64:
					arr[i] += fmt.Sprintf("%v", list.ListValues().(*array.Int64).Value(j))
				}
			}
		}
		return arr
	}

	return nil
}

// Append converted column to the existing data Field
func appendColumnToField(field *data.Field, columnType arrow.Type, column interface{}) {
	switch columnType {
	case arrow.UINT8:
		for j := range column.([]uint8) {
			field.Append(column.([]uint8)[j])
		}

	case arrow.UINT16:
		for j := range column.([]uint16) {
			field.Append(column.([]uint16)[j])
		}

	case arrow.UINT32:
		for j := range column.([]uint32) {
			field.Append(column.([]uint32)[j])
		}

	case arrow.UINT64:
		for j := range column.([]uint64) {
			field.Append(column.([]uint64)[j])
		}
	case arrow.INT8:
		for j := range column.([]int8) {
			field.Append(column.([]int8)[j])
		}

	case arrow.INT16:
		for j := range column.([]int16) {
			field.Append(column.([]int16)[j])
		}

	case arrow.INT32:
		for j := range column.([]int32) {
			field.Append(column.([]int32)[j])
		}

	case arrow.INT64:
		for j := range column.([]int64) {
			field.Append(column.([]int64)[j])
		}

	case arrow.FLOAT32:
		for j := range column.([]float32) {
			field.Append(column.([]float32)[j])
		}

	case arrow.DECIMAL128, arrow.FLOAT64:
		for j := range column.([]float64) {
			field.Append(column.([]float64)[j])
		}

	case arrow.TIMESTAMP:
		for j := range column.([]time.Time) {
			field.Append(column.([]time.Time)[j])
		}

	case arrow.STRING, arrow.LIST:
		for j := range column.([]string) {
			field.Append(column.([]string)[j])
		}
	}
}

// QueryData handles multiple queries and returns multiple responses.
// req contains the queries []DataQuery (where each query contains RefID as a unique identifier).
// The QueryDataResponse contains a map of RefID to the response for each query, and each response
// contains Frames ([]*Frame).
func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	// create response struct
	response := backend.NewQueryDataResponse()

	// loop over queries and execute them individually.
	for _, q := range req.Queries {
		res := d.query(ctx, req.PluginContext, q)

		// save the response in a hashmap
		// based on with RefID as identifier
		response.Responses[q.RefID] = res
	}

	return response, nil
}

func (d *Datasource) query(ctx context.Context, pCtx backend.PluginContext, query backend.DataQuery) backend.DataResponse {
	var response backend.DataResponse

	if len(query.JSON) == 0 {
		return backend.ErrDataResponse(backend.StatusBadRequest, "empty query")
	}

	q := &spiceQuery{}
	err := json.Unmarshal(query.JSON, &q)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, fmt.Sprintf("json unmarshal: %v", err.Error()))
	}

	reader, err := d.spice.Query(ctx, q.QueryText)

	if err != nil {
		log.DefaultLogger.Error("err: %w", err)

		errMsg := err.Error()

		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return backend.ErrDataResponse(backend.StatusTimeout, errMsg)

		case errMsg == "rpc error: code = Unknown desc = Exceeded concurrent request limit":
			return backend.ErrDataResponse(backend.StatusTooManyRequests, errMsg)

		default:
			return backend.ErrDataResponse(backend.StatusInternal, errMsg)
		}
	}

	schema := reader.Schema()

	frame := data.NewFrame("response")

	var page int64 = 0

	for reader.Next() {
		record := reader.Record()
		defer record.Release()

		for i, field := range schema.Fields() {
			column := record.Column(i)
			defer column.Release()

			columnType := field.Type.ID()

			arr := arrowColumnToArray(field, columnType, column)

			// setup fields on first record
			if page == 0 {
				frame.Fields = append(frame.Fields,
					data.NewField(field.Name, nil, arr))
			} else {
				// append data to existing fields
				appendColumnToField(frame.Fields[i], columnType, arr)
			}
		}

		page++
	}

	response.Frames = append(response.Frames, frame)

	reader.Release()
	return response
}

// CheckHealth handles health checks sent from Grafana to the plugin.
// The main use case for these health checks is the test button on the
// datasource configuration page which allows users to verify that
// a datasource is working as expected.
func (d *Datasource) CheckHealth(ctx context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	var status = backend.HealthStatusOk
	var message = "Data source is working"

	reader, err := d.spice.Query(ctx, "SELECT number FROM eth.recent_blocks LIMIT 1")
	if err != nil {
		status = backend.HealthStatusError
		message = fmt.Sprintf("error querying: %v", err.Error())
	}

	for reader.Next() {
		record := reader.Record()
		defer record.Release()

		if record.NumRows() != 1 {
			status = backend.HealthStatusError
			message = "error querying"
		}
	}

	return &backend.CheckHealthResult{
		Status:  status,
		Message: message,
	}, nil
}
