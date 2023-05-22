package plugin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
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
		panic("missing Spice.xyz apiKey")
	}

	if err := spice.Init(apiKey); err != nil {
		panic(fmt.Errorf("error initializing SpiceClient: %w", err))
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

func arrowColumnToArray(columnType arrow.Type, column arrow.Array) interface{} {
	length := column.Len()

	switch columnType {
	case arrow.STRING:
		arr := make([]string, length)
		for i := 0; i < column.Len(); i++ {
			arr[i] = column.(*array.String).Value(i)
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

	case arrow.DECIMAL128:
		arr := make([]float64, column.Len())
		for i := 0; i < column.Len(); i++ {
			arr[i] = column.(*array.Decimal128).Value(i).ToFloat64(1)
		}
		return arr
	}

	return nil
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
		panic(fmt.Errorf("error querying: %w", err))
	}

	schema := reader.Schema()

	frame := data.NewFrame("response")

	for reader.Next() {
		record := reader.Record()
		defer record.Release()

		for i, field := range schema.Fields() {
			column := record.Column(i)
			defer column.Release()

			columnSchema := schema.Field(i)
			columnType := columnSchema.Type.ID()

			arr := arrowColumnToArray(columnType, column)

			frame.Fields = append(frame.Fields,
				data.NewField(field.Name, nil, arr))
		}
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

	reader, err := d.spice.Query(ctx, "SELECT * FROM eth.recent_blocks LIMIT 1")
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
