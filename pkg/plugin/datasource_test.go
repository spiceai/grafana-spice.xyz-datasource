package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/decimal128"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/spiceai/gospice/v3"
)

const (
	TEST_API_KEY = "323337|b42eceab2e7c4a60a04ad57bebea830d" // spicehq/gospice-tests
)

func TestArrowColumnToArray(t *testing.T) {
	field := arrow.Field{}

	t.Run("arrow.BOOL", func(t *testing.T) {
		columnType := arrow.BOOL
		pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewBooleanBuilder(pool)
		defer builder.Release()

		builder.Resize(10)
		for i := 0; i < 10; i++ {
			builder.Append(i%2 == 0)
		}

		column := builder.NewBooleanArray()
		defer column.Release()

		data := arrowColumnToArray(field, columnType, column)

		results := data.([]bool)

		if len(results) != 10 {
			t.Fatal("wrong array length")
		}

		if results[1] {
			t.Fatal("wrong value")
		}
	})

	t.Run("arrow.UINT8", func(t *testing.T) {
		columnType := arrow.UINT8
		pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewUint8Builder(pool)
		defer builder.Release()

		builder.Resize(10)
		for i := 0; i < 10; i++ {
			builder.Append(uint8(i))
		}

		column := builder.NewUint8Array()
		defer column.Release()

		data := arrowColumnToArray(field, columnType, column)

		results := data.([]uint8)

		if len(results) != 10 {
			t.Fatal("wrong array length")
		}

		if results[1] != 1 {
			t.Fatal("wrong value")
		}
	})

	t.Run("arrow.UINT16", func(t *testing.T) {
		columnType := arrow.UINT16
		pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewUint16Builder(pool)
		defer builder.Release()

		builder.Resize(10)
		for i := 0; i < 10; i++ {
			builder.Append(uint16(i))
		}

		column := builder.NewUint16Array()
		defer column.Release()

		data := arrowColumnToArray(field, columnType, column)

		results := data.([]uint16)

		if len(results) != 10 {
			t.Fatal("wrong array length")
		}

		if results[1] != 1 {
			t.Fatal("wrong value")
		}
	})

	t.Run("arrow.UINT32", func(t *testing.T) {
		columnType := arrow.UINT32
		pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewUint32Builder(pool)
		defer builder.Release()

		builder.Resize(10)
		for i := 0; i < 10; i++ {
			builder.Append(uint32(i))
		}

		column := builder.NewUint32Array()
		defer column.Release()

		data := arrowColumnToArray(field, columnType, column)

		results := data.([]uint32)

		if len(results) != 10 {
			t.Fatal("wrong array length")
		}

		if results[1] != 1 {
			t.Fatal("wrong value")
		}
	})

	t.Run("arrow.UINT64", func(t *testing.T) {
		columnType := arrow.UINT64
		pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewUint64Builder(pool)
		defer builder.Release()

		builder.Resize(10)
		for i := 0; i < 10; i++ {
			builder.Append(uint64(i))
		}

		column := builder.NewUint64Array()
		defer column.Release()

		data := arrowColumnToArray(field, columnType, column)

		results := data.([]uint64)

		if len(results) != 10 {
			t.Fatal("wrong array length")
		}

		if results[1] != 1 {
			t.Fatal("wrong value")
		}
	})

	t.Run("arrow.INT8", func(t *testing.T) {
		columnType := arrow.INT8
		pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewInt8Builder(pool)
		defer builder.Release()

		builder.Resize(10)
		for i := 0; i < 10; i++ {
			builder.Append(int8(i))
		}

		column := builder.NewInt8Array()
		defer column.Release()

		data := arrowColumnToArray(field, columnType, column)

		results := data.([]int8)

		if len(results) != 10 {
			t.Fatal("wrong array length")
		}

		if results[1] != 1 {
			t.Fatal("wrong value")
		}
	})

	t.Run("arrow.INT16", func(t *testing.T) {
		columnType := arrow.INT16
		pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewInt16Builder(pool)
		defer builder.Release()

		builder.Resize(10)
		for i := 0; i < 10; i++ {
			builder.Append(int16(i))
		}

		column := builder.NewInt16Array()
		defer column.Release()

		data := arrowColumnToArray(field, columnType, column)

		results := data.([]int16)

		if len(results) != 10 {
			t.Fatal("wrong array length")
		}

		if results[1] != 1 {
			t.Fatal("wrong value")
		}
	})

	t.Run("arrow.INT32", func(t *testing.T) {
		columnType := arrow.INT32
		pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewInt32Builder(pool)
		defer builder.Release()

		builder.Resize(10)
		for i := 0; i < 10; i++ {
			builder.Append(int32(i))
		}

		column := builder.NewInt32Array()
		defer column.Release()

		data := arrowColumnToArray(field, columnType, column)

		results := data.([]int32)

		if len(results) != 10 {
			t.Fatal("wrong array length")
		}

		if results[1] != 1 {
			t.Fatal("wrong value")
		}
	})

	t.Run("arrow.INT64", func(t *testing.T) {
		columnType := arrow.INT64
		pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewInt64Builder(pool)
		defer builder.Release()

		builder.Resize(10)
		for i := 0; i < 10; i++ {
			builder.Append(int64(i))
		}

		column := builder.NewInt64Array()
		defer column.Release()

		data := arrowColumnToArray(field, columnType, column)

		results := data.([]int64)

		if len(results) != 10 {
			t.Fatal("wrong array length")
		}

		if results[1] != 1 {
			t.Fatal("wrong value")
		}
	})

	t.Run("arrow.FLOAT32", func(t *testing.T) {
		columnType := arrow.FLOAT32
		pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewFloat32Builder(pool)
		defer builder.Release()

		builder.Resize(10)
		for i := 0; i < 10; i++ {
			builder.Append(float32(i))
		}

		column := builder.NewFloat32Array()
		defer column.Release()

		data := arrowColumnToArray(field, columnType, column)

		results := data.([]float32)

		if len(results) != 10 {
			t.Fatal("wrong array length")
		}

		if results[1] != 1.0 {
			t.Fatal("wrong value")
		}
	})

	t.Run("arrow.FLOAT64", func(t *testing.T) {
		columnType := arrow.FLOAT64
		pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewFloat64Builder(pool)
		defer builder.Release()

		builder.Resize(10)
		for i := 0; i < 10; i++ {
			builder.Append(float64(i))
		}

		column := builder.NewFloat64Array()
		defer column.Release()

		data := arrowColumnToArray(field, columnType, column)

		results := data.([]float64)

		if len(results) != 10 {
			t.Fatal("wrong array length")
		}

		if results[1] != 1.0 {
			t.Fatal("wrong value")
		}
	})
	t.Run("arrow.DECIMAL128", func(t *testing.T) {
		columnType := arrow.DECIMAL128
		pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewDecimal128Builder(pool, &arrow.Decimal128Type{})
		defer builder.Release()

		builder.Resize(10)
		for i := 0; i < 10; i++ {
			builder.Append(decimal128.New(1, uint64(i)))
		}

		column := builder.NewDecimal128Array()
		defer column.Release()

		data := arrowColumnToArray(field, columnType, column)

		results := data.([]float64)

		if len(results) != 10 {
			t.Fatal("wrong array length")
		}

		if results[1] != 1.8446744073709553e+18 {
			t.Fatal("wrong value")
		}
	})

	t.Run("arrow.STRING", func(t *testing.T) {
		columnType := arrow.STRING
		pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewStringBuilder(pool)
		defer builder.Release()

		builder.Resize(10)
		for i := 0; i < 10; i++ {
			builder.Append(fmt.Sprintf("value: %v", i))
		}

		column := builder.NewStringArray()
		defer column.Release()

		data := arrowColumnToArray(field, columnType, column)

		results := data.([]string)

		if len(results) != 10 {
			t.Fatal("wrong array length")
		}

		if results[1] != "value: 1" {
			t.Fatal("wrong value")
		}
	})

	t.Run("arrow.TIMESTAMP", func(t *testing.T) {
		columnType := arrow.TIMESTAMP
		pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewTimestampBuilder(pool, &arrow.TimestampType{})
		defer builder.Release()

		now := time.Now()

		builder.Resize(10)
		for i := 0; i < 10; i++ {
			builder.Append(arrow.Timestamp(now.UnixNano()))
		}

		column := builder.NewTimestampArray()
		defer column.Release()

		data := arrowColumnToArray(arrow.Field{
			Type: &arrow.TimestampType{
				Unit: arrow.Nanosecond,
			},
		}, columnType, column)

		results := data.([]time.Time)

		if len(results) != 10 {
			t.Fatal("wrong array length")
		}

		if results[1].UTC() != now.UTC() {
			t.Fatalf("wrong value, %v %v", results[1].UTC(), now.UTC())
		}
	})
}

func TestQueryData(t *testing.T) {
	spice := gospice.NewSpiceClient()
	defer spice.Close()

	if err := spice.Init(TEST_API_KEY); err != nil {
		panic(fmt.Errorf("error initializing SpiceClient: %w", err))
	}

	ds := Datasource{
		spice: *spice,
	}

	resp, err := ds.QueryData(
		context.Background(),
		&backend.QueryDataRequest{
			Queries: []backend.DataQuery{
				{RefID: "A",
					JSON: json.RawMessage(`{"QueryText": "SELECT * FROM btc.recent_blocks LIMIT 10","QuerySource": "default"}`),
				},
			},
		},
	)
	if err != nil {
		t.Error(err)
	}

	if len(resp.Responses) != 1 {
		t.Fatal("QueryData must return a response")
	}
}
