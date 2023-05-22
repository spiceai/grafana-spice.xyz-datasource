package plugin

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/decimal128"
	"github.com/apache/arrow/go/v10/arrow/memory"
)

const (
	TEST_API_KEY = "_"
)

func TestArrowColumnToArray(t *testing.T) {
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

		data := arrowColumnToArray(columnType, column)

		int8data := data.([]int8)

		if len(int8data) != 10 {
			t.Fatal("wrong array lentgh")
		}

		if int8data[1] != 1 {
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

		data := arrowColumnToArray(columnType, column)

		int16data := data.([]int16)

		if len(int16data) != 10 {
			t.Fatal("wrong array lentgh")
		}

		if int16data[1] != 1 {
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

		data := arrowColumnToArray(columnType, column)

		int32data := data.([]int32)

		if len(int32data) != 10 {
			t.Fatal("wrong array lentgh")
		}

		if int32data[1] != 1 {
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

		data := arrowColumnToArray(columnType, column)

		int64data := data.([]int64)

		if len(int64data) != 10 {
			t.Fatal("wrong array lentgh")
		}

		if int64data[1] != 1 {
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

		data := arrowColumnToArray(columnType, column)

		stringdata := data.([]string)

		if len(stringdata) != 10 {
			t.Fatal("wrong array lentgh")
		}

		if stringdata[1] != "value: 1" {
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

		data := arrowColumnToArray(columnType, column)

		float64data := data.([]float64)

		if len(float64data) != 10 {
			t.Fatal("wrong array lentgh")
		}

		if float64data[1] != 1.8446744073709553e+18 {
			t.Fatal("wrong value")
		}
	})
}

// TODO: use mock SpiceClient or test api key

// func TestQueryData(t *testing.T) {
// 	spice := gospice.NewSpiceClient()
// 	defer spice.Close()

// 	if err := spice.Init(TEST_API_KEY); err != nil {
// 		panic(fmt.Errorf("error initializing SpiceClient: %w", err))
// 	}

// 	ds := Datasource{
// 		spice: *spice,
// 	}

// 	resp, err := ds.QueryData(
// 		context.Background(),
// 		&backend.QueryDataRequest{
// 			Queries: []backend.DataQuery{
// 				{RefID: "A",
// 					JSON: json.RawMessage(`{"QueryText": "SELECT * FROM btc.recent_blocks LIMIT 10"}`),
// 				},
// 			},
// 		},
// 	)
// 	if err != nil {
// 		t.Error(err)
// 	}

// 	if len(resp.Responses) != 1 {
// 		t.Fatal("QueryData must return a response")
// 	}
// }
