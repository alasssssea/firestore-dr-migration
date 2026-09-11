package partition

import "testing"

func TestCount(t *testing.T) {
	cases := []struct {
		name                string
		total               int64
		minDocsPerPartition int
		maxPartitions       int
		want                int
	}{
		{"below one partition stays whole", 5000, 10000, 32, 1},
		{"exactly one partition worth", 10000, 10000, 32, 1},
		{"floor division", 60000, 10000, 32, 6},
		{"capped by maxPartitions", 1000000, 10000, 32, 32},
		{"non-positive granularity defaults to 1", 5, 0, 32, 5},
		{"non-positive cap defaults to 1", 1000000, 10000, 0, 1},
		{"never below 1", 0, 10000, 32, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Count(tc.total, tc.minDocsPerPartition, tc.maxPartitions); got != tc.want {
				t.Errorf("Count(%d, %d, %d) = %d; want %d",
					tc.total, tc.minDocsPerPartition, tc.maxPartitions, got, tc.want)
			}
		})
	}
}
