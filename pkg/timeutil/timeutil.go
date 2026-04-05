package timeutil

import "time"

func NowUnixMilli() int64 { return time.Now().UnixMilli() }

// yyyymmdd HH:MM (KST)
func NowKSTString() string {
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		return time.Now().Format("20060102 15:04:05.000")
	}
	return time.Now().In(loc).Format("20060102 15:04:05.000")
}
