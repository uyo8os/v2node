//go:build !linux

package dispatcher

import (
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/features/stats"
)

func supportsMeteredSplice(buf.Writer) bool { return false }

func meteredDownlink(counter stats.Counter) stats.Counter { return counter }
