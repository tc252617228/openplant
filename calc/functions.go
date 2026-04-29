package calc

import "strings"

type FunctionCategory string

const (
	CategoryPointSnapshot   FunctionCategory = "point_snapshot"
	CategoryStatusMethod    FunctionCategory = "status_method"
	CategoryHistorySnapshot FunctionCategory = "history_snapshot"
	CategoryHistorySeries   FunctionCategory = "history_series"
	CategoryStatistic       FunctionCategory = "statistic"
	CategorySystem          FunctionCategory = "system"
	CategoryTime            FunctionCategory = "time"
	CategoryWrite           FunctionCategory = "write"
	CategoryPeriodic        FunctionCategory = "periodic"
	CategoryMirror          FunctionCategory = "mirror"
	CategoryWaterSteam      FunctionCategory = "water_steam"
)

type Function struct {
	Name        string
	Category    FunctionCategory
	Signature   string
	Description string
	Implemented bool
	Notes       string
}

func Functions() []Function {
	out := make([]Function, len(functions))
	copy(out, functions)
	return out
}

func LookupFunction(name string) (Function, bool) {
	normalized := NormalizeName(name)
	for _, fn := range functions {
		if strings.ToLower(fn.Name) == normalized {
			return fn, true
		}
	}
	if !strings.Contains(normalized, ".") {
		withOP := "op." + normalized
		for _, fn := range functions {
			if strings.ToLower(fn.Name) == withOP {
				return fn, true
			}
		}
	}
	return Function{}, false
}

func MustLookupFunction(name string) Function {
	fn, ok := LookupFunction(name)
	if !ok {
		panic("unknown OpenPlant calculation function: " + name)
	}
	return fn
}

func NamesByCategory(category FunctionCategory) []string {
	names := make([]string, 0)
	for _, fn := range functions {
		if fn.Category == category {
			names = append(names, fn.Name)
		}
	}
	return names
}

func NormalizeName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

var functions = []Function{
	{Name: "op.value", Category: CategoryPointSnapshot, Signature: `op.value(point)`, Description: "Realtime point value.", Implemented: true},
	{Name: "op.status", Category: CategoryPointSnapshot, Signature: `op.status(point)`, Description: "Realtime point status.", Implemented: true},
	{Name: "op.time", Category: CategoryPointSnapshot, Signature: `op.time(point)`, Description: "Realtime point timestamp.", Implemented: true},
	{Name: "op.get", Category: CategoryPointSnapshot, Signature: `op.get(point)`, Description: "Realtime point object/value lookup.", Implemented: true},
	{Name: "op.dynamic", Category: CategoryPointSnapshot, Signature: `op.dynamic(point)`, Description: "Realtime dynamic point field lookup.", Implemented: true},

	{Name: "good", Category: CategoryStatusMethod, Signature: `status:good()`, Description: "Status is good.", Implemented: true},
	{Name: "bad", Category: CategoryStatusMethod, Signature: `status:bad()`, Description: "Status is bad.", Implemented: true},
	{Name: "alarm", Category: CategoryStatusMethod, Signature: `status:alarm()`, Description: "Status is in alarm.", Implemented: true},
	{Name: "level", Category: CategoryStatusMethod, Signature: `status:level()`, Description: "Alarm level from status.", Implemented: true},
	{Name: "inhibit", Category: CategoryStatusMethod, Signature: `status:inhibit()`, Description: "Alarm inhibit state from status.", Implemented: true},
	{Name: "unack", Category: CategoryStatusMethod, Signature: `status:unack()`, Description: "Unacknowledged alarm state from status.", Implemented: true},

	{Name: "op.snapshot", Category: CategoryHistorySnapshot, Signature: `op.snapshot(point, time, mode)`, Description: "Historical value near a timestamp.", Implemented: true, Notes: "Documented modes: prev, inter, next, near, none."},
	{Name: "op.prev", Category: CategoryHistorySnapshot, Signature: `op.prev(point, time)`, Description: "Previous archive value.", Implemented: true},
	{Name: "op.next", Category: CategoryHistorySnapshot, Signature: `op.next(point, time)`, Description: "Next archive value.", Implemented: true},

	{Name: "op.archive", Category: CategoryHistorySeries, Signature: `op.archive(point, begin, end)`, Description: "Raw archive series.", Implemented: true},
	{Name: "op.plot", Category: CategoryHistorySeries, Signature: `op.plot(point, begin, end, interval)`, Description: "Plot archive series.", Implemented: true},
	{Name: "op.span", Category: CategoryHistorySeries, Signature: `op.span(point, begin, end, interval)`, Description: "Equal-spacing archive series.", Implemented: true},

	{Name: "op.stat", Category: CategoryStatistic, Signature: `op.stat(point, begin, end, interval)`, Description: "Statistic result over an interval.", Implemented: true},
	{Name: "op.max", Category: CategoryStatistic, Signature: `op.max(point, begin, end)`, Description: "Maximum archive value.", Implemented: true},
	{Name: "op.min", Category: CategoryStatistic, Signature: `op.min(point, begin, end)`, Description: "Minimum archive value.", Implemented: true},
	{Name: "op.avg", Category: CategoryStatistic, Signature: `op.avg(point, begin, end)`, Description: "Area/time weighted average.", Implemented: true},
	{Name: "op.mean", Category: CategoryStatistic, Signature: `op.mean(point, begin, end)`, Description: "Arithmetic mean.", Implemented: true},
	{Name: "op.sum", Category: CategoryStatistic, Signature: `op.sum(point, begin, end)`, Description: "Sum over a period.", Implemented: true},
	{Name: "op.flow", Category: CategoryStatistic, Signature: `op.flow(point, begin, end)`, Description: "Accumulated flow over a period.", Implemented: true},
	{Name: "op.stdev", Category: CategoryStatistic, Signature: `op.stdev(point, begin, end)`, Description: "Standard deviation.", Implemented: false, Notes: "Documented as not implemented."},

	{Name: "op.cacheq", Category: CategorySystem, Signature: `op.cacheq(database)`, Description: "Archive cache queue length.", Implemented: true},
	{Name: "op.dbload", Category: CategorySystem, Signature: `op.dbload()`, Description: "Database load.", Implemented: true},
	{Name: "op.dbmem", Category: CategorySystem, Signature: `op.dbmem()`, Description: "Database memory usage.", Implemented: true},
	{Name: "op.volfree", Category: CategorySystem, Signature: `op.volfree()`, Description: "Free database disk space.", Implemented: true},
	{Name: "op.voltotal", Category: CategorySystem, Signature: `op.voltotal()`, Description: "Total database disk space.", Implemented: true},
	{Name: "op.uptime", Category: CategorySystem, Signature: `op.uptime()`, Description: "Database uptime.", Implemented: true},
	{Name: "op.calc_time", Category: CategorySystem, Signature: `op.calc_time()`, Description: "Periodic calculation duration.", Implemented: true},
	{Name: "op.counter", Category: CategorySystem, Signature: `op.counter()`, Description: "Database counter.", Implemented: true},
	{Name: "op.event", Category: CategorySystem, Signature: `op.event()`, Description: "Realtime event queue length.", Implemented: true},
	{Name: "op.idle", Category: CategorySystem, Signature: `op.idle()`, Description: "Idle thread count.", Implemented: true},
	{Name: "op.load", Category: CategorySystem, Signature: `op.load()`, Description: "System load.", Implemented: true},
	{Name: "op.memfree", Category: CategorySystem, Signature: `op.memfree()`, Description: "Free system memory.", Implemented: true},
	{Name: "op.memtotal", Category: CategorySystem, Signature: `op.memtotal()`, Description: "Total system memory.", Implemented: true},
	{Name: "op.ping", Category: CategorySystem, Signature: `op.ping(ip)`, Description: "Ping a configured address.", Implemented: true},
	{Name: "op.rate", Category: CategorySystem, Signature: `op.rate(point, seconds)`, Description: "Average change rate.", Implemented: true},
	{Name: "op.session", Category: CategorySystem, Signature: `op.session()`, Description: "Active session count.", Implemented: true},
	{Name: "op.session_peak", Category: CategorySystem, Signature: `op.session_peak()`, Description: "Peak session count.", Implemented: true},
	{Name: "op.thread", Category: CategorySystem, Signature: `op.thread()`, Description: "Thread count.", Implemented: true},

	{Name: "op.now", Category: CategoryTime, Signature: `op.now()`, Description: "Current timestamp.", Implemented: true},
	{Name: "op.today", Category: CategoryTime, Signature: `op.today()`, Description: "Current date at day boundary.", Implemented: true},
	{Name: "op.date", Category: CategoryTime, Signature: `op.date(...)`, Description: "Construct or convert a date.", Implemented: true},
	{Name: "op.bday", Category: CategoryTime, Signature: `op.bday(time)`, Description: "Begin of day.", Implemented: true},
	{Name: "op.bmonth", Category: CategoryTime, Signature: `op.bmonth(time)`, Description: "Begin of month.", Implemented: true},
	{Name: "op.bnextmonth", Category: CategoryTime, Signature: `op.bnextmonth(time)`, Description: "Begin of next month.", Implemented: true},
	{Name: "op.timeadd", Category: CategoryTime, Signature: `op.timeadd(time, amount, unit)`, Description: "Add time.", Implemented: true},
	{Name: "op.timediff", Category: CategoryTime, Signature: `op.timediff(left, right, unit)`, Description: "Time difference.", Implemented: true},
	{Name: "op.year", Category: CategoryTime, Signature: `op.year(time)`, Description: "Year component.", Implemented: true},
	{Name: "op.month", Category: CategoryTime, Signature: `op.month(time)`, Description: "Month component.", Implemented: true},
	{Name: "op.day", Category: CategoryTime, Signature: `op.day(time)`, Description: "Day component.", Implemented: true},
	{Name: "op.hour", Category: CategoryTime, Signature: `op.hour(time)`, Description: "Hour component.", Implemented: true},
	{Name: "op.minute", Category: CategoryTime, Signature: `op.minute(time)`, Description: "Minute component.", Implemented: true},
	{Name: "op.second", Category: CategoryTime, Signature: `op.second(time)`, Description: "Second component.", Implemented: true},
	{Name: "op.msecond", Category: CategoryTime, Signature: `op.msecond(time)`, Description: "Millisecond component.", Implemented: true},
	{Name: "op.format", Category: CategoryTime, Signature: `op.format(time, layout)`, Description: "Format timestamp.", Implemented: true},

	{Name: "op.set", Category: CategoryWrite, Signature: `op.set(point, value)`, Description: "Write from a calculation formula.", Implemented: true},

	{Name: "op.acc", Category: CategoryPeriodic, Signature: `op.acc(point, ...)`, Description: "Periodic accumulation helper.", Implemented: true},
	{Name: "op.meter", Category: CategoryPeriodic, Signature: `op.meter(point, ...)`, Description: "Meter calculation helper.", Implemented: true},
	{Name: "op.meterp", Category: CategoryPeriodic, Signature: `op.meterp(point, ...)`, Description: "Meter pulse calculation helper.", Implemented: true},
	{Name: "op.calctime", Category: CategoryPeriodic, Signature: `op.calctime()`, Description: "Periodic calculation timestamp helper.", Implemented: true},
	{Name: "op.calcmaxtime", Category: CategoryPeriodic, Signature: `op.calcmaxtime()`, Description: "Maximum calculation time helper.", Implemented: true},
	{Name: "op.calcmaxtag", Category: CategoryPeriodic, Signature: `op.calcmaxtag()`, Description: "Maximum calculation tag helper.", Implemented: true},

	{Name: "op.ar_sync_time", Category: CategoryMirror, Signature: `op.ar_sync_time(...)`, Description: "Archive mirror synchronization time.", Implemented: true},
	{Name: "op.rt_sync_time", Category: CategoryMirror, Signature: `op.rt_sync_time(...)`, Description: "Realtime mirror synchronization time.", Implemented: true},

	{Name: "if97.*", Category: CategoryWaterSteam, Signature: `if97.*(...)`, Description: "IF97 water and steam function family.", Implemented: true},
	{Name: "ifc67.*", Category: CategoryWaterSteam, Signature: `ifc67.*(...)`, Description: "IFC67 water and steam function family.", Implemented: true},
}
