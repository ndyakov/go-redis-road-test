package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/term"
)

// Colour is never the only signal: every value has a word and a direction
// ("higher is better"), the selected engine has brackets, and actions in the
// chart axis are letters. NO_COLOR=1 turns colour off.
var noColor = os.Getenv("NO_COLOR") != ""

type rgb struct{ r, g, b int }

var (
	cText        = rgb{232, 240, 244}
	cMuted       = rgb{150, 166, 176}
	cBorder      = rgb{46, 64, 76}
	cDusk        = rgb{22, 51, 65}
	cLime        = rgb{220, 255, 30}
	cHyper       = rgb{255, 68, 56}
	engineColors = [numEngines]rgb{
		{185, 194, 198}, // pool
		{128, 219, 255}, // batching
		{70, 150, 235},  // batching, async
		{255, 68, 56},   // full duplex
		{199, 149, 227}, // full duplex, async
	}
)

func fg(c rgb, s string) string {
	if noColor || s == "" {
		return s
	}
	return fmt.Sprintf("\x1b[38;2;%d;%d;%dm%s\x1b[0m", c.r, c.g, c.b, s)
}

func fgbg(f, b rgb, s string) string {
	if noColor {
		return s
	}
	return fmt.Sprintf("\x1b[38;2;%d;%d;%d;48;2;%d;%d;%dm%s\x1b[0m", f.r, f.g, f.b, b.r, b.g, b.b, s)
}

func bold(s string) string {
	if noColor {
		return s
	}
	return "\x1b[1m" + s + "\x1b[22m"
}

func muted(s string) string { return fg(cMuted, s) }

var ansiRE = regexp.MustCompile("\x1b\\[[0-9;]*m")

// visible is the number of terminal cells s takes: every rune used here is one cell wide.
func visible(s string) int { return utf8.RuneCountInString(ansiRE.ReplaceAllString(s, "")) }

func pad(s string, w int) string {
	if n := visible(s); n < w {
		return s + strings.Repeat(" ", w-n)
	}
	return s
}

// box draws a rounded panel of the given outer width around lines.
func box(title string, lines []string, width int) []string {
	inner := width - 4
	top := fg(cBorder, "╭─ ") + bold(title) + " " + fg(cBorder, strings.Repeat("─", max(width-5-visible(title), 0))+"╮")
	out := []string{top}
	for _, l := range lines {
		out = append(out, fg(cBorder, "│ ")+pad(l, inner)+fg(cBorder, " │"))
	}
	return append(out, fg(cBorder, "╰"+strings.Repeat("─", width-2)+"╯"))
}

// side puts two panels next to each other.
func side(a, b []string, wa int) []string {
	n := max(len(a), len(b))
	out := make([]string, n)
	for i := 0; i < n; i++ {
		l, r := "", ""
		if i < len(a) {
			l = a[i]
		}
		if i < len(b) {
			r = b[i]
		}
		out[i] = pad(l, wa) + " " + r
	}
	return out
}

func runTUI(d *demo) error {
	fd := int(os.Stdin.Fd())
	if !term.IsTerminal(fd) {
		return fmt.Errorf("not a terminal; use --scripted")
	}
	old, err := term.MakeRaw(fd)
	if err != nil {
		return err
	}
	defer term.Restore(fd, old)
	fmt.Print("\x1b[?1049h\x1b[?25l") // alternate screen, hide cursor
	defer fmt.Print("\x1b[?25h\x1b[?1049l")

	if err := d.restart('1'); err != nil {
		return err
	}
	keys := make(chan byte, 16)
	go func() {
		buf := make([]byte, 1)
		for {
			if n, err := os.Stdin.Read(buf); err != nil || n == 0 {
				close(keys)
				return
			}
			keys <- buf[0]
		}
	}()
	t := time.NewTicker(tick)
	defer t.Stop()
	ctx := context.Background()
	draw(d)
	for {
		select {
		case k, ok := <-keys:
			if !ok || k == 'q' || k == 'Q' || k == 3 {
				return nil
			}
			handleKey(d, k)
			draw(d)
		case <-t.C:
			d.sample(ctx)
			draw(d)
		}
	}
}

func handleKey(d *demo, k byte) { d.key(k) }

func indexOf(xs []int, v int) int {
	for i, x := range xs {
		if x == v {
			return i
		}
	}
	return 0
}

var eighths = []rune(" ▁▂▃▄▅▆▇█")

// chart draws one column per point, bars growing from the bottom in eighths of
// a cell. level(i) returns the bar height in [0, 1]; mark(i) returns the row
// fraction of an overlay mark (the p50), or -1.
type chart struct {
	n      int
	level  func(i int) float64
	mark   func(i int) float64
	colour func(i int) rgb
	axis   [3]string // labels for top, middle, bottom
}

func (c chart) render(rows, cols int) []string {
	const axisW = 9
	out := make([]string, rows)
	start := max(c.n-cols, 0)
	for r := 0; r < rows; r++ {
		label := ""
		switch r {
		case 0:
			label = c.axis[0]
		case rows / 2:
			label = c.axis[1]
		case rows - 1:
			label = c.axis[2]
		}
		var b strings.Builder
		b.WriteString(muted(fmt.Sprintf("%*s ", axisW-1, label)) + fg(cBorder, "┤"))
		rowFromBottom := rows - 1 - r
		for i := start; i < c.n; i++ {
			lv := min(max(c.level(i), 0), 1) * float64(rows*8)
			cell := int(math.Round(lv)) - rowFromBottom*8
			ch := eighths[min(max(cell, 0), 8)]
			col := c.colour(i)
			m := -1
			if c.mark != nil {
				if f := c.mark(i); f >= 0 {
					m = int(math.Min(f*float64(rows), float64(rows)-1e-9))
				}
			}
			switch {
			case m == rowFromBottom && cell >= 8:
				b.WriteString(fgbg(cText, col, "━"))
			case m == rowFromBottom:
				b.WriteString(fg(cText, "━"))
			default:
				b.WriteString(fg(col, string(ch)))
			}
		}
		out[r] = b.String()
	}
	return out
}

func niceUp(v float64) float64 {
	if v <= 0 {
		return 1
	}
	p := math.Pow(10, math.Floor(math.Log10(v)))
	for _, m := range []float64{1, 2, 2.5, 5, 10} {
		if m*p >= v {
			return m * p
		}
	}
	return 10 * p
}

func draw(d *demo) {
	W, H, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || W < 80 {
		W, H = 120, 45
	}
	W = min(W, 200)
	d.mu.Lock()
	m := d.last
	eng, callers, win := d.engine, d.callers, d.window
	cfg := d.cfg
	tr := d.trouble
	raddr := d.target.addr + " [" + d.target.security() + "]"
	target := d.target
	rtt := d.rtt
	hist := append([]tickPoint(nil), d.history...)
	marks := append([]byte(nil), d.marks...)
	board := make(map[scenario]metrics, len(d.board))
	for k, v := range d.board {
		board[k] = v
	}
	var lastEvent string
	if n := len(d.events); n > 0 {
		lastEvent = d.events[n-1]
	}
	d.mu.Unlock()

	var lines []string
	add := func(ls ...string) { lines = append(lines, ls...) }

	// header bar
	wl := cfg.Workload
	if cfg.zeroCopy() {
		wl += " (GetToBuffer)"
	}
	title := "  go-redis engines   " + goRedisVersion() + "   " + wl + fmt.Sprintf(", %d-byte values   Redis at ", cfg.ValueSize) + raddr
	add(fgbg(cText, cDusk, bold(pad(title, W))))

	// controls
	var road []string
	for _, r := range rttSteps {
		s := fmt.Sprintf(" +%s ", r)
		if r == rtt {
			s = fgbg(rgb{9, 26, 35}, cLime, bold("["+strings.TrimSpace(s)+"]"))
		} else {
			s = muted(s)
		}
		road = append(road, s)
	}
	roadNote := "client and Redis on this machine"
	if rtt > 0 {
		roadNote = "added by the demo's road proxy"
	}
	var es []string
	for e := Engine(0); e < numEngines; e++ {
		label := fmt.Sprintf("%d %s", e+1, e)
		if e == eng {
			es = append(es, fgbg(rgb{9, 26, 35}, engineColors[e], bold("["+label+"]")))
		} else {
			es = append(es, fg(engineColors[e], " "+label+" "))
		}
	}
	inflight := muted("in flight per caller: 1 (blocking face)")
	if eng.Async() {
		inflight = fmt.Sprintf("in flight per caller: %s %s", bold(fmt.Sprint(win)), muted("(w)"))
	}
	add(box("setup", []string{
		bold("road    ") + strings.Join(road, " ") + muted("  (r)   ") + "measured " + bold(fmtDur(m.measuredRTT)) + muted("  "+roadNote),
		bold("engine  ") + strings.Join(es, " ") + muted("  (1-5)"),
		bold("load    ") + bold(fmt.Sprint(callers)) + map[bool]string{true: " caller ", false: " callers "}[callers == 1] + muted("(+/-)") + "    " + inflight,
		bold("start   ") + fg(engineColors[eng], strings.ReplaceAll(target.clientCode(cfg, eng), "\n", "; ")),
	}, W)...)

	// now + scoreboard
	leftW := 50
	now := []string{
		pad("requests/s", 14) + pad(bold(fmtRate(m.rate)), 18) + muted("higher better"),
		pad("Redis GETs/s", 14) + pad(bold(fmtRate(m.serverRate)), 18) + muted("from Redis"),
		pad("p50 / p99", 14) + pad(bold(fmtDur(m.p50)+" / "+fmtDur(m.p99)), 18) + muted("lower better"),
		pad("connections", 14) + pad(bold(fmt.Sprint(m.conns)), 18) + muted("lower better"),
		pad("client CPU", 14) + pad(bold(fmt.Sprintf("%.1f cores", m.cores)), 18) + muted("incl. proxy"),
		pad("allocs/req", 14) + pad(bold(fmt.Sprintf("%.1f", m.allocsPerReq)), 18) + muted(map[bool]string{true: "low rate", false: "lower better"}[m.allocLowRate]),
	}
	if m.errs > 0 {
		now = append(now, fg(cHyper, fmt.Sprintf("errors %d: %s", m.errs, m.lastErr)))
	}
	blocked := fmt.Sprintf("queued %d", tr.Queued)
	if eng == Pool {
		blocked = fmt.Sprintf("waiting %d", tr.PoolWaiting)
	}
	var errRate float64
	for _, v := range tr.ErrRates {
		errRate += v
	}
	now = append(now, muted(fmt.Sprintf("err %s/s  %s  delayed %.1f%%", fmtRate(errRate), blocked, tr.DelayedPct)))
	type row struct {
		sc scenario
		m  metrics
	}
	var rows []row
	load := cfg.loadKey()
	for sc, mm := range board {
		if sc.rtt == rtt && sc.callers == callers && sc.load == load {
			rows = append(rows, row{sc, mm})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].sc.engine != rows[j].sc.engine {
			return rows[i].sc.engine < rows[j].sc.engine
		}
		return rows[i].sc.window < rows[j].sc.window
	})
	// Two winners, never one: the most requests/s and the lowest p99. A row that
	// buys throughput with a long tail must not look like the overall best.
	best, fastest := 0.0, time.Duration(math.MaxInt64)
	for _, r := range rows {
		best = max(best, r.m.rate)
		if r.m.p99 > 0 {
			fastest = min(fastest, r.m.p99)
		}
	}
	rightW := W - leftW - 1
	const txtW = 36 // " 1.55M  p99 134.2 ms  501 conns ▲●"
	barW := max(rightW-4-22-txtW, 6)
	var sb []string
	for _, r := range rows {
		name := r.sc.engine.String()
		if r.sc.engine.Async() {
			name += fmt.Sprintf(" x%d", r.sc.window)
		}
		if r.sc.opts != "" {
			name += ", " + r.sc.opts
		}
		n := int(math.Round(r.m.rate / best * float64(barW)))
		bar := fg(engineColors[r.sc.engine], strings.Repeat("█", max(n, 1))) + strings.Repeat(" ", barW-max(n, 1))
		txt := fmt.Sprintf(" %7s  p99 %-8s %4d conn", fmtRate(r.m.rate), fmtDur(r.m.p99), r.m.conns)
		if r.m.conns != 1 {
			txt += "s"
		}
		tag := ""
		if len(rows) > 1 && r.m.rate == best {
			tag += "▲"
		}
		if len(rows) > 1 && r.m.p99 == fastest {
			tag += "●"
		}
		txt = pad(txt, 33) + fg(cLime, bold(tag))
		sb = append(sb, fg(engineColors[r.sc.engine], pad(name, 22))+bar+txt)
	}
	if len(sb) == 0 {
		sb = append(sb, muted("each engine you run shows here after 2.5 s on this road and load"))
	}
	nowTitle := "last 2 s"
	if m.warming {
		nowTitle = "last 2 s  (warming up)"
	}
	add(side(box(nowTitle, now, leftW),
		box(fmt.Sprintf("scoreboard +%s, %d callers, %s   ▲ most req/s  ● lowest p99   (x)", rtt, callers, target.security()), sb, rightW), leftW)...)

	// charts: what is left of the height, 60% throughput, 40% latency
	room := H - len(lines) - 2 - 6 // footer, and borders + axis lines of two boxes
	ch1 := max(room*3/5, 4)
	ch2 := max(room-ch1, 3)
	cols := W - 4 - 10
	n := len(hist)
	start := max(n-cols, 0)
	peak := 0.0
	maxP99 := 0.0
	for _, p := range hist[start:] {
		peak = max(peak, p.rate)
		maxP99 = max(maxP99, float64(p.p99)/float64(time.Millisecond))
	}
	peak = niceUp(peak * 1.05)
	colour := func(i int) rgb {
		if hist[i].warming {
			return rgb{70, 88, 100} // warm-up: dimmed, and left out of the numbers
		}
		return engineColors[hist[i].engine]
	}
	tp := chart{n: n, colour: colour,
		level: func(i int) float64 { return hist[i].rate / peak },
		axis:  [3]string{fmtRate(peak), fmtRate(peak / 2), "0"}}
	var axisMarks strings.Builder
	axisMarks.WriteString(strings.Repeat(" ", 10))
	for i := start; i < n; i++ {
		if marks[i] != 0 {
			axisMarks.WriteString(fg(cLime, string(marks[i])))
		} else {
			axisMarks.WriteString(fg(cBorder, "─"))
		}
	}
	var legend []string
	for e := Engine(0); e < numEngines; e++ {
		legend = append(legend, fg(engineColors[e], "■ ")+muted(e.String()))
	}
	add(box("throughput, requests/s  (higher is better)   500 ms per column, letters = keys, grey = warm-up",
		append(tp.render(ch1, cols), axisMarks.String()), W)...)

	// latency: log scale from 0.1 ms
	lo, hi := 0.1, niceUp(max(maxP99*1.1, 2))
	logPos := func(d time.Duration) float64 {
		ms := float64(d) / float64(time.Millisecond)
		if ms <= 0 {
			return -1
		}
		return (math.Log10(ms) - math.Log10(lo)) / (math.Log10(hi) - math.Log10(lo))
	}
	mid := math.Pow(10, (math.Log10(lo)+math.Log10(hi))/2)
	lt := chart{n: n, colour: colour,
		level: func(i int) float64 { return logPos(hist[i].p99) },
		mark:  func(i int) float64 { return logPos(hist[i].p50) },
		axis:  [3]string{fmt.Sprintf("%g ms", hi), fmt.Sprintf("%.2g ms", mid), fmt.Sprintf("%g ms", lo)}}
	add(box("latency: bar = p99, white line = p50, log scale  (lower is better)", append(lt.render(ch2, cols),
		strings.Repeat(" ", 10)+strings.Join(legend, "   ")), W)...)

	add(muted(" 1-5 engine   r road   +/- callers   w in flight (async)   x clear scoreboard   q quit"),
		muted(" last: "+lastEvent))

	var b strings.Builder
	b.WriteString("\x1b[H")
	for i, l := range lines {
		if i >= H {
			break
		}
		b.WriteString(l + "\x1b[K")
		if i < len(lines)-1 && i < H-1 {
			b.WriteString("\r\n")
		}
	}
	b.WriteString("\x1b[J")
	fmt.Print(b.String())
}
