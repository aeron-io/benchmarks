# gnuplot -p -e "filename='foo.csv'" latency_around_failover.p

set title "latency around failover"
set xlabel "request time (seconds)"
set ylabel "response latency (microseconds)"
set logscale y 10
set xtics 10
set datafile separator ','
set linetype 40 lc rgb "red" dashtype 2
set linetype 41 lc rgb "dark-green" dashtype 2
set linetype 42 lc rgb "light-green" dashtype 2
# annotations placeholder
first(x) = ($0 > 0 ? base : base = x)
plot filename every ::1 using (($1-first($1))/1000000000):(($2-$1)/1000) notitle with dots, \
  1/0 t "follower restart" with line linetype 42, \
  1/0 t "step down" with line linetype 40, \
  1/0 t "restart" with line linetype 41
