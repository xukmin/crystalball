#!/usr/bin/env gnuplot
#
# Gnuplot script.

set terminal png
set output "benchmark.png"
set xlabel "Number of Reducers"
set ylabel "Execution Time (ms)"
plot "benchmark.txt" with linespoints title "MapReduce Execution Time"

