
if [[ $# -ne 2 ]]; then
  echo Usage: script '<'#executors'>' '<'fib'>'
  exit 1
else
  cd ../build/integrationTest
  echo Running Fib'('$2')' on $1 executors
  java -cp ../main:../../external/'*':. -Dibis.constellation.distributed=false test.fib.Fibonacci $@
fi
