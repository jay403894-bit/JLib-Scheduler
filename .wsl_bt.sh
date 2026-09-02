cd ~/tt
which gdb >/dev/null 2>&1 || { echo "no gdb"; exit 0; }
./build/bin/SchedulerIoSocketTest > /tmp/s2.out 2>&1 &
pid=$!
sleep 25
echo "=== still alive? ==="; kill -0 $pid 2>/dev/null && echo yes || echo no
gdb -p $pid -batch -ex "thread apply all bt 6" 2>/dev/null | grep -E "^Thread|^#[0-9]" | head -60
kill -9 $pid 2>/dev/null
