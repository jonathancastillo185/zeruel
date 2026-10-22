"""Sampler de RAM: mide el RSS total de todos los procesos del contenedor
cada 100ms durante dur_s, y escribe el pico en /opt/airflow/bench_peak.txt."""
import time

def total_rss_mb():
    total = 0
    for pid in os_list():
        try:
            with open('/proc/%s/status' % pid) as f:
                for line in f:
                    if line.startswith('VmRSS:'):
                        total += int(line.split()[1])
                        break
        except Exception:
            pass
    return total / 1024.0

def os_list():
    import os
    return [p for p in os.listdir('/proc') if p.isdigit()]

peak = 0.0
base = total_rss_mb()
start = time.time()
dur = 240
while time.time() - start < dur:
    r = total_rss_mb()
    if r > peak:
        peak = r
    time.sleep(0.1)
with open('/opt/airflow/bench_peak.txt', 'w') as f:
    f.write('base=%.0f peak=%.0f delta=%.0f' % (base, peak, peak - base))
print('done base=%.0f peak=%.0f delta=%.0f' % (base, peak, peak - base))